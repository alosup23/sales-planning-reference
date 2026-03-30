using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace SalesPlanning.Api.Application;

public sealed class PlanningReconciliationScheduler(
    IPlanningAsyncJobManager asyncJobManager,
    ILogger<PlanningReconciliationScheduler> logger,
    string? connectionString = null) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly string? _connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_connectionString))
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CleanupExpiredArtifactsAsync(stoppingToken);
                await EnqueueDueSchedulesAsync(asyncJobManager, stoppingToken);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Scheduled reconciliation orchestration failed.");
            }

            await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken);
        }
    }

    private async Task EnqueueDueSchedulesAsync(IPlanningAsyncJobManager asyncJobManager, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            with due as (
                select schedule_key, scenario_version_id, interval_minutes
                from planning_reconciliation_schedules
                where is_enabled = 1
                  and next_run_at <= now()
                order by next_run_at
                for update skip locked
            )
            update planning_reconciliation_schedules schedule
            set last_enqueued_at = now(),
                next_run_at = now() + make_interval(mins => due.interval_minutes)
            from due
            where schedule.schedule_key = due.schedule_key
            returning due.schedule_key, due.scenario_version_id;
            """,
            connection,
            transaction);

        var dueSchedules = new List<(string Key, long ScenarioVersionId)>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                dueSchedules.Add((reader.GetString(0), reader.GetInt64(1)));
            }
        }

        await transaction.CommitAsync(cancellationToken);

        foreach (var schedule in dueSchedules)
        {
            await asyncJobManager.EnqueueAsync(
                new PlanningAsyncJobRequest(
                    "planning",
                    "reconciliation",
                    "system",
                    JsonSerializer.Serialize(new ReconciliationJobPayload(schedule.ScenarioVersionId, true), JsonOptions)),
                cancellationToken);

            logger.LogInformation("Queued scheduled reconciliation job for schedule {ScheduleKey} scenario {ScenarioVersionId}.", schedule.Key, schedule.ScenarioVersionId);
        }
    }

    private async Task CleanupExpiredArtifactsAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var jobs = new NpgsqlCommand(
            "delete from planning_async_jobs where retain_until is not null and retain_until < now();",
            connection);
        await jobs.ExecuteNonQueryAsync(cancellationToken);

        await using var reports = new NpgsqlCommand(
            "delete from planning_reconciliation_reports where retain_until is not null and retain_until < now();",
            connection);
        await reports.ExecuteNonQueryAsync(cancellationToken);
    }
}
