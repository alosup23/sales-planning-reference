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
    private readonly SemaphoreSlim _schemaInitLock = new(1, 1);
    private volatile bool _schemaReady;

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
                await EnsureSchemaAsync(stoppingToken);
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

    private async Task EnsureSchemaAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_connectionString) || _schemaReady)
        {
            return;
        }

        await _schemaInitLock.WaitAsync(cancellationToken);
        try
        {
            if (_schemaReady)
            {
                return;
            }

            await using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = new NpgsqlCommand(
                """
                create table if not exists planning_async_jobs (
                    job_id text primary key,
                    category text not null,
                    operation text not null,
                    requested_by text not null,
                    payload_json text not null,
                    upload_content bytea null,
                    upload_file_name text null,
                    status text not null,
                    progress_percent integer not null default 0,
                    progress_message text not null,
                    created_at timestamptz not null default now(),
                    started_at timestamptz null,
                    completed_at timestamptz null,
                    error_message text null,
                    summary_json text null,
                    download_content bytea null,
                    download_file_name text null,
                    download_content_type text null,
                    worker_id text null,
                    attempt_count integer not null default 0,
                    retain_until timestamptz null
                );

                create index if not exists idx_planning_async_jobs_retain_until
                    on planning_async_jobs (retain_until);

                create table if not exists planning_reconciliation_schedules (
                    schedule_key text primary key,
                    scenario_version_id bigint not null,
                    is_enabled integer not null default 1,
                    interval_minutes integer not null default 1440,
                    next_run_at timestamptz not null default now() + interval '1 day',
                    last_enqueued_at timestamptz null,
                    retain_days integer not null default 30
                );

                insert into planning_reconciliation_schedules (schedule_key, scenario_version_id, is_enabled, interval_minutes, next_run_at, retain_days)
                values ('default-scenario-1', 1, 1, 1440, now() + interval '1 day', 30)
                on conflict (schedule_key) do nothing;

                create table if not exists planning_reconciliation_reports (
                    report_id text primary key,
                    scenario_version_id bigint not null,
                    requested_by text not null,
                    is_scheduled boolean not null default false,
                    mismatch_count integer not null,
                    checked_cell_count integer not null,
                    status text not null,
                    report_json text not null,
                    created_at timestamptz not null default now(),
                    retain_until timestamptz null
                );

                create index if not exists idx_planning_reconciliation_reports_retain_until
                    on planning_reconciliation_reports (retain_until);
                """,
                connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
            _schemaReady = true;
        }
        finally
        {
            _schemaInitLock.Release();
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
