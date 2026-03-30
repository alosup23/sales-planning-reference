using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public sealed class PlanningAsyncJobManager : BackgroundService, IPlanningAsyncJobManager
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PlanningAsyncJobManager> _logger;
    private readonly string? _connectionString;
    private readonly string _workerId = $"{Environment.MachineName}:{Guid.NewGuid():N}";
    private readonly SemaphoreSlim _durableSignal = new(0);
    private readonly SemaphoreSlim _schemaInitLock = new(1, 1);
    private readonly Channel<string> _inMemoryChannel = Channel.CreateUnbounded<string>();
    private readonly ConcurrentDictionary<string, InMemoryPlanningAsyncJobState> _inMemoryJobs = new(StringComparer.OrdinalIgnoreCase);
    private volatile bool _durableSchemaReady;

    public PlanningAsyncJobManager(IServiceScopeFactory scopeFactory, ILogger<PlanningAsyncJobManager> logger, string? connectionString = null)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString;
    }

    public async Task<AsyncJobStatusResponse> EnqueueAsync(PlanningAsyncJobRequest request, CancellationToken cancellationToken)
    {
        if (UseDurableStore)
        {
            await EnsureDurableSchemaAsync(cancellationToken);
            var response = await InsertDurableJobAsync(request, cancellationToken);
            _durableSignal.Release();
            return response;
        }

        var jobId = Guid.NewGuid().ToString("N");
        var state = new InMemoryPlanningAsyncJobState(
            jobId,
            request,
            "queued",
            0,
            "Queued",
            DateTimeOffset.UtcNow,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

        _inMemoryJobs[jobId] = state;
        if (!_inMemoryChannel.Writer.TryWrite(jobId))
        {
            throw new InvalidOperationException("Unable to queue the async job.");
        }

        return ToResponse(state);
    }

    public async Task<AsyncJobStatusResponse> GetStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        if (UseDurableStore)
        {
            await EnsureDurableSchemaAsync(cancellationToken);
            return await GetDurableStatusAsync(jobId, cancellationToken);
        }

        return GetInMemoryStatus(jobId);
    }

    public async Task<(byte[] Content, string FileName, string ContentType)> DownloadResultAsync(string jobId, CancellationToken cancellationToken)
    {
        if (UseDurableStore)
        {
            await EnsureDurableSchemaAsync(cancellationToken);
            return await DownloadDurableResultAsync(jobId, cancellationToken);
        }

        return DownloadInMemoryResult(jobId);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (UseDurableStore)
        {
            await EnsureDurableSchemaAsync(stoppingToken);
            await RecoverDurableJobsAsync(stoppingToken);
            await ExecuteDurableLoopAsync(stoppingToken);
            return;
        }

        await ExecuteInMemoryLoopAsync(stoppingToken);
    }

    private bool UseDurableStore => !string.IsNullOrWhiteSpace(_connectionString);

    private async Task EnsureDurableSchemaAsync(CancellationToken cancellationToken)
    {
        if (!UseDurableStore || _durableSchemaReady)
        {
            return;
        }

        await _schemaInitLock.WaitAsync(cancellationToken);
        try
        {
            if (_durableSchemaReady)
            {
                return;
            }

            await using var connection = await OpenConnectionAsync(cancellationToken);
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

                create index if not exists idx_planning_async_jobs_status_created_at
                    on planning_async_jobs (status, created_at);

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

                create index if not exists idx_planning_reconciliation_reports_created_at
                    on planning_reconciliation_reports (created_at desc);

                create index if not exists idx_planning_reconciliation_reports_retain_until
                    on planning_reconciliation_reports (retain_until);
                """,
                connection);
            await command.ExecuteNonQueryAsync(cancellationToken);
            _durableSchemaReady = true;
        }
        finally
        {
            _schemaInitLock.Release();
        }
    }

    private async Task ExecuteDurableLoopAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var claimed = await TryClaimNextDurableJobAsync(stoppingToken);
                if (claimed is null)
                {
                    try
                    {
                        await _durableSignal.WaitAsync(TimeSpan.FromSeconds(5), stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    continue;
                }

                await ProcessDurableJobAsync(claimed, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Durable async job loop failed. Retrying.");
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }
    }

    private async Task ExecuteInMemoryLoopAsync(CancellationToken stoppingToken)
    {
        await foreach (var jobId in _inMemoryChannel.Reader.ReadAllAsync(stoppingToken))
        {
            if (!_inMemoryJobs.TryGetValue(jobId, out var state))
            {
                continue;
            }

            UpdateInMemoryState(state with
            {
                Status = "running",
                ProgressPercent = 5,
                ProgressMessage = "Starting",
                StartedAt = DateTimeOffset.UtcNow
            });

            var progress = new Progress<PlanningAsyncJobProgress>(update =>
            {
                if (_inMemoryJobs.TryGetValue(jobId, out var running))
                {
                    UpdateInMemoryState(running with
                    {
                        ProgressPercent = Math.Clamp(update.ProgressPercent, 0, 100),
                        ProgressMessage = string.IsNullOrWhiteSpace(update.ProgressMessage) ? running.ProgressMessage : update.ProgressMessage
                    });
                }
            });

            try
            {
                var result = await ExecuteJobAsync(jobId, state.Request, progress, stoppingToken);
                if (_inMemoryJobs.TryGetValue(jobId, out var finished))
                {
                    UpdateInMemoryState(finished with
                    {
                        Status = "completed",
                        ProgressPercent = 100,
                        ProgressMessage = "Completed",
                        CompletedAt = DateTimeOffset.UtcNow,
                        Summary = result.Summary,
                        DownloadContent = result.DownloadContent,
                        DownloadFileName = result.DownloadFileName,
                        DownloadContentType = result.DownloadContentType
                    });
                }
            }
            catch (Exception exception)
            {
                if (_inMemoryJobs.TryGetValue(jobId, out var failed))
                {
                    UpdateInMemoryState(failed with
                    {
                        Status = "failed",
                        ProgressMessage = "Failed",
                        CompletedAt = DateTimeOffset.UtcNow,
                        ErrorMessage = exception.Message
                    });
                }
            }
        }
    }

    private async Task<PlanningAsyncJobExecutionResult> ExecuteJobAsync(
        string jobId,
        PlanningAsyncJobRequest request,
        IProgress<PlanningAsyncJobProgress> progress,
        CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var planningService = scope.ServiceProvider.GetRequiredService<IPlanningService>();

        switch (request.Operation)
        {
            case "workbook-import":
            {
                var payload = DeserializePayload<WorkbookImportJobPayload>(request.PayloadJson);
                var uploadContent = RequireUploadContent(request);
                var uploadFileName = RequireUploadFileName(request);
                progress.Report(new PlanningAsyncJobProgress(15, "Importing workbook"));
                await using var stream = new MemoryStream(uploadContent, writable: false);
                var result = await planningService.ImportWorkbookAsync(payload.ScenarioVersionId, stream, uploadFileName, request.RequestedBy, cancellationToken);
                progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
                var reconciliation = await planningService.RunReconciliationAsync(payload.ScenarioVersionId, cancellationToken);
                await PersistReconciliationReportAsync(payload.ScenarioVersionId, request.RequestedBy, reconciliation, false, cancellationToken);
                return new PlanningAsyncJobExecutionResult(
                    new AsyncJobSummaryDto(
                        result.RowsProcessed,
                        result.CellsUpdated,
                        result.RowsCreated,
                        null,
                        null,
                        reconciliation.MismatchCount,
                        reconciliation.CheckedCellCount,
                        result.Status),
                    DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                    result.ExceptionFileName,
                    SpreadsheetContentType);
            }

            case "workbook-export":
            {
                var payload = DeserializePayload<WorkbookExportJobPayload>(request.PayloadJson);
                progress.Report(new PlanningAsyncJobProgress(40, "Generating workbook"));
                var result = await planningService.ExportWorkbookAsync(payload.ScenarioVersionId, cancellationToken);
                return new PlanningAsyncJobExecutionResult(
                    new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"),
                    result.Content,
                    result.FileName,
                    SpreadsheetContentType);
            }

            case "store-profile-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing store profiles",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportStoreProfilesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.StoresAdded, result.StoresUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "store-profile-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating store profile export", async (svc, ct) =>
                {
                    var result = await svc.ExportStoreProfilesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "product-profile-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing product profiles",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportProductProfilesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.ProductsAdded, result.ProductsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "product-profile-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating product profile export", async (svc, ct) =>
                {
                    var result = await svc.ExportProductProfilesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "inventory-profile-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing inventory profiles",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportInventoryProfilesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "inventory-profile-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating inventory export", async (svc, ct) =>
                {
                    var result = await svc.ExportInventoryProfilesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "pricing-policy-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing pricing policies",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportPricingPoliciesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "pricing-policy-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating pricing policy export", async (svc, ct) =>
                {
                    var result = await svc.ExportPricingPoliciesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "seasonality-event-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing seasonality and event profiles",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportSeasonalityEventProfilesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "seasonality-event-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating seasonality export", async (svc, ct) =>
                {
                    var result = await svc.ExportSeasonalityEventProfilesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "vendor-supply-import":
                return await ExecuteMasterDataImportAsync(
                    request,
                    progress,
                    "Importing vendor supply profiles",
                    async (svc, stream, fileName, ct) =>
                    {
                        var result = await svc.ImportVendorSupplyProfilesAsync(stream, fileName, ct);
                        var reconciliation = await svc.RunReconciliationAsync(1, ct);
                        await PersistReconciliationReportAsync(1, request.RequestedBy, reconciliation, false, ct);
                        return new PlanningAsyncJobExecutionResult(
                            new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                            DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                            result.ExceptionFileName,
                            SpreadsheetContentType);
                    },
                    planningService,
                    cancellationToken);

            case "vendor-supply-export":
                return await ExecuteMasterDataExportAsync(progress, "Generating vendor supply export", async (svc, ct) =>
                {
                    var result = await svc.ExportVendorSupplyProfilesAsync(ct);
                    return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
                }, planningService, cancellationToken);

            case "reconciliation":
            {
                var payload = DeserializePayload<ReconciliationJobPayload>(request.PayloadJson);
                progress.Report(new PlanningAsyncJobProgress(25, "Running reconciliation"));
                var result = await planningService.RunReconciliationAsync(payload.ScenarioVersionId, cancellationToken);
                await PersistReconciliationReportAsync(payload.ScenarioVersionId, request.RequestedBy, result, payload.Scheduled, cancellationToken);
                var reportContent = JsonSerializer.SerializeToUtf8Bytes(result, new JsonSerializerOptions(JsonSerializerDefaults.Web) { WriteIndented = true });
                return new PlanningAsyncJobExecutionResult(
                    new AsyncJobSummaryDto(null, null, null, null, null, result.MismatchCount, result.CheckedCellCount, result.Status),
                    reportContent,
                    $"reconciliation-report-{payload.ScenarioVersionId}.json",
                    "application/json");
            }

            default:
                throw new InvalidOperationException($"Async job operation '{request.Operation}' is not supported.");
        }
    }

    private static async Task<PlanningAsyncJobExecutionResult> ExecuteMasterDataImportAsync(
        PlanningAsyncJobRequest request,
        IProgress<PlanningAsyncJobProgress> progress,
        string progressMessage,
        Func<IPlanningService, Stream, string, CancellationToken, Task<PlanningAsyncJobExecutionResult>> work,
        IPlanningService planningService,
        CancellationToken cancellationToken)
    {
        progress.Report(new PlanningAsyncJobProgress(15, progressMessage));
        await using var stream = new MemoryStream(RequireUploadContent(request), writable: false);
        return await work(planningService, stream, RequireUploadFileName(request), cancellationToken);
    }

    private static async Task<PlanningAsyncJobExecutionResult> ExecuteMasterDataExportAsync(
        IProgress<PlanningAsyncJobProgress> progress,
        string progressMessage,
        Func<IPlanningService, CancellationToken, Task<PlanningAsyncJobExecutionResult>> work,
        IPlanningService planningService,
        CancellationToken cancellationToken)
    {
        progress.Report(new PlanningAsyncJobProgress(40, progressMessage));
        return await work(planningService, cancellationToken);
    }

    private async Task RecoverDurableJobsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            update planning_async_jobs
            set status = 'queued',
                progress_message = 'Queued after service restart',
                started_at = null,
                worker_id = null
            where status = 'running';
            """,
            connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<AsyncJobStatusResponse> InsertDurableJobAsync(PlanningAsyncJobRequest request, CancellationToken cancellationToken)
    {
        var jobId = Guid.NewGuid().ToString("N");
        var createdAt = DateTimeOffset.UtcNow;

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            insert into planning_async_jobs (
                job_id, category, operation, requested_by, payload_json, upload_content, upload_file_name,
                status, progress_percent, progress_message, created_at, retain_until
            )
            values (
                @jobId, @category, @operation, @requestedBy, @payloadJson, @uploadContent, @uploadFileName,
                'queued', 0, 'Queued', @createdAt, @retainUntil
            );
            """,
            connection);

        command.Parameters.AddWithValue("@jobId", jobId);
        command.Parameters.AddWithValue("@category", request.Category);
        command.Parameters.AddWithValue("@operation", request.Operation);
        command.Parameters.AddWithValue("@requestedBy", request.RequestedBy);
        command.Parameters.AddWithValue("@payloadJson", request.PayloadJson);
        command.Parameters.AddWithValue("@uploadContent", (object?)request.UploadContent ?? DBNull.Value);
        command.Parameters.AddWithValue("@uploadFileName", (object?)request.UploadFileName ?? DBNull.Value);
        command.Parameters.AddWithValue("@createdAt", createdAt);
        command.Parameters.AddWithValue("@retainUntil", createdAt.AddDays(30));
        await command.ExecuteNonQueryAsync(cancellationToken);

        return new AsyncJobStatusResponse(
            jobId,
            request.Category,
            request.Operation,
            "queued",
            0,
            "Queued",
            createdAt,
            null,
            null,
            null,
            null,
            false,
            null,
            null);
    }

    private async Task<DurablePlanningAsyncJob?> TryClaimNextDurableJobAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            with next_job as (
                select job_id
                from planning_async_jobs
                where status = 'queued'
                order by created_at
                limit 1
                for update skip locked
            )
            update planning_async_jobs jobs
            set status = 'running',
                progress_percent = 5,
                progress_message = 'Starting',
                started_at = now(),
                worker_id = @workerId,
                attempt_count = attempt_count + 1
            from next_job
            where jobs.job_id = next_job.job_id
            returning jobs.job_id, jobs.category, jobs.operation, jobs.requested_by, jobs.payload_json, jobs.upload_content, jobs.upload_file_name, jobs.created_at;
            """,
            connection,
            transaction);

        command.Parameters.AddWithValue("@workerId", _workerId);
        DurablePlanningAsyncJob? job = null;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                job = new DurablePlanningAsyncJob(
                    reader.GetString(0),
                    new PlanningAsyncJobRequest(
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetString(4),
                        reader.IsDBNull(5) ? null : (byte[])reader.GetValue(5),
                        reader.IsDBNull(6) ? null : reader.GetString(6)),
                    reader.GetFieldValue<DateTimeOffset>(7));
            }
        }

        await transaction.CommitAsync(cancellationToken);
        return job;
    }

    private async Task ProcessDurableJobAsync(DurablePlanningAsyncJob job, CancellationToken cancellationToken)
    {
        var progress = new Progress<PlanningAsyncJobProgress>(update =>
        {
            try
            {
                UpdateDurableProgressAsync(job.JobId, update, CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Unable to update async job progress for job {JobId}.", job.JobId);
            }
        });

        try
        {
            var result = await ExecuteJobAsync(job.JobId, job.Request, progress, cancellationToken);
            await CompleteDurableJobAsync(job.JobId, result, cancellationToken);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Async job {JobId} failed.", job.JobId);
            await FailDurableJobAsync(job.JobId, exception.Message, cancellationToken);
        }
    }

    private async Task UpdateDurableProgressAsync(string jobId, PlanningAsyncJobProgress progress, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            update planning_async_jobs
            set progress_percent = @progressPercent,
                progress_message = @progressMessage
            where job_id = @jobId;
            """,
            connection);
        command.Parameters.AddWithValue("@jobId", jobId);
        command.Parameters.AddWithValue("@progressPercent", Math.Clamp(progress.ProgressPercent, 0, 100));
        command.Parameters.AddWithValue("@progressMessage", string.IsNullOrWhiteSpace(progress.ProgressMessage) ? "Running" : progress.ProgressMessage);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task CompleteDurableJobAsync(string jobId, PlanningAsyncJobExecutionResult result, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            update planning_async_jobs
            set status = 'completed',
                progress_percent = 100,
                progress_message = 'Completed',
                completed_at = now(),
                summary_json = @summaryJson,
                download_content = @downloadContent,
                download_file_name = @downloadFileName,
                download_content_type = @downloadContentType
            where job_id = @jobId;
            """,
            connection);
        command.Parameters.AddWithValue("@jobId", jobId);
        command.Parameters.AddWithValue("@summaryJson", (object?)SerializeSummary(result.Summary) ?? DBNull.Value);
        command.Parameters.AddWithValue("@downloadContent", (object?)result.DownloadContent ?? DBNull.Value);
        command.Parameters.AddWithValue("@downloadFileName", (object?)result.DownloadFileName ?? DBNull.Value);
        command.Parameters.AddWithValue("@downloadContentType", (object?)result.DownloadContentType ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task FailDurableJobAsync(string jobId, string errorMessage, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            update planning_async_jobs
            set status = 'failed',
                progress_message = 'Failed',
                error_message = @errorMessage,
                completed_at = now()
            where job_id = @jobId;
            """,
            connection);
        command.Parameters.AddWithValue("@jobId", jobId);
        command.Parameters.AddWithValue("@errorMessage", errorMessage);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<AsyncJobStatusResponse> GetDurableStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select job_id, category, operation, status, progress_percent, progress_message, created_at, started_at, completed_at,
                   error_message, summary_json, download_file_name, download_content_type, download_content is not null
            from planning_async_jobs
            where job_id = @jobId;
            """,
            connection);
        command.Parameters.AddWithValue("@jobId", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        return new AsyncJobStatusResponse(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetString(2),
            reader.GetString(3),
            reader.GetInt32(4),
            reader.GetString(5),
            reader.GetFieldValue<DateTimeOffset>(6),
            reader.IsDBNull(7) ? null : reader.GetFieldValue<DateTimeOffset>(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : JsonSerializer.Deserialize<AsyncJobSummaryDto>(reader.GetString(10), JsonOptions),
            reader.GetBoolean(13),
            reader.IsDBNull(11) ? null : reader.GetString(11),
            reader.IsDBNull(12) ? null : reader.GetString(12));
    }

    private async Task<(byte[] Content, string FileName, string ContentType)> DownloadDurableResultAsync(string jobId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            select download_content, download_file_name, download_content_type
            from planning_async_jobs
            where job_id = @jobId;
            """,
            connection);
        command.Parameters.AddWithValue("@jobId", jobId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        if (reader.IsDBNull(0) || reader.IsDBNull(1) || reader.IsDBNull(2))
        {
            throw new InvalidOperationException("This async job does not have a downloadable result.");
        }

        return ((byte[])reader.GetValue(0), reader.GetString(1), reader.GetString(2));
    }

    private AsyncJobStatusResponse GetInMemoryStatus(string jobId)
    {
        if (!_inMemoryJobs.TryGetValue(jobId, out var state))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        return ToResponse(state);
    }

    private (byte[] Content, string FileName, string ContentType) DownloadInMemoryResult(string jobId)
    {
        if (!_inMemoryJobs.TryGetValue(jobId, out var state))
        {
            throw new InvalidOperationException($"Async job '{jobId}' was not found.");
        }

        if (state.DownloadContent is null || string.IsNullOrWhiteSpace(state.DownloadFileName) || string.IsNullOrWhiteSpace(state.DownloadContentType))
        {
            throw new InvalidOperationException("This async job does not have a downloadable result.");
        }

        return (state.DownloadContent, state.DownloadFileName, state.DownloadContentType);
    }

    private void UpdateInMemoryState(InMemoryPlanningAsyncJobState state)
    {
        _inMemoryJobs[state.JobId] = state;
    }

    private static AsyncJobStatusResponse ToResponse(InMemoryPlanningAsyncJobState state)
    {
        return new AsyncJobStatusResponse(
            state.JobId,
            state.Request.Category,
            state.Request.Operation,
            state.Status,
            state.ProgressPercent,
            state.ProgressMessage,
            state.CreatedAt,
            state.StartedAt,
            state.CompletedAt,
            state.ErrorMessage,
            state.Summary,
            state.DownloadContent is not null,
            state.DownloadFileName,
            state.DownloadContentType);
    }

    private async Task PersistReconciliationReportAsync(long scenarioVersionId, string requestedBy, ReconciliationReportResponse report, bool scheduled, CancellationToken cancellationToken)
    {
        if (!UseDurableStore)
        {
            return;
        }

        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            insert into planning_reconciliation_reports (
                report_id, scenario_version_id, requested_by, is_scheduled, mismatch_count, checked_cell_count, status, report_json, created_at, retain_until
            )
            values (
                @reportId, @scenarioVersionId, @requestedBy, @isScheduled, @mismatchCount, @checkedCellCount, @status, @reportJson, now(), now() + interval '30 days'
            );
            """,
            connection);
        command.Parameters.AddWithValue("@reportId", Guid.NewGuid().ToString("N"));
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("@requestedBy", requestedBy);
        command.Parameters.AddWithValue("@isScheduled", scheduled);
        command.Parameters.AddWithValue("@mismatchCount", report.MismatchCount);
        command.Parameters.AddWithValue("@checkedCellCount", report.CheckedCellCount);
        command.Parameters.AddWithValue("@status", report.Status);
        command.Parameters.AddWithValue("@reportJson", JsonSerializer.Serialize(report, JsonOptions));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static TPayload DeserializePayload<TPayload>(string payloadJson)
    {
        return JsonSerializer.Deserialize<TPayload>(payloadJson, JsonOptions)
            ?? throw new InvalidOperationException($"Unable to deserialize async job payload '{typeof(TPayload).Name}'.");
    }

    private static byte[] RequireUploadContent(PlanningAsyncJobRequest request)
    {
        return request.UploadContent ?? throw new InvalidOperationException($"Async job '{request.Operation}' requires upload content.");
    }

    private static string RequireUploadFileName(PlanningAsyncJobRequest request)
    {
        return string.IsNullOrWhiteSpace(request.UploadFileName)
            ? throw new InvalidOperationException($"Async job '{request.Operation}' requires an upload file name.")
            : request.UploadFileName;
    }

    private static string? SerializeSummary(AsyncJobSummaryDto? summary)
    {
        return summary is null ? null : JsonSerializer.Serialize(summary, JsonOptions);
    }

    private static byte[]? DecodeBase64OrNull(string? base64)
    {
        return string.IsNullOrWhiteSpace(base64) ? null : Convert.FromBase64String(base64);
    }

    private const string SpreadsheetContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

    private sealed record InMemoryPlanningAsyncJobState(
        string JobId,
        PlanningAsyncJobRequest Request,
        string Status,
        int ProgressPercent,
        string ProgressMessage,
        DateTimeOffset CreatedAt,
        DateTimeOffset? StartedAt,
        DateTimeOffset? CompletedAt,
        string? ErrorMessage,
        AsyncJobSummaryDto? Summary,
        byte[]? DownloadContent,
        string? DownloadFileName,
        string? DownloadContentType);

    private sealed record DurablePlanningAsyncJob(
        string JobId,
        PlanningAsyncJobRequest Request,
        DateTimeOffset CreatedAt);
}
