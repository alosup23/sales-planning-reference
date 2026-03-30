using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    public Task<AsyncJobStatusResponse> StartWorkbookImportJobAsync(long scenarioVersionId, byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync(
            "planning",
            "workbook-import",
            userId,
            async (progress, ct) =>
            {
                progress.Report(new PlanningAsyncJobProgress(15, "Importing workbook"));
                await using var stream = new MemoryStream(workbookBytes, writable: false);
                var result = await ImportWorkbookAsync(scenarioVersionId, stream, fileName, userId, ct);

                progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
                var reconciliation = await RunReconciliationAsync(scenarioVersionId, ct);

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
            },
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartWorkbookExportJobAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync(
            "planning",
            "workbook-export",
            userId,
            async (progress, ct) =>
            {
                progress.Report(new PlanningAsyncJobProgress(40, "Generating workbook"));
                var result = await ExportWorkbookAsync(scenarioVersionId, ct);
                return new PlanningAsyncJobExecutionResult(
                    new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"),
                    result.Content,
                    result.FileName,
                    SpreadsheetContentType);
            },
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartStoreProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "store-profile-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing store profiles"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportStoreProfilesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.StoresAdded, result.StoresUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartStoreProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "store-profile-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating store profile export"));
            var result = await ExportStoreProfilesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartProductProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "product-profile-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing product profiles"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportProductProfilesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.ProductsAdded, result.ProductsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartProductProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "product-profile-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating product profile export"));
            var result = await ExportProductProfilesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartInventoryProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "inventory-profile-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing inventory profiles"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportInventoryProfilesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartInventoryProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "inventory-profile-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating inventory export"));
            var result = await ExportInventoryProfilesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartPricingPolicyImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "pricing-policy-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing pricing policies"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportPricingPoliciesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartPricingPolicyExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "pricing-policy-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating pricing policy export"));
            var result = await ExportPricingPoliciesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartSeasonalityEventImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "seasonality-event-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing seasonality and event profiles"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportSeasonalityEventProfilesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartSeasonalityEventExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "seasonality-event-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating seasonality export"));
            var result = await ExportSeasonalityEventProfilesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartVendorSupplyImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        QueueImportJobAsync("master-data", "vendor-supply-import", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(15, "Importing vendor supply profiles"));
            await using var stream = new MemoryStream(workbookBytes, writable: false);
            var result = await ImportVendorSupplyProfilesAsync(stream, fileName, ct);
            progress.Report(new PlanningAsyncJobProgress(85, "Running reconciliation"));
            var reconciliation = await RunReconciliationAsync(1, ct);
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(result.RowsProcessed, null, null, result.RecordsAdded, result.RecordsUpdated, reconciliation.MismatchCount, reconciliation.CheckedCellCount, result.Status),
                DecodeBase64OrNull(result.ExceptionWorkbookBase64),
                result.ExceptionFileName,
                SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartVendorSupplyExportJobAsync(string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("master-data", "vendor-supply-export", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(40, "Generating vendor supply export"));
            var result = await ExportVendorSupplyProfilesAsync(ct);
            return new PlanningAsyncJobExecutionResult(new AsyncJobSummaryDto(null, null, null, null, null, null, null, "Export completed"), result.Content, result.FileName, SpreadsheetContentType);
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> StartReconciliationJobAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken) =>
        QueueExportJobAsync("planning", "reconciliation", userId, async (progress, ct) =>
        {
            progress.Report(new PlanningAsyncJobProgress(25, "Running reconciliation"));
            var result = await RunReconciliationAsync(scenarioVersionId, ct);
            var reportContent = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(result, new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web) { WriteIndented = true });
            return new PlanningAsyncJobExecutionResult(
                new AsyncJobSummaryDto(null, null, null, null, null, result.MismatchCount, result.CheckedCellCount, result.Status),
                reportContent,
                $"reconciliation-report-{scenarioVersionId}.json",
                "application/json");
        }, cancellationToken);

    public Task<AsyncJobStatusResponse> GetAsyncJobStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().GetStatusAsync(jobId, cancellationToken);
    }

    public Task<(byte[] Content, string FileName, string ContentType)> DownloadAsyncJobResultAsync(string jobId, CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().DownloadResultAsync(jobId, cancellationToken);
    }

    private Task<AsyncJobStatusResponse> QueueImportJobAsync(
        string category,
        string operation,
        string userId,
        Func<IProgress<PlanningAsyncJobProgress>, CancellationToken, Task<PlanningAsyncJobExecutionResult>> work,
        CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().EnqueueAsync(category, operation, userId, work, cancellationToken);
    }

    private Task<AsyncJobStatusResponse> QueueExportJobAsync(
        string category,
        string operation,
        string userId,
        Func<IProgress<PlanningAsyncJobProgress>, CancellationToken, Task<PlanningAsyncJobExecutionResult>> work,
        CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().EnqueueAsync(category, operation, userId, work, cancellationToken);
    }

    private IPlanningAsyncJobManager RequireAsyncJobManager()
    {
        return _asyncJobManager ?? throw new InvalidOperationException("Async job manager is not configured.");
    }

    private static byte[]? DecodeBase64OrNull(string? base64)
    {
        return string.IsNullOrWhiteSpace(base64) ? null : Convert.FromBase64String(base64);
    }

    private const string SpreadsheetContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
}
