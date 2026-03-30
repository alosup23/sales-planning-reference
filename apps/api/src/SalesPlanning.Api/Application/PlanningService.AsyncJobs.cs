using SalesPlanning.Api.Contracts;
using System.Text.Json;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    public Task<AsyncJobStatusResponse> StartWorkbookImportJobAsync(long scenarioVersionId, byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "planning",
                "workbook-import",
                userId,
                SerializePayload(new WorkbookImportJobPayload(scenarioVersionId)),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartWorkbookExportJobAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "planning",
                "workbook-export",
                userId,
                SerializePayload(new WorkbookExportJobPayload(scenarioVersionId))),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartStoreProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "store-profile-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartStoreProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "store-profile-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartProductProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "product-profile-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartProductProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "product-profile-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartInventoryProfileImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "inventory-profile-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartInventoryProfileExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "inventory-profile-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartPricingPolicyImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "pricing-policy-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartPricingPolicyExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "pricing-policy-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartSeasonalityEventImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "seasonality-event-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartSeasonalityEventExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "seasonality-event-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartVendorSupplyImportJobAsync(byte[] workbookBytes, string fileName, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "vendor-supply-import",
                userId,
                SerializePayload(new MasterDataImportJobPayload()),
                workbookBytes,
                fileName),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartVendorSupplyExportJobAsync(string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "master-data",
                "vendor-supply-export",
                userId,
                SerializePayload(new MasterDataExportJobPayload())),
            cancellationToken);

    public Task<AsyncJobStatusResponse> StartReconciliationJobAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken) =>
        RequireAsyncJobManager().EnqueueAsync(
            new PlanningAsyncJobRequest(
                "planning",
                "reconciliation",
                userId,
                SerializePayload(new ReconciliationJobPayload(scenarioVersionId, false))),
            cancellationToken);

    public Task<AsyncJobStatusResponse> GetAsyncJobStatusAsync(string jobId, CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().GetStatusAsync(jobId, cancellationToken);
    }

    public Task<(byte[] Content, string FileName, string ContentType)> DownloadAsyncJobResultAsync(string jobId, CancellationToken cancellationToken)
    {
        return RequireAsyncJobManager().DownloadResultAsync(jobId, cancellationToken);
    }

    private IPlanningAsyncJobManager RequireAsyncJobManager()
    {
        return _asyncJobManager ?? throw new InvalidOperationException("Async job manager is not configured.");
    }

    private static string SerializePayload<TPayload>(TPayload payload)
    {
        return JsonSerializer.Serialize(payload);
    }

    private static byte[]? DecodeBase64OrNull(string? base64)
    {
        return string.IsNullOrWhiteSpace(base64) ? null : Convert.FromBase64String(base64);
    }

    private const string SpreadsheetContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
}
