using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public interface IPlanningAsyncJobManager
{
    Task<AsyncJobStatusResponse> EnqueueAsync(PlanningAsyncJobRequest request, CancellationToken cancellationToken);

    Task<AsyncJobStatusResponse> GetStatusAsync(string jobId, CancellationToken cancellationToken);
    Task<(byte[] Content, string FileName, string ContentType)> DownloadResultAsync(string jobId, CancellationToken cancellationToken);
}

public sealed record PlanningAsyncJobRequest(
    string Category,
    string Operation,
    string RequestedBy,
    string PayloadJson,
    byte[]? UploadContent = null,
    string? UploadFileName = null);

public sealed record PlanningAsyncJobProgress(int ProgressPercent, string ProgressMessage);

public sealed record PlanningAsyncJobExecutionResult(
    AsyncJobSummaryDto? Summary,
    byte[]? DownloadContent = null,
    string? DownloadFileName = null,
    string? DownloadContentType = null);
