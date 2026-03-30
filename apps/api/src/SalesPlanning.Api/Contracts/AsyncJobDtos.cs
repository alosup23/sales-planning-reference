namespace SalesPlanning.Api.Contracts;

public sealed record AsyncJobStatusResponse(
    string JobId,
    string Category,
    string Operation,
    string Status,
    int ProgressPercent,
    string ProgressMessage,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    string? ErrorMessage,
    AsyncJobSummaryDto? Summary,
    bool HasDownload,
    string? DownloadFileName,
    string? DownloadContentType);

public sealed record AsyncJobSummaryDto(
    int? RowsProcessed,
    int? CellsUpdated,
    int? RowsCreated,
    int? RecordsAdded,
    int? RecordsUpdated,
    int? MismatchCount,
    int? CheckedCellCount,
    string? ResultMessage);

public sealed record StartAsyncJobResponse(string JobId);
