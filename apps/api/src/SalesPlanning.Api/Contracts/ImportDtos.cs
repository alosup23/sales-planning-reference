namespace SalesPlanning.Api.Contracts;

public sealed record ImportWorkbookResponse(
    int RowsProcessed,
    int CellsUpdated,
    int RowsCreated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);
