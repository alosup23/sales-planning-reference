namespace SalesPlanning.Api.Contracts;

public sealed record EditCellsRequest(
    long ScenarioVersionId,
    long MeasureId,
    string? Comment,
    IReadOnlyList<EditCellRequest> Cells);

public sealed record EditCellRequest(
    long StoreId,
    long ProductNodeId,
    long TimePeriodId,
    decimal NewValue,
    string EditMode,
    long? RowVersion);

public sealed record EditCellsResponse(long ActionId, int UpdatedCellCount, string Status);

