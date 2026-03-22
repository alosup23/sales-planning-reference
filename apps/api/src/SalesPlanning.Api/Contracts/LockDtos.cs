namespace SalesPlanning.Api.Contracts;

public sealed record LockCellsRequest(
    long ScenarioVersionId,
    long MeasureId,
    bool Locked,
    string? Reason,
    IReadOnlyList<LockCoordinateDto> Coordinates);

public sealed record LockCoordinateDto(long StoreId, long ProductNodeId, long TimePeriodId);

public sealed record LockCellsResponse(int UpdatedCellCount, bool Locked);

