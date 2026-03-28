namespace SalesPlanning.Api.Contracts;

public sealed record SplashRequest(
    long ScenarioVersionId,
    long MeasureId,
    SplashCoordinateDto SourceCell,
    decimal TotalValue,
    string Method,
    int RoundingScale,
    string? Comment,
    Dictionary<long, decimal>? ManualWeights,
    IReadOnlyList<SplashScopeRootDto>? ScopeRoots);

public sealed record SplashCoordinateDto(long StoreId, long ProductNodeId, long TimePeriodId);
public sealed record SplashScopeRootDto(long StoreId, long ProductNodeId);

public sealed record SplashResponse(
    long ActionId,
    string Status,
    int CellsUpdated,
    int LockedCellsSkipped,
    PlanningGridPatchDto? Patch,
    UndoRedoAvailabilityDto Availability);
