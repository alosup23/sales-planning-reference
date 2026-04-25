namespace SalesPlanning.Api.Contracts;

public sealed record ApplyGrowthFactorRequest(
    long ScenarioVersionId,
    long MeasureId,
    SplashCoordinateDto SourceCell,
    decimal BaseValue,
    decimal CurrentValue,
    decimal GrowthFactor,
    string? Comment,
    IReadOnlyList<SplashScopeRootDto>? ScopeRoots);

public sealed record ApplyGrowthFactorResponse(
    long ActionId,
    string Status,
    decimal GrowthFactor,
    int UpdatedCellCount,
    PlanningGridPatchDto? Patch,
    UndoRedoAvailabilityDto Availability);
