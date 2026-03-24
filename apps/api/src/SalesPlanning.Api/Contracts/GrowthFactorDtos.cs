namespace SalesPlanning.Api.Contracts;

public sealed record ApplyGrowthFactorRequest(
    long ScenarioVersionId,
    long MeasureId,
    SplashCoordinateDto SourceCell,
    decimal CurrentValue,
    decimal GrowthFactor,
    string? Comment,
    IReadOnlyList<SplashScopeRootDto>? ScopeRoots);

public sealed record ApplyGrowthFactorResponse(long ActionId, string Status, decimal GrowthFactor, int UpdatedCellCount);
