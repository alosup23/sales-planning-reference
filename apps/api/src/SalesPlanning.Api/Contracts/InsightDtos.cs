namespace SalesPlanning.Api.Contracts;

public sealed record PlanningInsightResponse(
    string ProviderStatus,
    string ScopeLabel,
    string RecommendedForecastModel,
    decimal SeasonalityStrength,
    decimal RecommendedPriceFloor,
    decimal RecommendedPriceTarget,
    decimal RecommendedPriceCeiling,
    decimal GrossProfitOpportunity,
    decimal QuantityOpportunity,
    IReadOnlyList<string> InsightBullets);
