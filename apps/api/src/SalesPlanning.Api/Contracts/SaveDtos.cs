namespace SalesPlanning.Api.Contracts;

public sealed record SaveScenarioRequest(long ScenarioVersionId, string Mode);

public sealed record SaveScenarioResponse(string Status, string Mode, DateTimeOffset SavedAt);
