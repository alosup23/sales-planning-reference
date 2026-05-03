namespace SalesPlanning.Api.Contracts;

public sealed record SaveScenarioRequest(long ScenarioVersionId, string Mode);

public sealed record SaveScenarioResponse(string Status, string Mode, DateTimeOffset SavedAt);

public sealed record DiscardDraftRequest(long ScenarioVersionId);

public sealed record DiscardDraftResponse(string Status, long ScenarioVersionId, DateTimeOffset DiscardedAt);
