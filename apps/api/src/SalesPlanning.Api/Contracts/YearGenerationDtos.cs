namespace SalesPlanning.Api.Contracts;

public sealed record GenerateNextYearRequest(long ScenarioVersionId, long SourceYearTimePeriodId);

public sealed record GenerateNextYearResponse(long SourceYearTimePeriodId, long GeneratedYearTimePeriodId, string Status, int CellsCopied);
