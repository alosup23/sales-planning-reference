namespace SalesPlanning.Api.Application;

public sealed record WorkbookImportJobPayload(long ScenarioVersionId);
public sealed record WorkbookExportJobPayload(long ScenarioVersionId);
public sealed record MasterDataImportJobPayload();
public sealed record MasterDataExportJobPayload();
public sealed record ReconciliationJobPayload(long ScenarioVersionId, bool Scheduled);
