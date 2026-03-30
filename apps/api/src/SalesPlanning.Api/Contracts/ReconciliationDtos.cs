namespace SalesPlanning.Api.Contracts;

public sealed record ReconciliationReportResponse(
    long ScenarioVersionId,
    int CheckedCellCount,
    int MismatchCount,
    IReadOnlyList<ReconciliationMismatchDto> Mismatches,
    string Status);

public sealed record ReconciliationMismatchDto(
    string Dimension,
    long StoreId,
    long ProductNodeId,
    long TimePeriodId,
    long MeasureId,
    decimal ExpectedValue,
    decimal ActualValue,
    decimal Difference,
    string Message);
