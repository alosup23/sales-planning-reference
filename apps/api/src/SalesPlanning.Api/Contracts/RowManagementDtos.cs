namespace SalesPlanning.Api.Contracts;

public sealed record AddRowRequest(
    long ScenarioVersionId,
    string Level,
    long? ParentProductNodeId,
    string Label,
    long? CopyFromStoreId);

public sealed record AddRowResponse(
    long StoreId,
    long ProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf);

public sealed record DeleteRowRequest(
    long ScenarioVersionId,
    long ProductNodeId);

public sealed record DeleteYearRequest(
    long ScenarioVersionId,
    long YearTimePeriodId);

public sealed record DeleteEntityResponse(
    int DeletedNodeCount,
    int DeletedCellCount,
    string Status);
