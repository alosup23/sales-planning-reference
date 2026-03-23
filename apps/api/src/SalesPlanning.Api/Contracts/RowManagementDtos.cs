namespace SalesPlanning.Api.Contracts;

public sealed record AddRowRequest(
    long ScenarioVersionId,
    long MeasureId,
    string Level,
    long? ParentProductNodeId,
    string Label);

public sealed record AddRowResponse(
    long StoreId,
    long ProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf);
