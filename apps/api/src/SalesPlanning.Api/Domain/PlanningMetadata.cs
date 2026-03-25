namespace SalesPlanning.Api.Domain;

public sealed record ProductNode(
    long ProductNodeId,
    long StoreId,
    long? ParentProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf,
    string NodeKind,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId);

public sealed record StoreNodeMetadata(
    long StoreId,
    string StoreLabel,
    string ClusterLabel,
    string RegionLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId);

public sealed record TimePeriodNode(long TimePeriodId, long? ParentTimePeriodId, string Label, string Grain, int SortOrder);

public sealed record PlanningMetadataSnapshot(
    IReadOnlyDictionary<long, ProductNode> ProductNodes,
    IReadOnlyDictionary<long, TimePeriodNode> TimePeriods,
    IReadOnlyDictionary<long, StoreNodeMetadata> Stores);
