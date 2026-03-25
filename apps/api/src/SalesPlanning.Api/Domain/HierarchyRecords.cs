namespace SalesPlanning.Api.Domain;

public sealed record HierarchyDepartmentRecord(
    string DepartmentLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    IReadOnlyList<HierarchyClassRecord> Classes);

public sealed record HierarchyClassRecord(
    string ClassLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    IReadOnlyList<HierarchySubclassRecord> Subclasses);

public sealed record HierarchySubclassRecord(
    string SubclassLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId);
