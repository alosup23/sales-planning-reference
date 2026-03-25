namespace SalesPlanning.Api.Contracts;

public sealed record HierarchyMappingResponse(
    IReadOnlyList<HierarchyDepartmentDto> Departments);

public sealed record HierarchyDepartmentDto(
    string DepartmentLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    IReadOnlyList<HierarchyClassDto> Classes);

public sealed record HierarchyClassDto(
    string ClassLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    IReadOnlyList<HierarchySubclassDto> Subclasses);

public sealed record HierarchySubclassDto(
    string SubclassLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId);

public sealed record AddHierarchyDepartmentRequest(string DepartmentLabel);

public sealed record AddHierarchyClassRequest(string DepartmentLabel, string ClassLabel);

public sealed record AddHierarchySubclassRequest(string DepartmentLabel, string ClassLabel, string SubclassLabel);
