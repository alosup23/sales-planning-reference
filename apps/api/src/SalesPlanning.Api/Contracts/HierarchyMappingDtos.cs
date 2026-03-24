namespace SalesPlanning.Api.Contracts;

public sealed record HierarchyMappingResponse(
    IReadOnlyList<HierarchyDepartmentDto> Departments);

public sealed record HierarchyDepartmentDto(
    string DepartmentLabel,
    IReadOnlyList<string> ClassLabels);

public sealed record AddHierarchyDepartmentRequest(string DepartmentLabel);

public sealed record AddHierarchyClassRequest(string DepartmentLabel, string ClassLabel);
