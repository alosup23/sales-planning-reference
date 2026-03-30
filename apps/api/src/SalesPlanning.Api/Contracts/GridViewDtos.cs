namespace SalesPlanning.Api.Contracts;

public sealed record PlanningGridViewRequest(
    long ScenarioVersionId,
    string View,
    long? SelectedStoreId,
    string? SelectedDepartmentLabel,
    string? DepartmentLayout,
    bool ExpandAllBranches);

public sealed record PlanningDepartmentScopeResponse(
    IReadOnlyList<string> Departments);
