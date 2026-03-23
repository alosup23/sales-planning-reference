namespace SalesPlanning.Api.Contracts;

public sealed record HierarchyMappingResponse(
    IReadOnlyList<HierarchyCategoryDto> Categories);

public sealed record HierarchyCategoryDto(
    string CategoryLabel,
    IReadOnlyList<string> SubcategoryLabels);

public sealed record AddHierarchyCategoryRequest(string CategoryLabel);

public sealed record AddHierarchySubcategoryRequest(string CategoryLabel, string SubcategoryLabel);
