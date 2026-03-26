namespace SalesPlanning.Api.Contracts;

public sealed record ProductProfileDto(
    string SkuVariant,
    string Description,
    string? Description2,
    decimal Price,
    decimal Cost,
    string DptNo,
    string ClssNo,
    string? BrandNo,
    string Department,
    string Class,
    string? Brand,
    string? RevDepartment,
    string? RevClass,
    string Subclass,
    string? ProdGroup,
    string? ProdType,
    string? ActiveFlag,
    string? OrderFlag,
    string? BrandType,
    string? LaunchMonth,
    string? Gender,
    string? Size,
    string? Collection,
    string? Promo,
    string? RamadhanPromo,
    bool IsActive);

public sealed record ProductProfileResponse(
    IReadOnlyList<ProductProfileDto> Profiles,
    int TotalCount,
    int PageNumber,
    int PageSize,
    string? SearchTerm);

public sealed record UpsertProductProfileRequest(
    string SkuVariant,
    string Description,
    string? Description2,
    decimal Price,
    decimal Cost,
    string DptNo,
    string ClssNo,
    string? BrandNo,
    string Department,
    string Class,
    string? Brand,
    string? RevDepartment,
    string? RevClass,
    string Subclass,
    string? ProdGroup,
    string? ProdType,
    string? ActiveFlag,
    string? OrderFlag,
    string? BrandType,
    string? LaunchMonth,
    string? Gender,
    string? Size,
    string? Collection,
    string? Promo,
    string? RamadhanPromo,
    bool IsActive);

public sealed record DeleteProductProfileRequest(string SkuVariant);

public sealed record InactivateProductProfileRequest(string SkuVariant);

public sealed record ProductProfileImportResponse(
    int RowsProcessed,
    int ProductsAdded,
    int ProductsUpdated,
    int HierarchyRowsProcessed,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);

public sealed record ProductProfileOptionDto(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record ProductProfileOptionsResponse(IReadOnlyList<ProductProfileOptionDto> Options);

public sealed record UpsertProductProfileOptionRequest(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record DeleteProductProfileOptionRequest(
    string FieldName,
    string Value);

public sealed record ProductHierarchyCatalogDto(
    string DptNo,
    string ClssNo,
    string Department,
    string Class,
    string ProdGroup,
    bool IsActive);

public sealed record ProductHierarchySubclassDto(
    string Department,
    string Class,
    string Subclass,
    bool IsActive);

public sealed record ProductHierarchyResponse(
    IReadOnlyList<ProductHierarchyCatalogDto> HierarchyRows,
    IReadOnlyList<ProductHierarchySubclassDto> SubclassRows);

public sealed record UpsertProductHierarchyRequest(
    string DptNo,
    string ClssNo,
    string Department,
    string Class,
    string ProdGroup,
    bool IsActive);

public sealed record DeleteProductHierarchyRequest(
    string DptNo,
    string ClssNo);
