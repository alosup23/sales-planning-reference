namespace SalesPlanning.Api.Contracts;

public sealed record StoreProfileDto(
    long StoreId,
    string StoreCode,
    string BranchName,
    string? State,
    string ClusterLabel,
    decimal? Latitude,
    decimal? Longitude,
    string RegionLabel,
    string? OpeningDate,
    string? Sssg,
    string? SalesType,
    string? Status,
    string? Storey,
    string? BuildingStatus,
    decimal? Gta,
    decimal? Nta,
    string? Rsom,
    string? Dm,
    decimal? Rental,
    string LifecycleState,
    string? RampProfileCode,
    bool IsActive);

public sealed record StoreProfileResponse(
    IReadOnlyList<StoreProfileDto> Stores);

public sealed record UpsertStoreProfileRequest(
    long ScenarioVersionId,
    long? StoreId,
    string StoreCode,
    string BranchName,
    string? State,
    string ClusterLabel,
    decimal? Latitude,
    decimal? Longitude,
    string RegionLabel,
    string? OpeningDate,
    string? Sssg,
    string? SalesType,
    string? Status,
    string? Storey,
    string? BuildingStatus,
    decimal? Gta,
    decimal? Nta,
    string? Rsom,
    string? Dm,
    decimal? Rental,
    string LifecycleState,
    string? RampProfileCode,
    bool IsActive);

public sealed record DeleteStoreProfileRequest(
    long ScenarioVersionId,
    long StoreId);

public sealed record InactivateStoreProfileRequest(
    long StoreId);

public sealed record StoreProfileImportResponse(
    int RowsProcessed,
    int StoresAdded,
    int StoresUpdated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);

public sealed record StoreProfileOptionDto(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record StoreProfileOptionsResponse(
    IReadOnlyList<StoreProfileOptionDto> Options);

public sealed record UpsertStoreProfileOptionRequest(
    string FieldName,
    string Value,
    bool IsActive);

public sealed record DeleteStoreProfileOptionRequest(
    string FieldName,
    string Value);
