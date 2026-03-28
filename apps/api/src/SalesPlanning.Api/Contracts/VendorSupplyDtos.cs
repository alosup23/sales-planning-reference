namespace SalesPlanning.Api.Contracts;

public sealed record VendorSupplyProfileDto(
    long? VendorSupplyProfileId,
    string Supplier,
    string? Brand,
    int? LeadTimeDays,
    int? Moq,
    int? CasePack,
    string? ReplenishmentType,
    string? PaymentTerms,
    bool IsActive);

public sealed record VendorSupplyProfileResponse(
    IReadOnlyList<VendorSupplyProfileDto> Profiles,
    int TotalCount,
    int PageNumber,
    int PageSize,
    string? SearchTerm);

public sealed record UpsertVendorSupplyProfileRequest(
    long? VendorSupplyProfileId,
    string Supplier,
    string? Brand,
    int? LeadTimeDays,
    int? Moq,
    int? CasePack,
    string? ReplenishmentType,
    string? PaymentTerms,
    bool IsActive);

public sealed record DeleteVendorSupplyProfileRequest(long VendorSupplyProfileId);

public sealed record VendorSupplyProfileImportResponse(
    int RowsProcessed,
    int RecordsAdded,
    int RecordsUpdated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);
