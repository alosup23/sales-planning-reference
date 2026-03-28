namespace SalesPlanning.Api.Contracts;

public sealed record PricingPolicyDto(
    long? PricingPolicyId,
    string? Department,
    string? Class,
    string? Subclass,
    string? Brand,
    string? PriceLadderGroup,
    decimal? MinPrice,
    decimal? MaxPrice,
    decimal? MarkdownFloorPrice,
    decimal? MinimumMarginPct,
    bool KviFlag,
    bool MarkdownEligible,
    bool IsActive);

public sealed record PricingPolicyResponse(
    IReadOnlyList<PricingPolicyDto> Policies,
    int TotalCount,
    int PageNumber,
    int PageSize,
    string? SearchTerm);

public sealed record UpsertPricingPolicyRequest(
    long? PricingPolicyId,
    string? Department,
    string? Class,
    string? Subclass,
    string? Brand,
    string? PriceLadderGroup,
    decimal? MinPrice,
    decimal? MaxPrice,
    decimal? MarkdownFloorPrice,
    decimal? MinimumMarginPct,
    bool KviFlag,
    bool MarkdownEligible,
    bool IsActive);

public sealed record DeletePricingPolicyRequest(long PricingPolicyId);

public sealed record InactivatePricingPolicyRequest(long PricingPolicyId);

public sealed record PricingPolicyImportResponse(
    int RowsProcessed,
    int RecordsAdded,
    int RecordsUpdated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);
