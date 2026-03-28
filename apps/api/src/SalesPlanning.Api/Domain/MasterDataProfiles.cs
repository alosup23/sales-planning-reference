namespace SalesPlanning.Api.Domain;

public sealed record InventoryProfileRecord(
    long InventoryProfileId,
    string StoreCode,
    string ProductCode,
    decimal StartingInventory,
    decimal? InboundQty,
    decimal? ReservedQty,
    decimal? ProjectedStockOnHand,
    decimal? SafetyStock,
    decimal? WeeksOfCoverTarget,
    decimal? SellThroughTargetPct,
    bool IsActive);

public sealed record PricingPolicyRecord(
    long PricingPolicyId,
    string? Department,
    string? ClassLabel,
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

public sealed record SeasonalityEventProfileRecord(
    long SeasonalityEventProfileId,
    string? Department,
    string? ClassLabel,
    string? Subclass,
    string? SeasonCode,
    string? EventCode,
    int Month,
    decimal Weight,
    string? PromoWindow,
    bool PeakFlag,
    bool IsActive);

public sealed record VendorSupplyProfileRecord(
    long VendorSupplyProfileId,
    string Supplier,
    string? Brand,
    int? LeadTimeDays,
    int? Moq,
    int? CasePack,
    string? ReplenishmentType,
    string? PaymentTerms,
    bool IsActive);
