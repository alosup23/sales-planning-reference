namespace SalesPlanning.Api.Contracts;

public sealed record InventoryProfileDto(
    long? InventoryProfileId,
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

public sealed record InventoryProfileResponse(
    IReadOnlyList<InventoryProfileDto> Profiles,
    int TotalCount,
    int PageNumber,
    int PageSize,
    string? SearchTerm);

public sealed record UpsertInventoryProfileRequest(
    long? InventoryProfileId,
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

public sealed record DeleteInventoryProfileRequest(long InventoryProfileId);

public sealed record InactivateInventoryProfileRequest(long InventoryProfileId);

public sealed record InventoryProfileImportResponse(
    int RowsProcessed,
    int RecordsAdded,
    int RecordsUpdated,
    string Status,
    string? ExceptionFileName,
    string? ExceptionWorkbookBase64);
