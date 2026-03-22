namespace SalesPlanning.Api.Contracts;

public sealed record AuditTrailItemDto(
    long ActionId,
    string ActionType,
    string Method,
    string UserId,
    string? Comment,
    DateTimeOffset CreatedAt,
    IReadOnlyList<AuditCellDeltaDto> Deltas);

public sealed record AuditCellDeltaDto(
    long StoreId,
    long ProductNodeId,
    long TimePeriodId,
    decimal OldValue,
    decimal NewValue,
    string ChangeKind);

