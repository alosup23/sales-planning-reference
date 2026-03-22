namespace SalesPlanning.Api.Domain;

public sealed record PlanningActionAudit(
    long ActionId,
    string ActionType,
    string Method,
    string UserId,
    string? Comment,
    DateTimeOffset CreatedAt,
    IReadOnlyList<PlanningCellDeltaAudit> Deltas);

public sealed record PlanningCellDeltaAudit(
    PlanningCellCoordinate Coordinate,
    decimal OldValue,
    decimal NewValue,
    bool WasLocked,
    string ChangeKind);

