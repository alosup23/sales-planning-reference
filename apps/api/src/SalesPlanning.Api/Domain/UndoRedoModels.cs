namespace SalesPlanning.Api.Domain;

public sealed record PlanningCellState(
    decimal? InputValue,
    decimal? OverrideValue,
    bool IsSystemGeneratedOverride,
    decimal DerivedValue,
    decimal EffectiveValue,
    decimal GrowthFactor,
    bool IsLocked,
    string? LockReason,
    string? LockedBy,
    long RowVersion,
    string CellKind)
{
    public static PlanningCellState FromCell(PlanningCell cell) =>
        new(
            cell.InputValue,
            cell.OverrideValue,
            cell.IsSystemGeneratedOverride,
            cell.DerivedValue,
            cell.EffectiveValue,
            cell.GrowthFactor,
            cell.IsLocked,
            cell.LockReason,
            cell.LockedBy,
            cell.RowVersion,
            cell.CellKind);

    public PlanningCell ToPlanningCell(PlanningCellCoordinate coordinate) =>
        new()
        {
            Coordinate = coordinate,
            InputValue = InputValue,
            OverrideValue = OverrideValue,
            IsSystemGeneratedOverride = IsSystemGeneratedOverride,
            DerivedValue = DerivedValue,
            EffectiveValue = EffectiveValue,
            GrowthFactor = GrowthFactor,
            IsLocked = IsLocked,
            LockReason = LockReason,
            LockedBy = LockedBy,
            RowVersion = RowVersion,
            CellKind = CellKind
        };
}

public sealed record PlanningCommandCellDelta(
    PlanningCellCoordinate Coordinate,
    PlanningCellState OldState,
    PlanningCellState NewState,
    string ChangeKind);

public sealed record PlanningCommandBatch(
    long CommandBatchId,
    long ScenarioVersionId,
    string UserId,
    string CommandKind,
    string? CommandScopeJson,
    bool IsUndone,
    long? SupersededByBatchId,
    DateTimeOffset CreatedAt,
    DateTimeOffset? UndoneAt,
    IReadOnlyList<PlanningCommandCellDelta> Deltas);

public sealed record PlanningUndoRedoAvailability(
    bool CanUndo,
    bool CanRedo,
    int UndoDepth,
    int RedoDepth,
    int Limit);
