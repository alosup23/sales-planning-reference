namespace SalesPlanning.Api.Domain;

public sealed class PlanningCell
{
    public PlanningCellCoordinate Coordinate { get; init; }
    public decimal? InputValue { get; set; }
    public decimal? OverrideValue { get; set; }
    public bool IsSystemGeneratedOverride { get; set; }
    public decimal DerivedValue { get; set; }
    public decimal EffectiveValue { get; set; }
    public bool IsLocked { get; set; }
    public string? LockReason { get; set; }
    public string? LockedBy { get; set; }
    public long RowVersion { get; set; }
    public string CellKind { get; set; } = "leaf";

    public PlanningCell Clone()
    {
        return new PlanningCell
        {
            Coordinate = Coordinate,
            InputValue = InputValue,
            OverrideValue = OverrideValue,
            IsSystemGeneratedOverride = IsSystemGeneratedOverride,
            DerivedValue = DerivedValue,
            EffectiveValue = EffectiveValue,
            IsLocked = IsLocked,
            LockReason = LockReason,
            LockedBy = LockedBy,
            RowVersion = RowVersion,
            CellKind = CellKind
        };
    }
}
