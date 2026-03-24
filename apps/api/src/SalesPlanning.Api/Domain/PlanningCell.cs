namespace SalesPlanning.Api.Domain;

public sealed class PlanningCell
{
    public PlanningCellCoordinate Coordinate { get; set; }
    public decimal? InputValue { get; set; }
    public decimal? OverrideValue { get; set; }
    public bool IsSystemGeneratedOverride { get; set; }
    public decimal DerivedValue { get; set; }
    public decimal EffectiveValue { get; set; }
    public decimal GrowthFactor { get; set; } = 1.0m;
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
            GrowthFactor = GrowthFactor,
            IsLocked = IsLocked,
            LockReason = LockReason,
            LockedBy = LockedBy,
            RowVersion = RowVersion,
            CellKind = CellKind
        };
    }
}
