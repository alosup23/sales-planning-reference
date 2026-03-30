namespace SalesPlanning.Api.Contracts;

public sealed record GridSliceResponse(
    long ScenarioVersionId,
    IReadOnlyList<GridMeasureDto> Measures,
    IReadOnlyList<GridPeriodDto> Periods,
    IReadOnlyList<GridRowDto> Rows);

public sealed record GridBranchResponse(
    long ScenarioVersionId,
    long ParentProductNodeId,
    IReadOnlyList<GridRowDto> Rows);

public sealed record GridViewBlockResponse(
    long ScenarioVersionId,
    string ParentViewRowId,
    IReadOnlyList<GridRowDto> Rows);

public sealed record GridMeasureDto(long MeasureId, string Label, int DecimalPlaces, bool DerivedAtAggregateLevels, bool DisplayAsPercent, bool EditableAtLeaf, bool EditableAtAggregate);

public sealed record GridPeriodDto(long TimePeriodId, string Label, string Grain, long? ParentTimePeriodId, int SortOrder);

public sealed record GridRowDto(
    long StoreId,
    long ProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf,
    string NodeKind,
    string StoreLabel,
    string ClusterLabel,
    string RegionLabel,
    string LifecycleState,
    string? RampProfileCode,
    long? EffectiveFromTimePeriodId,
    long? EffectiveToTimePeriodId,
    Dictionary<long, GridPeriodCellDto> Cells,
    string? ViewRowId = null,
    string? StructureRole = null,
    long? BindingStoreId = null,
    long? BindingProductNodeId = null,
    IReadOnlyList<GridScopeRootDto>? SplashRoots = null);

public sealed record GridPeriodCellDto(Dictionary<long, GridCellDto> Measures);

public sealed record GridCellDto(
    decimal Value,
    decimal GrowthFactor,
    bool IsLocked,
    bool IsCalculated,
    bool IsOverride,
    long RowVersion,
    string CellKind);

public sealed record GridScopeRootDto(long StoreId, long ProductNodeId);
