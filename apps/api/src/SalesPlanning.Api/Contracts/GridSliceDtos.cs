namespace SalesPlanning.Api.Contracts;

public sealed record GridSliceResponse(
    long ScenarioVersionId,
    IReadOnlyList<GridMeasureDto> Measures,
    IReadOnlyList<GridPeriodDto> Periods,
    IReadOnlyList<GridRowDto> Rows);

public sealed record GridMeasureDto(long MeasureId, string Label, int DecimalPlaces, bool DerivedAtAggregateLevels);

public sealed record GridPeriodDto(long TimePeriodId, string Label, string Grain, long? ParentTimePeriodId, int SortOrder);

public sealed record GridRowDto(
    long StoreId,
    long ProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf,
    Dictionary<long, GridPeriodCellDto> Cells);

public sealed record GridPeriodCellDto(Dictionary<long, GridCellDto> Measures);

public sealed record GridCellDto(
    decimal Value,
    bool IsLocked,
    bool IsCalculated,
    bool IsOverride,
    long RowVersion,
    string CellKind);
