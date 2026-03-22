namespace SalesPlanning.Api.Contracts;

public sealed record GridSliceResponse(
    long ScenarioVersionId,
    long MeasureId,
    IReadOnlyList<GridPeriodDto> Periods,
    IReadOnlyList<GridRowDto> Rows);

public sealed record GridPeriodDto(long TimePeriodId, string Label, string Grain, long? ParentTimePeriodId, int SortOrder);

public sealed record GridRowDto(
    long StoreId,
    long ProductNodeId,
    string Label,
    int Level,
    string[] Path,
    bool IsLeaf,
    Dictionary<long, GridCellDto> Cells);

public sealed record GridCellDto(
    decimal Value,
    bool IsLocked,
    bool IsCalculated,
    bool IsOverride,
    long RowVersion,
    string CellKind);

