namespace SalesPlanning.Api.Contracts;

public sealed record PlanningGridPatchDto(
    long ScenarioVersionId,
    IReadOnlyList<GridCellPatchDto> Cells);

public sealed record GridCellPatchDto(
    long StoreId,
    long ProductNodeId,
    long TimePeriodId,
    long MeasureId,
    GridCellDto Cell);
