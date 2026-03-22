namespace SalesPlanning.Api.Domain;

public readonly record struct PlanningCellCoordinate(
    long ScenarioVersionId,
    long MeasureId,
    long StoreId,
    long ProductNodeId,
    long TimePeriodId)
{
    public string Key => $"{ScenarioVersionId}:{MeasureId}:{StoreId}:{ProductNodeId}:{TimePeriodId}";
}

