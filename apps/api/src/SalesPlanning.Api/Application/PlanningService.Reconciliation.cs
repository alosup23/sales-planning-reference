using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    public async Task<ReconciliationReportResponse> RunReconciliationAsync(long scenarioVersionId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var cells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
        var mismatches = new List<ReconciliationMismatchDto>();
        var cellLookup = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var checkedCellCount = 0;

        foreach (var node in metadata.ProductNodes.Values.Where(node => !node.IsLeaf))
        {
            var directChildren = metadata.ProductNodes.Values
                .Where(candidate => candidate.ParentProductNodeId == node.ProductNodeId)
                .ToList();
            if (directChildren.Count == 0)
            {
                continue;
            }

            foreach (var period in metadata.TimePeriods.Values)
            {
                checkedCellCount += ReconcileMeasureRollup(
                    scenarioVersionId,
                    node,
                    directChildren,
                    period.TimePeriodId,
                    PlanningMeasures.SalesRevenue,
                    metadata,
                    cellLookup,
                    mismatches);
                checkedCellCount += ReconcileMeasureRollup(
                    scenarioVersionId,
                    node,
                    directChildren,
                    period.TimePeriodId,
                    PlanningMeasures.SoldQuantity,
                    metadata,
                    cellLookup,
                    mismatches);
                checkedCellCount += ReconcileMeasureRollup(
                    scenarioVersionId,
                    node,
                    directChildren,
                    period.TimePeriodId,
                    PlanningMeasures.TotalCosts,
                    metadata,
                    cellLookup,
                    mismatches);
                checkedCellCount += ReconcileMeasureRollup(
                    scenarioVersionId,
                    node,
                    directChildren,
                    period.TimePeriodId,
                    PlanningMeasures.GrossProfit,
                    metadata,
                    cellLookup,
                    mismatches);
            }
        }

        foreach (var node in metadata.ProductNodes.Values)
        {
            foreach (var yearPeriod in metadata.TimePeriods.Values.Where(period => string.Equals(period.Grain, "year", StringComparison.OrdinalIgnoreCase)))
            {
                var monthPeriods = metadata.TimePeriods.Values
                    .Where(period => period.ParentTimePeriodId == yearPeriod.TimePeriodId)
                    .ToList();
                if (monthPeriods.Count == 0)
                {
                    continue;
                }

                checkedCellCount += ReconcileTimeRollup(scenarioVersionId, node, yearPeriod.TimePeriodId, monthPeriods, PlanningMeasures.SalesRevenue, cellLookup, mismatches);
                checkedCellCount += ReconcileTimeRollup(scenarioVersionId, node, yearPeriod.TimePeriodId, monthPeriods, PlanningMeasures.SoldQuantity, cellLookup, mismatches);
                checkedCellCount += ReconcileTimeRollup(scenarioVersionId, node, yearPeriod.TimePeriodId, monthPeriods, PlanningMeasures.TotalCosts, cellLookup, mismatches);
                checkedCellCount += ReconcileTimeRollup(scenarioVersionId, node, yearPeriod.TimePeriodId, monthPeriods, PlanningMeasures.GrossProfit, cellLookup, mismatches);
            }
        }

        return new ReconciliationReportResponse(
            scenarioVersionId,
            checkedCellCount,
            mismatches.Count,
            mismatches,
            mismatches.Count == 0 ? "passed" : "failed");
    }

    private static int ReconcileMeasureRollup(
        long scenarioVersionId,
        ProductNode node,
        IReadOnlyList<ProductNode> directChildren,
        long timePeriodId,
        long measureId,
        PlanningMetadataSnapshot metadata,
        IReadOnlyDictionary<string, PlanningCell> cellLookup,
        ICollection<ReconciliationMismatchDto> mismatches)
    {
        var parentCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, timePeriodId);
        if (!cellLookup.TryGetValue(parentCoordinate.Key, out var parentCell))
        {
            return 0;
        }

        var expected = directChildren.Sum(child =>
            cellLookup.TryGetValue(new PlanningCellCoordinate(scenarioVersionId, measureId, child.StoreId, child.ProductNodeId, timePeriodId).Key, out var childCell)
                ? childCell.EffectiveValue
                : 0m);
        if (!ValuesMatch(parentCell.EffectiveValue, expected))
        {
            mismatches.Add(new ReconciliationMismatchDto(
                "product-rollup",
                node.StoreId,
                node.ProductNodeId,
                timePeriodId,
                measureId,
                expected,
                parentCell.EffectiveValue,
                parentCell.EffectiveValue - expected,
                $"Product rollup mismatch for {node.Label}."));
        }

        return 1;
    }

    private static int ReconcileTimeRollup(
        long scenarioVersionId,
        ProductNode node,
        long yearTimePeriodId,
        IReadOnlyList<TimePeriodNode> monthPeriods,
        long measureId,
        IReadOnlyDictionary<string, PlanningCell> cellLookup,
        ICollection<ReconciliationMismatchDto> mismatches)
    {
        var yearCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, yearTimePeriodId);
        if (!cellLookup.TryGetValue(yearCoordinate.Key, out var yearCell))
        {
            return 0;
        }

        var expected = monthPeriods.Sum(period =>
            cellLookup.TryGetValue(new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, period.TimePeriodId).Key, out var childCell)
                ? childCell.EffectiveValue
                : 0m);
        if (!ValuesMatch(yearCell.EffectiveValue, expected))
        {
            mismatches.Add(new ReconciliationMismatchDto(
                "time-rollup",
                node.StoreId,
                node.ProductNodeId,
                yearTimePeriodId,
                measureId,
                expected,
                yearCell.EffectiveValue,
                yearCell.EffectiveValue - expected,
                $"Time rollup mismatch for {node.Label}."));
        }

        return 1;
    }

    private static bool ValuesMatch(decimal left, decimal right)
    {
        return Math.Abs(left - right) <= 0.01m;
    }
}
