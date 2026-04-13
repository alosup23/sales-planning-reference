using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    private sealed record PlanningMutationInstruction(
        long ScenarioVersionId,
        long MeasureId,
        long SourceTimePeriodId,
        bool IsDirectLeafEdit,
        bool UseProductDeltaRollup,
        string Method,
        IReadOnlyList<SplashScopeRootDto> ScopeRoots,
        IReadOnlyList<long> TargetLeafProductIds,
        IReadOnlyList<long> TargetLeafTimeIds,
        IReadOnlyList<long> AggregateProductNodeIds,
        IReadOnlyList<long> AggregateTimePeriodIds,
        IReadOnlyList<long> LoadedProductNodeIds,
        IReadOnlyList<long> LoadedTimePeriodIds);

    private async Task<IReadOnlyList<PlanningCell>> LoadWorkingSetCellsAsync(
        long scenarioVersionId,
        string userId,
        PlanningMetadataSnapshot metadata,
        IReadOnlyCollection<PlanningMutationInstruction> instructions,
        CancellationToken cancellationToken)
    {
        var coordinates = BuildWorkingSetCoordinates(scenarioVersionId, metadata, instructions);
        return await LoadEffectiveCellsAsync(scenarioVersionId, userId, metadata, coordinates, cancellationToken);
    }

    private static PlanningMutationInstruction BuildEditInstruction(
        long scenarioVersionId,
        long measureId,
        long storeId,
        long productNodeId,
        long timePeriodId,
        bool requestedLeafInput,
        PlanningMetadataSnapshot metadata)
    {
        var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId);
        if (requestedLeafInput && IsLeafWriteCoordinate(coordinate, metadata))
        {
            return BuildInstruction(
                scenarioVersionId,
                measureId,
                timePeriodId,
                true,
                "input",
                [new SplashScopeRootDto(storeId, productNodeId)],
                [productNodeId],
                [timePeriodId],
                metadata);
        }

        var method = metadata.TimePeriods.Values.Any(period => period.ParentTimePeriodId == timePeriodId)
            ? "seasonality_profile"
            : "existing_plan";
        return BuildSplashInstruction(
            scenarioVersionId,
            measureId,
            timePeriodId,
            [new SplashScopeRootDto(storeId, productNodeId)],
            metadata,
            method);
    }

    private static PlanningMutationInstruction BuildSplashInstruction(
        long scenarioVersionId,
        long measureId,
        long sourceTimePeriodId,
        IReadOnlyList<SplashScopeRootDto> scopeRoots,
        PlanningMetadataSnapshot metadata,
        string method)
    {
        var targetLeafProductIds = scopeRoots
            .SelectMany(root => GetLeafProductIds(root.ProductNodeId, metadata))
            .Distinct()
            .ToList();
        var targetLeafTimeIds = GetLeafTimeIds(sourceTimePeriodId, metadata)
            .Distinct()
            .ToList();

        return BuildInstruction(
            scenarioVersionId,
            measureId,
            sourceTimePeriodId,
            false,
            method,
            scopeRoots,
            targetLeafProductIds,
            targetLeafTimeIds,
            metadata);
    }

    private static PlanningMutationInstruction BuildInstruction(
        long scenarioVersionId,
        long measureId,
        long sourceTimePeriodId,
        bool isDirectLeafEdit,
        string method,
        IReadOnlyList<SplashScopeRootDto> scopeRoots,
        IReadOnlyList<long> targetLeafProductIds,
        IReadOnlyList<long> targetLeafTimeIds,
        PlanningMetadataSnapshot metadata)
    {
        var aggregateProductNodeIds = targetLeafProductIds
            .SelectMany(productNodeId => GetAncestorProductIdsExcludingSelf(productNodeId, metadata))
            .Distinct()
            .ToList();
        var aggregateTimePeriodIds = targetLeafTimeIds
            .SelectMany(timePeriodId => GetAncestorTimeIdsExcludingSelf(timePeriodId, metadata))
            .Distinct()
            .ToList();

        var useProductDeltaRollup = targetLeafProductIds.Count == 1
            && scopeRoots.Count == 1
            && scopeRoots[0].ProductNodeId == targetLeafProductIds[0];

        var loadedProductNodeIds = targetLeafProductIds
            .Concat(aggregateProductNodeIds)
            .Concat(useProductDeltaRollup
                ? []
                : aggregateProductNodeIds.SelectMany(productNodeId => GetDirectChildProductIds(productNodeId, metadata)))
            .Distinct()
            .ToList();
        var loadedTimePeriodIds = targetLeafTimeIds
            .Concat(aggregateTimePeriodIds)
            .Concat(aggregateTimePeriodIds.SelectMany(timePeriodId => GetDirectChildTimeIds(timePeriodId, metadata)))
            .Distinct()
            .ToList();

        return new PlanningMutationInstruction(
            scenarioVersionId,
            measureId,
            sourceTimePeriodId,
            isDirectLeafEdit,
            useProductDeltaRollup,
            method,
            scopeRoots,
            targetLeafProductIds,
            targetLeafTimeIds,
            aggregateProductNodeIds,
            aggregateTimePeriodIds,
            loadedProductNodeIds,
            loadedTimePeriodIds);
    }

    private static IReadOnlyList<PlanningCellCoordinate> BuildWorkingSetCoordinates(
        long scenarioVersionId,
        PlanningMetadataSnapshot metadata,
        IReadOnlyCollection<PlanningMutationInstruction> instructions)
    {
        var productNodeIds = instructions
            .SelectMany(instruction => instruction.LoadedProductNodeIds)
            .Distinct()
            .OrderBy(id => id)
            .ToList();
        if (instructions.Any(instruction => !instruction.UseProductDeltaRollup))
        {
            productNodeIds = productNodeIds
                .Concat(instructions.SelectMany(instruction => instruction.AggregateProductNodeIds)
                    .SelectMany(productNodeId => GetDirectChildProductIds(productNodeId, metadata)))
                .Distinct()
                .OrderBy(id => id)
                .ToList();
        }
        var timePeriodIds = instructions
            .SelectMany(instruction => instruction.LoadedTimePeriodIds)
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        var coordinates = new List<PlanningCellCoordinate>(productNodeIds.Count * timePeriodIds.Count * PlanningMeasures.SupportedMeasureIds.Count);
        foreach (var productNodeId in productNodeIds)
        {
            var storeId = metadata.ProductNodes[productNodeId].StoreId;
            foreach (var timePeriodId in timePeriodIds)
            {
                foreach (var measureId in PlanningMeasures.SupportedMeasureIds)
                {
                    coordinates.Add(new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId));
                }
            }
        }

        return coordinates;
    }

    private static IReadOnlyList<PlanningCellCoordinate> BuildImpactedCoordinates(
        long scenarioVersionId,
        PlanningMetadataSnapshot metadata,
        IReadOnlyCollection<PlanningMutationInstruction> instructions)
    {
        var productNodeIds = instructions
            .SelectMany(instruction => instruction.TargetLeafProductIds.Concat(instruction.AggregateProductNodeIds))
            .Distinct()
            .OrderBy(id => id)
            .ToList();
        var timePeriodIds = instructions
            .SelectMany(instruction => instruction.TargetLeafTimeIds.Concat(instruction.AggregateTimePeriodIds))
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        return BuildCoordinatesForMeasures(scenarioVersionId, metadata, productNodeIds, timePeriodIds, PlanningMeasures.SupportedMeasureIds);
    }

    private static IReadOnlyList<PlanningCellCoordinate> BuildCoordinatesForMeasures(
        long scenarioVersionId,
        PlanningMetadataSnapshot metadata,
        IReadOnlyList<long> productNodeIds,
        IReadOnlyList<long> timePeriodIds,
        IReadOnlyList<long> measureIds)
    {
        var coordinates = new List<PlanningCellCoordinate>(productNodeIds.Count * timePeriodIds.Count * measureIds.Count);
        foreach (var productNodeId in productNodeIds)
        {
            var storeId = metadata.ProductNodes[productNodeId].StoreId;
            foreach (var timePeriodId in timePeriodIds)
            {
                foreach (var measureId in measureIds)
                {
                    coordinates.Add(new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId));
                }
            }
        }

        return coordinates;
    }

    private static void RecalculateImpactedCells(
        IReadOnlyList<PlanningCell> originalCells,
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        IReadOnlyCollection<PlanningMutationInstruction> instructions)
    {
        var originalByKey = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var targetLeafProductIds = instructions
            .SelectMany(instruction => instruction.TargetLeafProductIds)
            .Distinct()
            .ToList();
        var aggregateTimePeriodIds = instructions
            .SelectMany(instruction => instruction.AggregateTimePeriodIds)
            .Distinct()
            .OrderByDescending(timePeriodId => GetTimeDepth(timePeriodId, metadata))
            .ToList();

        foreach (var leafProductId in targetLeafProductIds)
        {
            foreach (var aggregateTimePeriodId in aggregateTimePeriodIds)
            {
                RecalculateLeafTimeAggregate(workingCells, metadata, scenarioVersionId, leafProductId, aggregateTimePeriodId);
            }
        }

        var impactedTimeIds = instructions
            .SelectMany(instruction => instruction.TargetLeafTimeIds.Concat(instruction.AggregateTimePeriodIds))
            .Distinct()
            .OrderBy(timePeriodId => metadata.TimePeriods[timePeriodId].SortOrder)
            .ToList();
        var aggregateProductNodeIds = instructions
            .SelectMany(instruction => instruction.AggregateProductNodeIds)
            .Distinct()
            .OrderByDescending(productNodeId => metadata.ProductNodes[productNodeId].Level)
            .ToList();

        if (instructions.All(instruction => instruction.UseProductDeltaRollup))
        {
            RecalculateAggregateProductsByDelta(originalByKey, workingCells, metadata, scenarioVersionId, instructions, aggregateProductNodeIds);
            return;
        }

        foreach (var productNodeId in aggregateProductNodeIds)
        {
            foreach (var timePeriodId in impactedTimeIds)
            {
                RecalculateAggregateProductTime(workingCells, metadata, scenarioVersionId, productNodeId, timePeriodId);
            }
        }
    }

    private static void RecalculateAggregateProductsByDelta(
        IReadOnlyDictionary<string, PlanningCell> originalCells,
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        IReadOnlyCollection<PlanningMutationInstruction> instructions,
        IReadOnlyList<long> aggregateProductNodeIds)
    {
        var deltaByCoordinate = new Dictionary<string, decimal>(StringComparer.Ordinal);

        foreach (var instruction in instructions)
        {
            var leafProductId = instruction.TargetLeafProductIds[0];
            var impactedTimeIds = instruction.TargetLeafTimeIds
                .Concat(instruction.AggregateTimePeriodIds)
                .Distinct()
                .ToList();

            foreach (var timePeriodId in impactedTimeIds)
            {
                foreach (var measureId in new[] { PlanningMeasures.SalesRevenue, PlanningMeasures.SoldQuantity, PlanningMeasures.TotalCosts, PlanningMeasures.GrossProfit })
                {
                    var leafCoordinate = new PlanningCellCoordinate(
                        scenarioVersionId,
                        measureId,
                        metadata.ProductNodes[leafProductId].StoreId,
                        leafProductId,
                        timePeriodId);

                    if (!originalCells.TryGetValue(leafCoordinate.Key, out var originalLeafCell)
                        || !workingCells.TryGetValue(leafCoordinate.Key, out var updatedLeafCell))
                    {
                        continue;
                    }

                    var delta = updatedLeafCell.EffectiveValue - originalLeafCell.EffectiveValue;
                    if (delta == 0m)
                    {
                        continue;
                    }

                    foreach (var ancestorProductNodeId in instruction.AggregateProductNodeIds)
                    {
                        var ancestorCoordinate = new PlanningCellCoordinate(
                            scenarioVersionId,
                            measureId,
                            metadata.ProductNodes[ancestorProductNodeId].StoreId,
                            ancestorProductNodeId,
                            timePeriodId);

                        deltaByCoordinate[ancestorCoordinate.Key] = deltaByCoordinate.TryGetValue(ancestorCoordinate.Key, out var existingDelta)
                            ? existingDelta + delta
                            : delta;
                    }
                }
            }
        }

        foreach (var productNodeId in aggregateProductNodeIds)
        {
            foreach (var timePeriodId in instructions
                .SelectMany(instruction => instruction.TargetLeafTimeIds.Concat(instruction.AggregateTimePeriodIds))
                .Distinct()
                .OrderBy(timeId => metadata.TimePeriods[timeId].SortOrder))
            {
                var storeId = metadata.ProductNodes[productNodeId].StoreId;
                foreach (var measureId in new[] { PlanningMeasures.SalesRevenue, PlanningMeasures.SoldQuantity, PlanningMeasures.TotalCosts, PlanningMeasures.GrossProfit })
                {
                    var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId);
                    if (!originalCells.TryGetValue(coordinate.Key, out var originalCell))
                    {
                        continue;
                    }

                    if (!workingCells.TryGetValue(coordinate.Key, out var workingCell))
                    {
                        continue;
                    }

                    var delta = deltaByCoordinate.GetValueOrDefault(coordinate.Key, 0m);
                    SetAggregateValue(workingCell, originalCell.EffectiveValue + delta);
                }

                RecalculateDerivedRatesForCoordinate(workingCells, scenarioVersionId, storeId, productNodeId, timePeriodId, isLeafMonth: false);
            }
        }
    }

    private static void RecalculateLeafTimeAggregate(
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        long leafProductNodeId,
        long aggregateTimePeriodId)
    {
        var childTimeIds = GetDirectChildTimeIds(aggregateTimePeriodId, metadata);
        if (childTimeIds.Count == 0)
        {
            return;
        }

        var storeId = metadata.ProductNodes[leafProductNodeId].StoreId;
        RecalculateSumMeasuresForProductTimeChildren(workingCells, scenarioVersionId, storeId, leafProductNodeId, aggregateTimePeriodId, childTimeIds);
        RecalculateDerivedRatesForCoordinate(workingCells, scenarioVersionId, storeId, leafProductNodeId, aggregateTimePeriodId, isLeafMonth: false);
    }

    private static void RecalculateAggregateProductTime(
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        long productNodeId,
        long timePeriodId)
    {
        var childProductIds = GetDirectChildProductIds(productNodeId, metadata);
        if (childProductIds.Count == 0)
        {
            return;
        }

        var storeId = metadata.ProductNodes[productNodeId].StoreId;
        RecalculateSumMeasuresForProductChildren(workingCells, scenarioVersionId, storeId, productNodeId, timePeriodId, childProductIds);
        RecalculateDerivedRatesForCoordinate(workingCells, scenarioVersionId, storeId, productNodeId, timePeriodId, isLeafMonth: false);
    }

    private static void RecalculateSumMeasuresForProductTimeChildren(
        IDictionary<string, PlanningCell> workingCells,
        long scenarioVersionId,
        long storeId,
        long productNodeId,
        long aggregateTimePeriodId,
        IReadOnlyList<long> childTimeIds)
    {
        foreach (var measureId in new[] { PlanningMeasures.SalesRevenue, PlanningMeasures.SoldQuantity, PlanningMeasures.TotalCosts, PlanningMeasures.GrossProfit })
        {
            var cell = workingCells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, aggregateTimePeriodId).Key];
            var value = childTimeIds.Sum(childTimeId => workingCells[new PlanningCellCoordinate(
                scenarioVersionId,
                measureId,
                storeId,
                productNodeId,
                childTimeId).Key].EffectiveValue);
            SetAggregateValue(cell, value);
        }
    }

    private static void RecalculateSumMeasuresForProductChildren(
        IDictionary<string, PlanningCell> workingCells,
        long scenarioVersionId,
        long storeId,
        long productNodeId,
        long timePeriodId,
        IReadOnlyList<long> childProductIds)
    {
        foreach (var measureId in new[] { PlanningMeasures.SalesRevenue, PlanningMeasures.SoldQuantity, PlanningMeasures.TotalCosts, PlanningMeasures.GrossProfit })
        {
            var cell = workingCells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId).Key];
            var value = childProductIds.Sum(childProductId => workingCells[new PlanningCellCoordinate(
                scenarioVersionId,
                measureId,
                storeId,
                childProductId,
                timePeriodId).Key].EffectiveValue);
            SetAggregateValue(cell, value);
        }
    }

    private static void RecalculateDerivedRatesForCoordinate(
        IDictionary<string, PlanningCell> workingCells,
        long scenarioVersionId,
        long storeId,
        long productNodeId,
        long timePeriodId,
        bool isLeafMonth)
    {
        var quantity = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, storeId, productNodeId, timePeriodId).Key].EffectiveValue;
        var revenue = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, storeId, productNodeId, timePeriodId).Key].EffectiveValue;
        var totalCosts = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, storeId, productNodeId, timePeriodId).Key].EffectiveValue;

        var aspCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, storeId, productNodeId, timePeriodId).Key];
        var unitCostCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, storeId, productNodeId, timePeriodId).Key];
        var grossProfitPercentCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfitPercent, storeId, productNodeId, timePeriodId).Key];

        var aspValue = quantity > 0m ? PlanningMath.NormalizeAsp(revenue / quantity) : 1.00m;
        var unitCostValue = quantity > 0m ? PlanningMath.NormalizeUnitCost(totalCosts / quantity) : 0m;
        var grossProfitPercentValue = PlanningMath.CalculateGrossProfitPercent(
            quantity > 0m ? revenue / quantity : 1.00m,
            quantity > 0m ? totalCosts / quantity : 0m);

        if (isLeafMonth)
        {
            SetLeafValue(aspCell, aspValue);
            SetLeafValue(unitCostCell, unitCostValue);
            SetCalculatedLeafValue(grossProfitPercentCell, grossProfitPercentValue);
            return;
        }

        SetAggregateValue(aspCell, aspValue);
        SetAggregateValue(unitCostCell, unitCostValue);
        SetAggregateValue(grossProfitPercentCell, grossProfitPercentValue);
    }

    private static IReadOnlyList<long> GetAncestorProductIdsExcludingSelf(long productNodeId, PlanningMetadataSnapshot metadata)
    {
        var ancestors = new List<long>();
        var current = metadata.ProductNodes[productNodeId];
        while (current.ParentProductNodeId is not null)
        {
            current = metadata.ProductNodes[current.ParentProductNodeId.Value];
            ancestors.Add(current.ProductNodeId);
        }

        return ancestors;
    }

    private static IReadOnlyList<long> GetAncestorTimeIdsExcludingSelf(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        var ancestors = new List<long>();
        var current = metadata.TimePeriods[timePeriodId];
        while (current.ParentTimePeriodId is not null)
        {
            current = metadata.TimePeriods[current.ParentTimePeriodId.Value];
            ancestors.Add(current.TimePeriodId);
        }

        return ancestors;
    }

    private static IReadOnlyList<long> GetDirectChildProductIds(long productNodeId, PlanningMetadataSnapshot metadata)
    {
        return metadata.ProductNodes.Values
            .Where(node => node.ParentProductNodeId == productNodeId)
            .Select(node => node.ProductNodeId)
            .Distinct()
            .ToList();
    }

    private static IReadOnlyList<long> GetDirectChildTimeIds(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        return metadata.TimePeriods.Values
            .Where(node => node.ParentTimePeriodId == timePeriodId)
            .Select(node => node.TimePeriodId)
            .Distinct()
            .ToList();
    }
}
