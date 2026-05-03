using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    private async Task<IReadOnlyList<PlanningCell>> LoadEffectiveCellsAsync(
        long scenarioVersionId,
        string userId,
        IEnumerable<PlanningCellCoordinate> coordinates,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        return await LoadEffectiveCellsAsync(scenarioVersionId, userId, metadata, coordinates, cancellationToken);
    }

    private async Task<IReadOnlyList<PlanningCell>> LoadEffectiveCellsAsync(
        long scenarioVersionId,
        string userId,
        PlanningMetadataSnapshot metadata,
        IEnumerable<PlanningCellCoordinate> coordinates,
        CancellationToken cancellationToken)
    {
        var coordinateList = coordinates
            .DistinctBy(coordinate => coordinate.Key)
            .ToList();
        if (coordinateList.Count == 0)
        {
            return [];
        }

        var baseCellsTask = _repository.GetCellsAsync(coordinateList, cancellationToken);
        var draftCellsTask = _repository.GetDraftCellsAsync(scenarioVersionId, userId, coordinateList, cancellationToken);
        await Task.WhenAll(baseCellsTask, draftCellsTask);

        return HydrateMissingCells(metadata, coordinateList, ApplyDraftOverlay(await baseCellsTask, await draftCellsTask));
    }

    private async Task<IReadOnlyList<PlanningCell>> LoadEffectiveScenarioCellsAsync(
        long scenarioVersionId,
        string userId,
        CancellationToken cancellationToken)
    {
        var baseCells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
        if (baseCells.Count == 0)
        {
            return [];
        }

        var coordinates = baseCells
            .Select(cell => cell.Coordinate)
            .ToList();
        var draftCells = await _repository.GetDraftCellsAsync(scenarioVersionId, userId, coordinates, cancellationToken);
        return ApplyDraftOverlay(baseCells, draftCells);
    }

    private static IReadOnlyList<PlanningCell> ApplyDraftOverlay(
        IReadOnlyList<PlanningCell> baseCells,
        IReadOnlyList<PlanningCell> draftCells)
    {
        var cellsByKey = baseCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone(), StringComparer.Ordinal);

        foreach (var draftCell in draftCells)
        {
            cellsByKey[draftCell.Coordinate.Key] = draftCell.Clone();
        }

        return cellsByKey.Values.ToList();
    }

    private static IReadOnlyList<PlanningCell> HydrateMissingCells(
        PlanningMetadataSnapshot metadata,
        IReadOnlyList<PlanningCellCoordinate> requestedCoordinates,
        IReadOnlyList<PlanningCell> cells)
    {
        var cellsByKey = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone(), StringComparer.Ordinal);
        foreach (var coordinate in requestedCoordinates)
        {
            if (cellsByKey.ContainsKey(coordinate.Key))
            {
                continue;
            }

            cellsByKey[coordinate.Key] = CreateDefaultPlanningCell(metadata, coordinate);
        }

        return cellsByKey.Values.ToList();
    }

    private static PlanningCell CreateDefaultPlanningCell(
        PlanningMetadataSnapshot metadata,
        PlanningCellCoordinate coordinate)
    {
        var node = metadata.ProductNodes[coordinate.ProductNodeId];
        var timePeriod = metadata.TimePeriods[coordinate.TimePeriodId];
        var measure = PlanningMeasures.GetDefinition(coordinate.MeasureId);
        var isLeafMonth = node.IsLeaf && string.Equals(timePeriod.Grain, "month", StringComparison.OrdinalIgnoreCase);
        var cellKind = isLeafMonth && measure.EditableAtLeaf ? "leaf" : "calculated";

        return new PlanningCell
        {
            Coordinate = coordinate,
            InputValue = null,
            OverrideValue = null,
            IsSystemGeneratedOverride = false,
            DerivedValue = 0m,
            EffectiveValue = 0m,
            GrowthFactor = 1.0m,
            IsLocked = false,
            LockReason = null,
            LockedBy = null,
            RowVersion = 1,
            CellKind = cellKind,
        };
    }

    private async Task<GridSliceResponse> ApplyDraftOverlayAsync(
        GridSliceResponse slice,
        string userId,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var rows = await ApplyDraftOverlayToRowsAsync(slice.ScenarioVersionId, slice.Rows, userId, metadata, cancellationToken);
        return slice with { Rows = rows };
    }

    private async Task<GridBranchResponse> ApplyDraftOverlayAsync(
        GridBranchResponse branch,
        string userId,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var rows = await ApplyDraftOverlayToRowsAsync(branch.ScenarioVersionId, branch.Rows, userId, metadata, cancellationToken);
        return branch with { Rows = rows };
    }

    private async Task<IReadOnlyList<GridRowDto>> ApplyDraftOverlayToRowsAsync(
        long scenarioVersionId,
        IReadOnlyList<GridRowDto> rows,
        string userId,
        PlanningMetadataSnapshot metadata,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return rows;
        }

        var coordinates = new List<PlanningCellCoordinate>();
        foreach (var row in rows)
        {
            var storeId = row.BindingStoreId ?? row.StoreId;
            var productNodeId = row.BindingProductNodeId ?? row.ProductNodeId;
            if (storeId <= 0 || productNodeId <= 0)
            {
                continue;
            }

            foreach (var (timePeriodId, periodCell) in row.Cells)
            {
                foreach (var measureId in periodCell.Measures.Keys)
                {
                    coordinates.Add(new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId));
                }
            }
        }

        if (coordinates.Count == 0)
        {
            return rows;
        }

        var draftCells = await _repository.GetDraftCellsAsync(scenarioVersionId, userId, coordinates, cancellationToken);
        if (draftCells.Count == 0)
        {
            return rows;
        }

        var draftByKey = draftCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell, StringComparer.Ordinal);
        return rows
            .Select(row => ApplyDraftOverlayToRow(row, scenarioVersionId, draftByKey, metadata))
            .ToList();
    }

    private static GridRowDto ApplyDraftOverlayToRow(
        GridRowDto row,
        long scenarioVersionId,
        IReadOnlyDictionary<string, PlanningCell> draftByKey,
        PlanningMetadataSnapshot metadata)
    {
        var storeId = row.BindingStoreId ?? row.StoreId;
        var productNodeId = row.BindingProductNodeId ?? row.ProductNodeId;
        if (storeId <= 0 || productNodeId <= 0)
        {
            return row;
        }

        Dictionary<long, GridPeriodCellDto>? updatedCells = null;

        foreach (var (timePeriodId, periodCell) in row.Cells)
        {
            Dictionary<long, GridCellDto>? updatedMeasures = null;
            var isLeafMonth = metadata.ProductNodes[productNodeId].IsLeaf
                && string.Equals(metadata.TimePeriods[timePeriodId].Grain, "month", StringComparison.OrdinalIgnoreCase);

            foreach (var (measureId, cell) in periodCell.Measures)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId);
                draftByKey.TryGetValue(coordinate.Key, out var draftCell);
                var lockState = ResolveDraftLockState(
                    coordinate,
                    cell.LockState,
                    draftByKey,
                    metadata);
                var isLocked = !string.Equals(lockState, "unlocked", StringComparison.OrdinalIgnoreCase);
                if (draftCell is null
                    && isLocked == cell.IsLocked
                    && string.Equals(lockState, cell.LockState, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                updatedMeasures ??= new Dictionary<long, GridCellDto>(periodCell.Measures);
                if (draftCell is not null && isLeafMonth)
                {
                    updatedMeasures[measureId] = new GridCellDto(
                        draftCell.BaseValue,
                        draftCell.EffectiveValue,
                        draftCell.GrowthFactor,
                        isLocked,
                        lockState,
                        string.Equals(draftCell.CellKind, "calculated", StringComparison.OrdinalIgnoreCase),
                        draftCell.OverrideValue is not null && !draftCell.IsSystemGeneratedOverride,
                        draftCell.RowVersion,
                        draftCell.CellKind);
                    continue;
                }

                updatedMeasures[measureId] = cell with
                {
                    IsLocked = isLocked,
                    LockState = lockState,
                    RowVersion = draftCell?.RowVersion ?? cell.RowVersion
                };
            }

            if (updatedMeasures is null)
            {
                continue;
            }

            updatedCells ??= new Dictionary<long, GridPeriodCellDto>(row.Cells);
            updatedCells[timePeriodId] = new GridPeriodCellDto(updatedMeasures);
        }

        var mergedRow = updatedCells is null ? row : row with { Cells = updatedCells };
        return RecalculateTimeAggregateDisplayCells(mergedRow, metadata);
    }

    private static string ResolveDraftLockState(
        PlanningCellCoordinate coordinate,
        string currentLockState,
        IReadOnlyDictionary<string, PlanningCell> draftByKey,
        PlanningMetadataSnapshot metadata)
    {
        if (draftByKey.TryGetValue(coordinate.Key, out var draftCell) && draftCell.IsLocked)
        {
            return "explicit";
        }

        var hasImplicitLock = draftByKey.Values.Any(cell =>
            cell.IsLocked
            && cell.Coordinate.MeasureId == coordinate.MeasureId
            && cell.Coordinate.StoreId == coordinate.StoreId
            && IsDescendantProduct(coordinate.ProductNodeId, cell.Coordinate.ProductNodeId, metadata)
            && IsDescendantTime(coordinate.TimePeriodId, cell.Coordinate.TimePeriodId, metadata));

        if (hasImplicitLock)
        {
            return "implicit";
        }

        return currentLockState;
    }

    private static GridRowDto RecalculateTimeAggregateDisplayCells(GridRowDto row, PlanningMetadataSnapshot metadata)
    {
        var updatedCells = row.Cells.ToDictionary(entry => entry.Key, entry => entry.Value);
        var aggregatePeriods = metadata.TimePeriods.Values
            .Where(period => updatedCells.ContainsKey(period.TimePeriodId))
            .Where(period => metadata.TimePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId && updatedCells.ContainsKey(child.TimePeriodId)))
            .OrderByDescending(period => GetTimeDepth(period.TimePeriodId, metadata))
            .ThenBy(period => period.SortOrder)
            .ToList();

        foreach (var aggregatePeriod in aggregatePeriods)
        {
            var childPeriods = metadata.TimePeriods.Values
                .Where(period => period.ParentTimePeriodId == aggregatePeriod.TimePeriodId && updatedCells.ContainsKey(period.TimePeriodId))
                .OrderBy(period => period.SortOrder)
                .ToList();
            if (childPeriods.Count == 0)
            {
                continue;
            }

            var currentMeasures = updatedCells[aggregatePeriod.TimePeriodId].Measures.ToDictionary(entry => entry.Key, entry => entry.Value);
            foreach (var measure in PlanningMeasures.Definitions)
            {
                if (!currentMeasures.TryGetValue(measure.MeasureId, out var currentCell))
                {
                    continue;
                }

                decimal SumChild(long additiveMeasureId, bool useBaseValue) => childPeriods.Sum(period =>
                {
                    var childCell = updatedCells[period.TimePeriodId].Measures[additiveMeasureId];
                    return useBaseValue ? childCell.BaseValue : childCell.Value;
                });

                var revenueBase = SumChild(PlanningMeasures.SalesRevenue, useBaseValue: true);
                var quantityBase = SumChild(PlanningMeasures.SoldQuantity, useBaseValue: true);
                var totalCostsBase = SumChild(PlanningMeasures.TotalCosts, useBaseValue: true);
                var grossProfitBase = SumChild(PlanningMeasures.GrossProfit, useBaseValue: true);
                var revenueValue = SumChild(PlanningMeasures.SalesRevenue, useBaseValue: false);
                var quantityValue = SumChild(PlanningMeasures.SoldQuantity, useBaseValue: false);
                var totalCostsValue = SumChild(PlanningMeasures.TotalCosts, useBaseValue: false);
                var grossProfitValue = SumChild(PlanningMeasures.GrossProfit, useBaseValue: false);
                var childMeasureCells = childPeriods.Select(period => updatedCells[period.TimePeriodId].Measures[measure.MeasureId]).ToList();
                var derivedBaseValue = measure.MeasureId switch
                {
                    PlanningMeasures.SalesRevenue => PlanningMath.NormalizeRevenue(revenueBase),
                    PlanningMeasures.SoldQuantity => PlanningMath.NormalizeQuantity(quantityBase),
                    PlanningMeasures.AverageSellingPrice => quantityBase > 0m ? PlanningMath.NormalizeAsp(revenueBase / quantityBase) : 1.00m,
                    PlanningMeasures.UnitCost => quantityBase > 0m ? PlanningMath.NormalizeUnitCost(totalCostsBase / quantityBase) : 0m,
                    PlanningMeasures.TotalCosts => PlanningMath.NormalizeTotalCosts(totalCostsBase),
                    PlanningMeasures.GrossProfit => PlanningMath.NormalizeGrossProfit(grossProfitBase),
                    PlanningMeasures.GrossProfitPercent => revenueBase > 0m
                        ? PlanningMath.NormalizeGrossProfitPercent(((revenueBase - totalCostsBase) / revenueBase) * 100m)
                        : 0m,
                    _ => currentCell.BaseValue
                };
                var derivedEffectiveValue = measure.MeasureId switch
                {
                    PlanningMeasures.SalesRevenue => PlanningMath.NormalizeRevenue(revenueValue),
                    PlanningMeasures.SoldQuantity => PlanningMath.NormalizeQuantity(quantityValue),
                    PlanningMeasures.AverageSellingPrice => quantityValue > 0m ? PlanningMath.NormalizeAsp(revenueValue / quantityValue) : 1.00m,
                    PlanningMeasures.UnitCost => quantityValue > 0m ? PlanningMath.NormalizeUnitCost(totalCostsValue / quantityValue) : 0m,
                    PlanningMeasures.TotalCosts => PlanningMath.NormalizeTotalCosts(totalCostsValue),
                    PlanningMeasures.GrossProfit => PlanningMath.NormalizeGrossProfit(grossProfitValue),
                    PlanningMeasures.GrossProfitPercent => revenueValue > 0m
                        ? PlanningMath.NormalizeGrossProfitPercent(((revenueValue - totalCostsValue) / revenueValue) * 100m)
                        : 0m,
                    _ => currentCell.Value
                };
                var aggregateGrowthFactor = DeriveAggregateGrowthFactor(derivedBaseValue, derivedEffectiveValue, childMeasureCells);
                if (measure.MeasureId is PlanningMeasures.AverageSellingPrice or PlanningMeasures.UnitCost or PlanningMeasures.GrossProfitPercent)
                {
                    derivedBaseValue = aggregateGrowthFactor != 0m
                        ? PlanningMath.NormalizeMeasureValue(measure.MeasureId, derivedEffectiveValue / aggregateGrowthFactor)
                        : derivedBaseValue;
                }

                currentMeasures[measure.MeasureId] = currentCell with
                {
                    BaseValue = derivedBaseValue,
                    Value = derivedEffectiveValue,
                    GrowthFactor = aggregateGrowthFactor,
                    IsCalculated = true
                };
            }

            updatedCells[aggregatePeriod.TimePeriodId] = new GridPeriodCellDto(currentMeasures);
        }

        return row with { Cells = updatedCells };
    }

    private static decimal DeriveAggregateGrowthFactor(decimal baseValue, decimal effectiveValue, IReadOnlyList<GridCellDto> childCells)
    {
        if (childCells.Count == 0)
        {
            return 1.0m;
        }

        var firstGrowthFactor = childCells[0].GrowthFactor;
        if (childCells.All(cell => cell.GrowthFactor == firstGrowthFactor))
        {
            return firstGrowthFactor;
        }

        if (baseValue <= 0m || effectiveValue <= 0m)
        {
            return 1.0m;
        }

        return PlanningMath.NormalizeGrowthFactor(effectiveValue / baseValue);
    }
}
