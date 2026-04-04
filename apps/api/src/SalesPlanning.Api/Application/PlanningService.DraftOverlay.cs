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
        var rows = await ApplyDraftOverlayToRowsAsync(slice.ScenarioVersionId, slice.Rows, userId, cancellationToken);
        return slice with { Rows = rows };
    }

    private async Task<GridBranchResponse> ApplyDraftOverlayAsync(
        GridBranchResponse branch,
        string userId,
        CancellationToken cancellationToken)
    {
        var rows = await ApplyDraftOverlayToRowsAsync(branch.ScenarioVersionId, branch.Rows, userId, cancellationToken);
        return branch with { Rows = rows };
    }

    private async Task<IReadOnlyList<GridRowDto>> ApplyDraftOverlayToRowsAsync(
        long scenarioVersionId,
        IReadOnlyList<GridRowDto> rows,
        string userId,
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
            .Select(row => ApplyDraftOverlayToRow(row, scenarioVersionId, draftByKey))
            .ToList();
    }

    private static GridRowDto ApplyDraftOverlayToRow(
        GridRowDto row,
        long scenarioVersionId,
        IReadOnlyDictionary<string, PlanningCell> draftByKey)
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

            foreach (var (measureId, cell) in periodCell.Measures)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId);
                if (!draftByKey.TryGetValue(coordinate.Key, out var draftCell))
                {
                    continue;
                }

                updatedMeasures ??= new Dictionary<long, GridCellDto>(periodCell.Measures);
                updatedMeasures[measureId] = new GridCellDto(
                    draftCell.EffectiveValue,
                    draftCell.GrowthFactor,
                    draftCell.IsLocked,
                    string.Equals(draftCell.CellKind, "calculated", StringComparison.OrdinalIgnoreCase),
                    draftCell.OverrideValue is not null,
                    draftCell.RowVersion,
                    draftCell.CellKind);
            }

            if (updatedMeasures is null)
            {
                continue;
            }

            updatedCells ??= new Dictionary<long, GridPeriodCellDto>(row.Cells);
            updatedCells[timePeriodId] = new GridPeriodCellDto(updatedMeasures);
        }

        return updatedCells is null ? row : row with { Cells = updatedCells };
    }
}
