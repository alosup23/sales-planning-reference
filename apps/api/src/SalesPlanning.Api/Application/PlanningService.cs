using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed class PlanningService : IPlanningService
{
    private readonly IPlanningRepository _repository;
    private readonly ISplashAllocator _splashAllocator;
    private long _actionSeed = 1000;

    public PlanningService(IPlanningRepository repository, ISplashAllocator splashAllocator)
    {
        _repository = repository;
        _splashAllocator = splashAllocator;
    }

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken)
    {
        return _repository.GetGridSliceAsync(scenarioVersionId, measureId, cancellationToken);
    }

    public async Task<EditCellsResponse> ApplyEditsAsync(EditCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var deltas = new List<PlanningCellDeltaAudit>();
        var cellsToPersist = new List<PlanningCell>();

        foreach (var edit in request.Cells)
        {
            var coordinate = new PlanningCellCoordinate(
                request.ScenarioVersionId,
                request.MeasureId,
                edit.StoreId,
                edit.ProductNodeId,
                edit.TimePeriodId);

            var existing = await _repository.GetCellAsync(coordinate, cancellationToken)
                ?? throw new InvalidOperationException("The target cell does not exist.");

            if (existing.IsLocked)
            {
                throw new InvalidOperationException($"Cell {coordinate.Key} is locked and cannot be edited.");
            }

            var isAggregateCell = IsAggregateCell(existing.Coordinate, metadata);
            ValidateEditMode(edit.EditMode, isAggregateCell, coordinate);

            if (edit.RowVersion is not null && existing.RowVersion != edit.RowVersion.Value)
            {
                throw new InvalidOperationException($"Cell {coordinate.Key} has changed since it was last read.");
            }

            var oldValue = existing.EffectiveValue;
            if (string.Equals(edit.EditMode, "override", StringComparison.OrdinalIgnoreCase))
            {
                existing.InputValue = null;
                existing.OverrideValue = edit.NewValue;
                existing.IsSystemGeneratedOverride = false;
                existing.EffectiveValue = edit.NewValue;
                existing.CellKind = "override";
            }
            else
            {
                existing.InputValue = edit.NewValue;
                existing.OverrideValue = null;
                existing.IsSystemGeneratedOverride = false;
                existing.DerivedValue = edit.NewValue;
                existing.EffectiveValue = edit.NewValue;
                existing.CellKind = "input";
            }

            existing.RowVersion += 1;
            cellsToPersist.Add(existing);
            deltas.Add(new PlanningCellDeltaAudit(coordinate, oldValue, existing.EffectiveValue, false, "manual-edit"));

            await _repository.UpsertCellsAsync(new[] { existing }, cancellationToken);
            await RecalculateAncestorsAsync(existing.Coordinate, request.ScenarioVersionId, request.MeasureId, request.Comment, deltas, cellsToPersist, cancellationToken);
        }

        await _repository.UpsertCellsAsync(cellsToPersist.GroupBy(c => c.Coordinate.Key).Select(g => g.Last()), cancellationToken);
        var actionId = await AppendAuditAsync("manual_edit", "manual", userId, request.Comment, deltas, cancellationToken);

        return new EditCellsResponse(actionId, deltas.Count, "applied");
    }

    public async Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken)
    {
        var sourceCoordinate = new PlanningCellCoordinate(
            request.ScenarioVersionId,
            request.MeasureId,
            request.SourceCell.StoreId,
            request.SourceCell.ProductNodeId,
            request.SourceCell.TimePeriodId);

        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var sourceCell = await _repository.GetCellAsync(sourceCoordinate, cancellationToken)
            ?? throw new KeyNotFoundException($"Source cell {sourceCoordinate.Key} was not found.");

        if (sourceCell.IsLocked)
        {
            throw new InvalidOperationException("The source cell is locked and cannot be used for splash.");
        }

        var sourcePeriod = metadata.TimePeriods[sourceCoordinate.TimePeriodId];
        var targetPeriods = metadata.TimePeriods.Values
            .Where(x => x.ParentTimePeriodId == sourcePeriod.TimePeriodId)
            .OrderBy(x => x.SortOrder)
            .Select(x => x.TimePeriodId)
            .ToList();

        if (targetPeriods.Count == 0)
        {
            throw new InvalidOperationException("The selected source cell has no child periods to splash into.");
        }

        var targetCells = await _repository.GetCellsForProductAndPeriodsAsync(
            request.ScenarioVersionId,
            request.MeasureId,
            request.SourceCell.StoreId,
            request.SourceCell.ProductNodeId,
            targetPeriods,
            cancellationToken);

        if (targetCells.Count != targetPeriods.Count)
        {
            throw new InvalidOperationException("The splash target range is incomplete.");
        }

        var weights = ResolveWeights(request, targetCells);
        var allocations = _splashAllocator.Allocate(
            request.TotalValue,
            targetCells.Select(cell => new SplashTarget(cell, weights[cell.Coordinate.TimePeriodId])).ToList(),
            request.RoundingScale);

        var cellsToPersist = new List<PlanningCell>();
        var deltas = new List<PlanningCellDeltaAudit>();

        foreach (var allocation in allocations)
        {
            var cell = allocation.Cell;
            var oldValue = cell.EffectiveValue;
            cell.InputValue = allocation.NewValue;
            cell.DerivedValue = allocation.NewValue;
            cell.EffectiveValue = allocation.NewValue;
            cell.CellKind = "input";
            cell.RowVersion += 1;
            cellsToPersist.Add(cell);
            deltas.Add(new PlanningCellDeltaAudit(cell.Coordinate, oldValue, cell.EffectiveValue, false, "splash"));
        }

        await _repository.UpsertCellsAsync(cellsToPersist, cancellationToken);
        await RecalculateAncestorsAsync(sourceCoordinate, request.ScenarioVersionId, request.MeasureId, request.Comment, deltas, cellsToPersist, cancellationToken);
        await _repository.UpsertCellsAsync(cellsToPersist.GroupBy(c => c.Coordinate.Key).Select(g => g.Last()), cancellationToken);
        var actionId = await AppendAuditAsync("splash", request.Method, userId, request.Comment, deltas, cancellationToken);

        var lockedCellsSkipped = targetCells.Count(cell => cell.IsLocked);
        return new SplashResponse(actionId, "applied", allocations.Count, lockedCellsSkipped);
    }

    public async Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var cells = await _repository.GetCellsAsync(
            request.Coordinates.Select(c => new PlanningCellCoordinate(
                request.ScenarioVersionId,
                request.MeasureId,
                c.StoreId,
                c.ProductNodeId,
                c.TimePeriodId)),
            cancellationToken);
        var cellsToPersist = new List<PlanningCell>();
        var deltas = new List<PlanningCellDeltaAudit>();

        foreach (var cell in cells)
        {
            var oldValue = cell.EffectiveValue;
            cell.IsLocked = request.Locked;
            cell.LockReason = request.Reason;
            cell.LockedBy = request.Locked ? userId : null;

            var isAggregateCell = IsAggregateCell(cell.Coordinate, metadata);
            if (request.Locked && isAggregateCell && cell.OverrideValue is null)
            {
                cell.OverrideValue = cell.EffectiveValue;
                cell.IsSystemGeneratedOverride = true;
                cell.CellKind = "override";
            }
            else if (!request.Locked && cell.IsSystemGeneratedOverride)
            {
                cell.OverrideValue = null;
                cell.IsSystemGeneratedOverride = false;
                cell.CellKind = isAggregateCell ? "calculated" : "leaf";
            }

            cell.RowVersion += 1;
            cellsToPersist.Add(cell);

            if (!request.Locked && isAggregateCell)
            {
                await _repository.UpsertCellsAsync(new[] { cell }, cancellationToken);
                await RecalculateSingleCellAsync(
                    cell.Coordinate.ScenarioVersionId,
                    cell.Coordinate.MeasureId,
                    cell.Coordinate.StoreId,
                    cell.Coordinate.ProductNodeId,
                    cell.Coordinate.TimePeriodId,
                    deltas,
                    cellsToPersist,
                    cancellationToken);
                await RecalculateAncestorsAsync(
                    cell.Coordinate,
                    cell.Coordinate.ScenarioVersionId,
                    cell.Coordinate.MeasureId,
                    request.Reason,
                    deltas,
                    cellsToPersist,
                    cancellationToken);
            }

            if (oldValue != cell.EffectiveValue)
            {
                deltas.Add(new PlanningCellDeltaAudit(cell.Coordinate, oldValue, cell.EffectiveValue, !request.Locked, request.Locked ? "lock" : "unlock"));
            }
        }

        await _repository.UpsertCellsAsync(cellsToPersist.GroupBy(c => c.Coordinate.Key).Select(g => g.Last()), cancellationToken);
        await AppendAuditAsync(request.Locked ? "lock" : "unlock", "lock", userId, request.Reason, deltas, cancellationToken);

        return new LockCellsResponse(cells.Count, request.Locked);
    }

    public async Task<IReadOnlyList<AuditTrailItemDto>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        var audits = await _repository.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
        return audits.Select(audit => new AuditTrailItemDto(
            audit.ActionId,
            audit.ActionType,
            audit.Method,
            audit.UserId,
            audit.Comment,
            audit.CreatedAt,
            audit.Deltas.Select(delta => new AuditCellDeltaDto(
                delta.Coordinate.StoreId,
                delta.Coordinate.ProductNodeId,
                delta.Coordinate.TimePeriodId,
                delta.OldValue,
                delta.NewValue,
                delta.ChangeKind)).ToList()))
            .ToList();
    }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        return _repository.ResetAsync(cancellationToken);
    }

    private async Task RecalculateAncestorsAsync(
        PlanningCellCoordinate sourceCoordinate,
        long scenarioVersionId,
        long measureId,
        string? comment,
        List<PlanningCellDeltaAudit> deltas,
        List<PlanningCell> cellsToPersist,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var impactedProductNodeIds = GetAncestorProductNodeIds(sourceCoordinate.ProductNodeId, metadata);
        var impactedTimePeriodIds = GetAncestorTimePeriodIds(sourceCoordinate.TimePeriodId, metadata);

        foreach (var ancestorTimePeriodId in impactedTimePeriodIds)
        {
            await RecalculateSingleCellAsync(
                scenarioVersionId,
                measureId,
                sourceCoordinate.StoreId,
                sourceCoordinate.ProductNodeId,
                ancestorTimePeriodId,
                deltas,
                cellsToPersist,
                cancellationToken);
        }

        foreach (var productNodeId in impactedProductNodeIds)
        {
            await RecalculateSingleCellAsync(
                scenarioVersionId,
                measureId,
                sourceCoordinate.StoreId,
                productNodeId,
                sourceCoordinate.TimePeriodId,
                deltas,
                cellsToPersist,
                cancellationToken);

            foreach (var ancestorTimePeriodId in impactedTimePeriodIds)
            {
                await RecalculateSingleCellAsync(
                    scenarioVersionId,
                    measureId,
                    sourceCoordinate.StoreId,
                    productNodeId,
                    ancestorTimePeriodId,
                    deltas,
                    cellsToPersist,
                    cancellationToken);
                }
        }
    }

    private async Task RecalculateSingleCellAsync(
        long scenarioVersionId,
        long measureId,
        long storeId,
        long productNodeId,
        long timePeriodId,
        List<PlanningCellDeltaAudit> deltas,
        List<PlanningCell> cellsToPersist,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var productNode = metadata.ProductNodes[productNodeId];
        var timePeriod = metadata.TimePeriods[timePeriodId];

        var childProductIds = metadata.ProductNodes.Values
            .Where(node => node.ParentProductNodeId == productNodeId)
            .Select(node => node.ProductNodeId)
            .ToList();
        var childTimeIds = metadata.TimePeriods.Values
            .Where(node => node.ParentTimePeriodId == timePeriodId)
            .OrderBy(node => node.SortOrder)
            .Select(node => node.TimePeriodId)
            .ToList();

        IReadOnlyList<PlanningCell> children;
        if (childProductIds.Count > 0)
        {
            children = (await Task.WhenAll(childProductIds.Select(id =>
                GetPendingOrPersistedCellAsync(
                    new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, id, timePeriodId),
                    cellsToPersist,
                    cancellationToken))))
                .Where(cell => cell is not null)
                .Cast<PlanningCell>()
                .ToList();
        }
        else
        {
            var persistedChildren = await _repository.GetCellsForProductAndPeriodsAsync(
                scenarioVersionId,
                measureId,
                storeId,
                productNodeId,
                childTimeIds,
                cancellationToken);

            children = childTimeIds
                .Select(timeId => GetPendingOrPersistedCell(
                        new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timeId),
                        cellsToPersist)
                    ?? persistedChildren.FirstOrDefault(cell => cell.Coordinate.TimePeriodId == timeId))
                .Where(cell => cell is not null)
                .Cast<PlanningCell>()
                .ToList();
        }

        if (children.Count == 0)
        {
            return;
        }

        var aggregate = await _repository.GetCellAsync(
            new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNodeId, timePeriodId),
            cancellationToken) ?? throw new InvalidOperationException("Aggregate cell not found.");

        var oldValue = aggregate.EffectiveValue;
        aggregate.DerivedValue = children.Sum(child => child.EffectiveValue);
        if (aggregate.OverrideValue is null)
        {
            aggregate.EffectiveValue = aggregate.DerivedValue;
            aggregate.CellKind = childProductIds.Count > 0 || childTimeIds.Count > 0 ? "calculated" : aggregate.CellKind;
        }

        aggregate.RowVersion += 1;
        cellsToPersist.Add(aggregate);

        if (aggregate.EffectiveValue != oldValue)
        {
            deltas.Add(new PlanningCellDeltaAudit(aggregate.Coordinate, oldValue, aggregate.EffectiveValue, aggregate.IsLocked, "rollup"));
        }
    }

    private static PlanningCell? GetPendingOrPersistedCell(PlanningCellCoordinate coordinate, IReadOnlyList<PlanningCell> cellsToPersist)
    {
        return cellsToPersist.LastOrDefault(cell => cell.Coordinate.Key == coordinate.Key);
    }

    private async Task<PlanningCell?> GetPendingOrPersistedCellAsync(
        PlanningCellCoordinate coordinate,
        IReadOnlyList<PlanningCell> cellsToPersist,
        CancellationToken cancellationToken)
    {
        return GetPendingOrPersistedCell(coordinate, cellsToPersist)
            ?? await _repository.GetCellAsync(coordinate, cancellationToken);
    }

    private static Dictionary<long, decimal> ResolveWeights(SplashRequest request, IReadOnlyList<PlanningCell> targetCells)
    {
        var weights = request.Method switch
        {
            "equal_distribution" => targetCells.ToDictionary(cell => cell.Coordinate.TimePeriodId, _ => 1m),
            "proportional_to_existing_plan" => targetCells.ToDictionary(
                cell => cell.Coordinate.TimePeriodId,
                cell => Math.Max(cell.EffectiveValue, 0m)),
            "seasonality_profile" => request.ManualWeights?.Count > 0
                ? request.ManualWeights
                : throw new InvalidOperationException("Seasonality weights are required."),
            "manual_weights" => request.ManualWeights?.Count > 0
                ? request.ManualWeights
                : throw new InvalidOperationException("Manual weights are required."),
            _ => throw new InvalidOperationException($"Unsupported splash method '{request.Method}'.")
        };

        foreach (var targetCell in targetCells)
        {
            if (!weights.TryGetValue(targetCell.Coordinate.TimePeriodId, out var weight))
            {
                throw new InvalidOperationException($"A weight was not provided for time period {targetCell.Coordinate.TimePeriodId}.");
            }

            if (weight < 0)
            {
                throw new InvalidOperationException("Negative splash weights are not supported.");
            }
        }

        return weights;
    }

    private static IReadOnlyList<long> GetAncestorProductNodeIds(long productNodeId, PlanningMetadataSnapshot metadata)
    {
        var ancestors = new List<long>();
        var currentProductNodeId = metadata.ProductNodes[productNodeId].ParentProductNodeId;

        while (currentProductNodeId is not null)
        {
            ancestors.Add(currentProductNodeId.Value);
            currentProductNodeId = metadata.ProductNodes[currentProductNodeId.Value].ParentProductNodeId;
        }

        return ancestors;
    }

    private static IReadOnlyList<long> GetAncestorTimePeriodIds(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        var ancestors = new List<long>();
        var currentTimePeriodId = metadata.TimePeriods[timePeriodId].ParentTimePeriodId;

        while (currentTimePeriodId is not null)
        {
            ancestors.Add(currentTimePeriodId.Value);
            currentTimePeriodId = metadata.TimePeriods[currentTimePeriodId.Value].ParentTimePeriodId;
        }

        return ancestors;
    }

    private static bool IsAggregateCell(PlanningCellCoordinate coordinate, PlanningMetadataSnapshot metadata)
    {
        return metadata.ProductNodes.Values.Any(node => node.ParentProductNodeId == coordinate.ProductNodeId)
            || metadata.TimePeriods.Values.Any(period => period.ParentTimePeriodId == coordinate.TimePeriodId);
    }

    private static void ValidateEditMode(string editMode, bool isAggregateCell, PlanningCellCoordinate coordinate)
    {
        var normalizedMode = editMode.Trim().ToLowerInvariant();
        if (isAggregateCell && normalizedMode != "override")
        {
            throw new InvalidOperationException($"Aggregate cell {coordinate.Key} must be edited in override mode.");
        }

        if (!isAggregateCell && normalizedMode != "input")
        {
            throw new InvalidOperationException($"Leaf cell {coordinate.Key} must be edited in input mode.");
        }
    }

    private async Task<long> AppendAuditAsync(
        string actionType,
        string method,
        string userId,
        string? comment,
        IReadOnlyList<PlanningCellDeltaAudit> deltas,
        CancellationToken cancellationToken)
    {
        var audit = new PlanningActionAudit(
            Interlocked.Increment(ref _actionSeed),
            actionType,
            method,
            userId,
            comment,
            DateTimeOffset.UtcNow,
            deltas);

        await _repository.AppendAuditAsync(audit, cancellationToken);
        return audit.ActionId;
    }
}
