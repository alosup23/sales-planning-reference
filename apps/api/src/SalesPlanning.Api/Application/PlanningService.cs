using ClosedXML.Excel;
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
        var deltas = new List<PlanningCellDeltaAudit>();

        foreach (var edit in request.Cells)
        {
            var coordinate = new PlanningCellCoordinate(
                request.ScenarioVersionId,
                request.MeasureId,
                edit.StoreId,
                edit.ProductNodeId,
                edit.TimePeriodId);

            deltas.AddRange(await ApplyValueChangeAsync(
                coordinate,
                edit.NewValue,
                edit.EditMode,
                edit.RowVersion,
                request.Comment,
                "manual-edit",
                "manual",
                null,
                cancellationToken));
        }

        var actionId = await AppendAuditAsync("manual_edit", "manual", userId, request.Comment, deltas, cancellationToken);
        return new EditCellsResponse(actionId, deltas.Count, "applied");
    }

    public async Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken)
    {
        var coordinate = new PlanningCellCoordinate(
            request.ScenarioVersionId,
            request.MeasureId,
            request.SourceCell.StoreId,
            request.SourceCell.ProductNodeId,
            request.SourceCell.TimePeriodId);

        var deltas = await ApplyValueChangeAsync(
            coordinate,
            request.TotalValue,
            "override",
            null,
            request.Comment,
            "splash",
            request.Method,
            request.ManualWeights,
            cancellationToken);

        var actionId = await AppendAuditAsync("splash", request.Method, userId, request.Comment, deltas, cancellationToken);
        var lockedCellsSkipped = deltas.Count(delta => delta.WasLocked);
        return new SplashResponse(actionId, "applied", deltas.Count, lockedCellsSkipped);
    }

    public async Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        var scenarioCells = (await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, request.MeasureId, cancellationToken))
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell);

        foreach (var coordinate in request.Coordinates)
        {
            var key = new PlanningCellCoordinate(
                request.ScenarioVersionId,
                request.MeasureId,
                coordinate.StoreId,
                coordinate.ProductNodeId,
                coordinate.TimePeriodId).Key;

            if (!scenarioCells.TryGetValue(key, out var cell))
            {
                throw new InvalidOperationException($"Cell {key} was not found.");
            }

            cell.IsLocked = request.Locked;
            cell.LockReason = request.Reason;
            cell.LockedBy = request.Locked ? userId : null;
            cell.RowVersion += 1;
        }

        await _repository.UpsertCellsAsync(scenarioCells.Values, cancellationToken);
        await AppendAuditAsync(request.Locked ? "lock" : "unlock", "lock", userId, request.Reason, Array.Empty<PlanningCellDeltaAudit>(), cancellationToken);
        return new LockCellsResponse(request.Coordinates.Count, request.Locked);
    }

    public Task<IReadOnlyList<AuditTrailItemDto>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        return GetAuditInternalAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
    }

    public async Task<AddRowResponse> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        var node = await _repository.AddRowAsync(request, cancellationToken);
        return new AddRowResponse(node.StoreId, node.ProductNodeId, node.Label, node.Level, node.Path, node.IsLeaf);
    }

    public async Task<HierarchyMappingResponse> GetHierarchyMappingsAsync(CancellationToken cancellationToken)
    {
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<HierarchyMappingResponse> AddHierarchyCategoryAsync(AddHierarchyCategoryRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertHierarchyCategoryAsync(request.CategoryLabel, cancellationToken);
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<HierarchyMappingResponse> AddHierarchySubcategoryAsync(AddHierarchySubcategoryRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertHierarchySubcategoryAsync(request.CategoryLabel, request.SubcategoryLabel, cancellationToken);
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<ImportWorkbookResponse> ImportWorkbookAsync(long scenarioVersionId, long measureId, Stream workbookStream, string fileName, string userId, CancellationToken cancellationToken)
    {
        if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Only .xlsx workbook uploads are supported in this reference implementation.");
        }

        using var workbook = new XLWorkbook(workbookStream);
        var worksheet = workbook.Worksheets.FirstOrDefault()
            ?? throw new InvalidOperationException("The uploaded workbook does not contain any worksheets.");

        var headerMap = worksheet.Row(1)
            .CellsUsed()
            .ToDictionary(
                cell => cell.GetString().Trim(),
                cell => cell.Address.ColumnNumber,
                StringComparer.OrdinalIgnoreCase);

        foreach (var requiredHeader in new[] { "Store", "Category", "Subcategory" })
        {
            if (!headerMap.ContainsKey(requiredHeader))
            {
                throw new InvalidOperationException($"The workbook is missing the required '{requiredHeader}' column.");
            }
        }

        var monthColumns = new Dictionary<long, int>();
        foreach (var (timePeriodId, label) in new Dictionary<long, string>
                 {
                     [202601] = "Jan",
                     [202602] = "Feb",
                     [202603] = "Mar",
                     [202604] = "Apr",
                     [202605] = "May",
                     [202606] = "Jun",
                     [202607] = "Jul",
                     [202608] = "Aug",
                     [202609] = "Sep",
                     [202610] = "Oct",
                     [202611] = "Nov",
                     [202612] = "Dec"
                 })
        {
            if (headerMap.TryGetValue(label, out var columnNumber))
            {
                monthColumns[timePeriodId] = columnNumber;
            }
        }

        if (monthColumns.Count == 0 && !headerMap.ContainsKey("FY26"))
        {
            throw new InvalidOperationException("The workbook must contain at least one month column or an FY26 total column.");
        }

        var rowsProcessed = 0;
        var rowsCreated = 0;
        var cellsUpdated = 0;
        var importDeltas = new List<PlanningCellDeltaAudit>();

        foreach (var row in worksheet.RowsUsed().Skip(1))
        {
            var storeLabel = row.Cell(headerMap["Store"]).GetString().Trim();
            var categoryLabel = row.Cell(headerMap["Category"]).GetString().Trim();
            var subcategoryLabel = row.Cell(headerMap["Subcategory"]).GetString().Trim();

            if (string.IsNullOrWhiteSpace(storeLabel) || string.IsNullOrWhiteSpace(categoryLabel) || string.IsNullOrWhiteSpace(subcategoryLabel))
            {
                continue;
            }

            rowsProcessed += 1;

            var storeNode = await EnsureNodeAsync(scenarioVersionId, measureId, "store", null, storeLabel, cancellationToken);
            var categoryNode = await EnsureNodeAsync(scenarioVersionId, measureId, "category", storeNode.Node.ProductNodeId, categoryLabel, cancellationToken);
            var subcategoryNode = await EnsureNodeAsync(scenarioVersionId, measureId, "subcategory", categoryNode.Node.ProductNodeId, subcategoryLabel, cancellationToken);
            rowsCreated += storeNode.CreatedCount + categoryNode.CreatedCount + subcategoryNode.CreatedCount;

            var importedAnyMonth = false;
            foreach (var monthColumn in monthColumns)
            {
                var cellValue = row.Cell(monthColumn.Value);
                if (cellValue.IsEmpty())
                {
                    continue;
                }

                importedAnyMonth = true;
                importDeltas.AddRange(await ApplyValueChangeAsync(
                    new PlanningCellCoordinate(scenarioVersionId, measureId, subcategoryNode.Node.StoreId, subcategoryNode.Node.ProductNodeId, monthColumn.Key),
                    GetNumericCellValue(cellValue),
                    "input",
                    null,
                    $"Import from {fileName}",
                    "import",
                    "import-month",
                    null,
                    cancellationToken));
                cellsUpdated += 1;
            }

            if (!importedAnyMonth && headerMap.TryGetValue("FY26", out var yearColumn) && !row.Cell(yearColumn).IsEmpty())
            {
                importDeltas.AddRange(await ApplyValueChangeAsync(
                    new PlanningCellCoordinate(scenarioVersionId, measureId, subcategoryNode.Node.StoreId, subcategoryNode.Node.ProductNodeId, 202600),
                    GetNumericCellValue(row.Cell(yearColumn)),
                    "override",
                    null,
                    $"Import from {fileName}",
                    "import",
                    "import-year",
                    null,
                    cancellationToken));
                cellsUpdated += 1;
            }
        }

        await AppendAuditAsync("import", "workbook", userId, $"Imported workbook {fileName}", importDeltas, cancellationToken);
        return new ImportWorkbookResponse(rowsProcessed, cellsUpdated, rowsCreated, "applied");
    }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        return _repository.ResetAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<AuditTrailItemDto>> GetAuditInternalAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
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

    private async Task<IReadOnlyList<PlanningCellDeltaAudit>> ApplyValueChangeAsync(
        PlanningCellCoordinate coordinate,
        decimal newValue,
        string editMode,
        long? rowVersion,
        string? comment,
        string changeKind,
        string method,
        Dictionary<long, decimal>? manualWeights,
        CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var originalCells = await _repository.GetScenarioCellsAsync(coordinate.ScenarioVersionId, coordinate.MeasureId, cancellationToken);
        var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

        if (!workingCells.TryGetValue(coordinate.Key, out var sourceCell))
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} was not found.");
        }

        if (rowVersion is not null && sourceCell.RowVersion != rowVersion.Value)
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} has changed since it was last read.");
        }

        if (IsLockedBySelfOrAncestor(coordinate, workingCells.Values, metadata))
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} is locked and cannot be changed.");
        }

        var normalizedEditMode = editMode.Trim().ToLowerInvariant();
        var isLeafCoordinate = IsLeafWriteCoordinate(coordinate, metadata);
        if (normalizedEditMode == "input")
        {
            if (!isLeafCoordinate)
            {
                throw new InvalidOperationException($"Cell {coordinate.Key} is an aggregate intersection and requires top-down override or splash.");
            }

            sourceCell.InputValue = newValue;
            sourceCell.DerivedValue = newValue;
            sourceCell.EffectiveValue = newValue;
            sourceCell.CellKind = "input";
        }
        else
        {
            if (isLeafCoordinate)
            {
                ApplyAggregateAllocation(coordinate, newValue, method, manualWeights, workingCells, metadata);
            }
            else
            {
                ApplyAggregateAllocation(coordinate, newValue, method, manualWeights, workingCells, metadata);
            }
        }

        RecalculateAll(workingCells, metadata, coordinate.ScenarioVersionId, coordinate.MeasureId);
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, changeKind, cancellationToken);
        return deltas;
    }

    private void ApplyAggregateAllocation(
        PlanningCellCoordinate source,
        decimal totalValue,
        string method,
        Dictionary<long, decimal>? manualWeights,
        Dictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata)
    {
        var targetProductIds = GetLeafProductIds(source.ProductNodeId, metadata);
        var targetTimeIds = GetLeafTimeIds(source.TimePeriodId, metadata);
        var targetCells = targetProductIds
            .SelectMany(productId => targetTimeIds.Select(timeId => workingCells[new PlanningCellCoordinate(
                source.ScenarioVersionId,
                source.MeasureId,
                source.StoreId,
                productId,
                timeId).Key]))
            .ToList();

        var weights = BuildWeights(source, targetCells, method, manualWeights, metadata);
        var splashTargets = targetCells
            .Select(cell =>
            {
                var clone = cell.Clone();
                clone.IsLocked = IsLockedBySelfOrAncestor(clone.Coordinate, workingCells.Values, metadata);
                return new SplashTarget(clone, weights[clone.Coordinate.Key]);
            })
            .ToList();

        var allocations = _splashAllocator.Allocate(totalValue, splashTargets, 0);
        foreach (var allocation in allocations)
        {
            var target = workingCells[allocation.Cell.Coordinate.Key];
            target.InputValue = allocation.NewValue;
            target.DerivedValue = allocation.NewValue;
            target.EffectiveValue = allocation.NewValue;
            target.CellKind = "input";
        }
    }

    private Dictionary<string, decimal> BuildWeights(
        PlanningCellCoordinate source,
        IReadOnlyList<PlanningCell> targetCells,
        string method,
        Dictionary<long, decimal>? manualWeights,
        PlanningMetadataSnapshot metadata)
    {
        if (targetCells.Count == 0)
        {
            throw new InvalidOperationException("No leaf targets were found for this splash.");
        }

        var normalizedMethod = method.Trim().ToLowerInvariant();
        if (normalizedMethod == "equal_distribution")
        {
            return targetCells.ToDictionary(cell => cell.Coordinate.Key, _ => 1m);
        }

        if (normalizedMethod is "seasonality_profile" or "manual_weights")
        {
            if (manualWeights is null || manualWeights.Count == 0)
            {
                throw new InvalidOperationException("Manual or seasonality weights are required for this splash.");
            }

            var timeWeights = GetLeafTimeIds(source.TimePeriodId, metadata)
                .ToDictionary(timeId => timeId, timeId =>
                {
                    if (!manualWeights.TryGetValue(timeId, out var weight))
                    {
                        throw new InvalidOperationException($"A weight was not provided for time period {timeId}.");
                    }

                    if (weight < 0)
                    {
                        throw new InvalidOperationException("Negative splash weights are not supported.");
                    }

                    return weight;
                });

            var productWeights = BuildProductWeights(targetCells);
            return targetCells.ToDictionary(
                cell => cell.Coordinate.Key,
                cell => timeWeights[cell.Coordinate.TimePeriodId] * productWeights[cell.Coordinate.ProductNodeId]);
        }

        var existingPlanWeights = targetCells.ToDictionary(
            cell => cell.Coordinate.Key,
            cell => Math.Max(cell.EffectiveValue, 0m));

        return existingPlanWeights.Values.Sum() > 0
            ? existingPlanWeights
            : targetCells.ToDictionary(cell => cell.Coordinate.Key, _ => 1m);
    }

    private static Dictionary<long, decimal> BuildProductWeights(IReadOnlyList<PlanningCell> targetCells)
    {
        var productWeights = targetCells
            .GroupBy(cell => cell.Coordinate.ProductNodeId)
            .ToDictionary(group => group.Key, group => Math.Max(group.Sum(cell => cell.EffectiveValue), 0m));

        if (productWeights.Values.Sum() > 0)
        {
            return productWeights;
        }

        return productWeights.Keys.ToDictionary(key => key, _ => 1m);
    }

    private static bool IsLeafWriteCoordinate(PlanningCellCoordinate coordinate, PlanningMetadataSnapshot metadata)
    {
        return metadata.ProductNodes[coordinate.ProductNodeId].IsLeaf &&
               !metadata.TimePeriods.Values.Any(period => period.ParentTimePeriodId == coordinate.TimePeriodId);
    }

    private static IReadOnlyList<long> GetLeafProductIds(long productNodeId, PlanningMetadataSnapshot metadata)
    {
        var descendants = metadata.ProductNodes.Values
            .Where(node => IsDescendantProduct(node.ProductNodeId, productNodeId, metadata))
            .Where(node => node.IsLeaf)
            .Select(node => node.ProductNodeId)
            .ToList();

        return descendants.Count == 0 ? new[] { productNodeId } : descendants;
    }

    private static bool IsDescendantProduct(long candidateId, long ancestorId, PlanningMetadataSnapshot metadata)
    {
        var current = candidateId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = metadata.ProductNodes[current];
            if (node.ParentProductNodeId is null)
            {
                return false;
            }

            current = node.ParentProductNodeId.Value;
        }
    }

    private static IReadOnlyList<long> GetLeafTimeIds(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        var children = metadata.TimePeriods.Values
            .Where(node => node.ParentTimePeriodId == timePeriodId)
            .Select(node => node.TimePeriodId)
            .ToList();

        if (children.Count == 0)
        {
            return new[] { timePeriodId };
        }

        return children
            .SelectMany(childId => GetLeafTimeIds(childId, metadata))
            .Distinct()
            .OrderBy(id => metadata.TimePeriods[id].SortOrder)
            .ToList();
    }

    private static void RecalculateAll(
        Dictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        long measureId)
    {
        var timeDepth = metadata.TimePeriods.Values.ToDictionary(period => period.TimePeriodId, period => GetTimeDepth(period.TimePeriodId));

        foreach (var leafMonthCell in workingCells.Values.Where(cell =>
                     metadata.ProductNodes[cell.Coordinate.ProductNodeId].IsLeaf &&
                     !metadata.TimePeriods.Values.Any(period => period.ParentTimePeriodId == cell.Coordinate.TimePeriodId)))
        {
            leafMonthCell.DerivedValue = leafMonthCell.InputValue ?? 0m;
            leafMonthCell.EffectiveValue = leafMonthCell.DerivedValue;
            leafMonthCell.CellKind = leafMonthCell.InputValue is null ? "leaf" : "input";
            leafMonthCell.OverrideValue = null;
            leafMonthCell.IsSystemGeneratedOverride = false;
        }

        var aggregateTimes = metadata.TimePeriods.Values
            .Where(period => metadata.TimePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId))
            .OrderByDescending(period => timeDepth[period.TimePeriodId])
            .ThenBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in metadata.ProductNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var aggregateTime in aggregateTimes)
            {
                var childTimeIds = metadata.TimePeriods.Values
                    .Where(period => period.ParentTimePeriodId == aggregateTime.TimePeriodId)
                    .Select(period => period.TimePeriodId)
                    .ToList();
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, leafNode.StoreId, leafNode.ProductNodeId, aggregateTime.TimePeriodId);
                var cell = workingCells[coordinate.Key];
                cell.DerivedValue = childTimeIds.Sum(childTimeId => workingCells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    leafNode.StoreId,
                    leafNode.ProductNodeId,
                    childTimeId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
                cell.CellKind = "calculated";
                cell.OverrideValue = null;
                cell.IsSystemGeneratedOverride = false;
            }
        }

        foreach (var productNode in metadata.ProductNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            var childProductIds = metadata.ProductNodes.Values
                .Where(node => node.ParentProductNodeId == productNode.ProductNodeId)
                .Select(node => node.ProductNodeId)
                .ToList();

            foreach (var timePeriod in metadata.TimePeriods.Values)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId);
                var cell = workingCells[coordinate.Key];
                cell.DerivedValue = childProductIds.Sum(childProductId => workingCells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    productNode.StoreId,
                    childProductId,
                    timePeriod.TimePeriodId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
                cell.CellKind = "calculated";
                cell.OverrideValue = null;
                cell.IsSystemGeneratedOverride = false;
            }
        }

        int GetTimeDepth(long timePeriodId)
        {
            var depth = 0;
            var current = metadata.TimePeriods[timePeriodId];
            while (current.ParentTimePeriodId is not null)
            {
                depth += 1;
                current = metadata.TimePeriods[current.ParentTimePeriodId.Value];
            }

            return depth;
        }
    }

    private async Task<IReadOnlyList<PlanningCellDeltaAudit>> PersistScenarioChangesAsync(
        IReadOnlyList<PlanningCell> originalCells,
        IEnumerable<PlanningCell> workingCells,
        string changeKind,
        CancellationToken cancellationToken)
    {
        var originalByKey = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var changedCells = new List<PlanningCell>();
        var deltas = new List<PlanningCellDeltaAudit>();

        foreach (var cell in workingCells)
        {
            if (!originalByKey.TryGetValue(cell.Coordinate.Key, out var original))
            {
                changedCells.Add(cell);
                continue;
            }

            if (!HasMaterialChange(original, cell))
            {
                continue;
            }

            var updated = cell.Clone();
            updated.RowVersion = original.RowVersion + 1;
            changedCells.Add(updated);

            if (original.EffectiveValue != updated.EffectiveValue)
            {
                deltas.Add(new PlanningCellDeltaAudit(updated.Coordinate, original.EffectiveValue, updated.EffectiveValue, original.IsLocked, changeKind));
            }
        }

        await _repository.UpsertCellsAsync(changedCells, cancellationToken);
        return deltas;
    }

    private static bool HasMaterialChange(PlanningCell left, PlanningCell right)
    {
        return left.InputValue != right.InputValue ||
               left.OverrideValue != right.OverrideValue ||
               left.DerivedValue != right.DerivedValue ||
               left.EffectiveValue != right.EffectiveValue ||
               left.IsLocked != right.IsLocked ||
               left.LockReason != right.LockReason ||
               left.LockedBy != right.LockedBy ||
               left.CellKind != right.CellKind;
    }

    private static bool IsLockedBySelfOrAncestor(
        PlanningCellCoordinate coordinate,
        IEnumerable<PlanningCell> scenarioCells,
        PlanningMetadataSnapshot metadata)
    {
        return scenarioCells.Any(cell =>
            cell.IsLocked &&
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsDescendantProduct(coordinate.ProductNodeId, cell.Coordinate.ProductNodeId, metadata) &&
            IsDescendantTime(coordinate.TimePeriodId, cell.Coordinate.TimePeriodId, metadata));
    }

    private static bool IsDescendantTime(long candidateId, long ancestorId, PlanningMetadataSnapshot metadata)
    {
        var current = candidateId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = metadata.TimePeriods[current];
            if (node.ParentTimePeriodId is null)
            {
                return false;
            }

            current = node.ParentTimePeriodId.Value;
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

    private async Task<(ProductNode Node, int CreatedCount)> EnsureNodeAsync(
        long scenarioVersionId,
        long measureId,
        string level,
        long? parentProductNodeId,
        string label,
        CancellationToken cancellationToken)
    {
        ProductNode? existingNode;
        if (level == "store")
        {
            existingNode = await _repository.FindProductNodeByPathAsync(new[] { label }, cancellationToken);
        }
        else
        {
            var metadata = await _repository.GetMetadataAsync(cancellationToken);
            var parent = metadata.ProductNodes[parentProductNodeId!.Value];
            existingNode = await _repository.FindProductNodeByPathAsync(parent.Path.Append(label).ToArray(), cancellationToken);
        }

        if (existingNode is not null)
        {
            return (existingNode, 0);
        }

        var createdNode = await _repository.AddRowAsync(new AddRowRequest(scenarioVersionId, measureId, level, parentProductNodeId, label, null), cancellationToken);
        return (createdNode, 1);
    }

    private async Task<HierarchyMappingResponse> BuildHierarchyMappingResponseAsync(CancellationToken cancellationToken)
    {
        var mappings = await _repository.GetHierarchyMappingsAsync(cancellationToken);
        return new HierarchyMappingResponse(
            mappings
                .OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase)
                .Select(entry => new HierarchyCategoryDto(
                    entry.Key,
                    entry.Value.OrderBy(value => value, StringComparer.OrdinalIgnoreCase).ToList()))
                .ToList());
    }

    private static decimal GetNumericCellValue(IXLCell cell)
    {
        if (cell.TryGetValue<decimal>(out var decimalValue))
        {
            return decimalValue;
        }

        if (decimal.TryParse(cell.GetString(), out var parsedValue))
        {
            return parsedValue;
        }

        throw new InvalidOperationException($"Cell {cell.Address} does not contain a numeric value.");
    }
}
