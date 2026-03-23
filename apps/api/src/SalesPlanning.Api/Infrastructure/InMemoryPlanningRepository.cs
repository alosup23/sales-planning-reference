using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed class InMemoryPlanningRepository : IPlanningRepository
{
    private readonly Dictionary<string, PlanningCell> _cells = new();
    private readonly List<PlanningActionAudit> _audits = new();
    private Dictionary<long, ProductNode> _productNodes = new();
    private readonly Dictionary<long, TimePeriodNode> _timePeriods;
    private readonly Dictionary<string, List<string>> _hierarchyMappings = new(StringComparer.OrdinalIgnoreCase);
    private long _productNodeSeed = 3000;
    private long _storeSeed = 200;

    public InMemoryPlanningRepository()
    {
        _timePeriods = BuildTimePeriods();
        ResetState();
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken)
    {
        return Task.FromResult(new PlanningMetadataSnapshot(_productNodes, _timePeriods));
    }

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken)
    {
        var cells = coordinates
            .Where(coordinate => _cells.ContainsKey(coordinate.Key))
            .Select(coordinate => _cells[coordinate.Key].Clone())
            .ToList();

        return Task.FromResult<IReadOnlyList<PlanningCell>>(cells);
    }

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken)
    {
        return Task.FromResult(_cells.TryGetValue(coordinate.Key, out var cell) ? cell.Clone() : null);
    }

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken)
    {
        var cells = _cells.Values
            .Where(cell =>
                cell.Coordinate.ScenarioVersionId == scenarioVersionId &&
                cell.Coordinate.MeasureId == measureId)
            .Select(cell => cell.Clone())
            .ToList();

        return Task.FromResult<IReadOnlyList<PlanningCell>>(cells);
    }

    public Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        foreach (var cell in cells)
        {
            _cells[cell.Coordinate.Key] = cell.Clone();
        }

        return Task.CompletedTask;
    }

    public Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        _audits.Add(audit);
        return Task.CompletedTask;
    }

    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken)
    {
        var nextActionId = (_audits.Count == 0 ? 1000 : _audits.Max(audit => audit.ActionId)) + 1;
        return Task.FromResult(nextActionId);
    }

    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        var results = _audits
            .Where(audit => audit.Deltas.Any(delta =>
                delta.Coordinate.ScenarioVersionId == scenarioVersionId &&
                delta.Coordinate.MeasureId == measureId &&
                delta.Coordinate.StoreId == storeId &&
                delta.Coordinate.ProductNodeId == productNodeId))
            .OrderByDescending(audit => audit.CreatedAt)
            .ToList();

        return Task.FromResult<IReadOnlyList<PlanningActionAudit>>(results);
    }

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken)
    {
        var scenarioCells = _cells.Values
            .Where(cell =>
                cell.Coordinate.ScenarioVersionId == scenarioVersionId &&
                cell.Coordinate.MeasureId == measureId)
            .Select(cell => cell.Clone())
            .ToList();

        var rows = _productNodes.Values
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .Select(node =>
            {
                var cells = scenarioCells
                    .Where(cell =>
                        cell.Coordinate.StoreId == node.StoreId &&
                        cell.Coordinate.ProductNodeId == node.ProductNodeId)
                    .ToDictionary(
                        cell => cell.Coordinate.TimePeriodId,
                        cell => new GridCellDto(
                            cell.EffectiveValue,
                            IsEffectivelyLocked(cell.Coordinate, scenarioCells),
                            cell.CellKind == "calculated",
                            cell.OverrideValue is not null,
                            cell.RowVersion,
                            cell.CellKind));

                return new GridRowDto(
                    node.StoreId,
                    node.ProductNodeId,
                    node.Label,
                    node.Level,
                    node.Path,
                    node.IsLeaf,
                    cells);
            })
            .ToList();

        var periods = _timePeriods.Values
            .OrderBy(node => node.SortOrder)
            .Select(node => new GridPeriodDto(node.TimePeriodId, node.Label, node.Grain, node.ParentTimePeriodId, node.SortOrder))
            .ToList();

        return Task.FromResult(new GridSliceResponse(scenarioVersionId, measureId, periods, rows));
    }

    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        var normalizedLevel = request.Level.Trim().ToLowerInvariant();
        ProductNode node;

        switch (normalizedLevel)
        {
            case "store":
            {
                var storeId = ++_storeSeed;
                var productNodeId = ++_productNodeSeed;
                node = new ProductNode(productNodeId, storeId, null, request.Label.Trim(), 0, new[] { request.Label.Trim() }, false);
                _productNodes[node.ProductNodeId] = node;
                InitializeCellsForNode(request.ScenarioVersionId, request.MeasureId, node);
                if (request.CopyFromStoreId is not null)
                {
                    CloneStoreHierarchyAndData(
                        request.ScenarioVersionId,
                        request.MeasureId,
                        request.CopyFromStoreId.Value,
                        node);
                }
                break;
            }
            case "category":
            {
                var parent = GetRequiredNode(request.ParentProductNodeId, 0, "category");
                node = new ProductNode(
                    ++_productNodeSeed,
                    parent.StoreId,
                    parent.ProductNodeId,
                    request.Label.Trim(),
                    1,
                    parent.Path.Append(request.Label.Trim()).ToArray(),
                    false);
                _productNodes[node.ProductNodeId] = node;
                InitializeCellsForNode(request.ScenarioVersionId, request.MeasureId, node);
                UpsertHierarchyCategory(node.Label);
                break;
            }
            case "subcategory":
            {
                var parent = GetRequiredNode(request.ParentProductNodeId, 1, "subcategory");
                if (parent.IsLeaf)
                {
                    _productNodes[parent.ProductNodeId] = parent with { IsLeaf = false };
                }

                node = new ProductNode(
                    ++_productNodeSeed,
                    parent.StoreId,
                    parent.ProductNodeId,
                    request.Label.Trim(),
                    2,
                    parent.Path.Append(request.Label.Trim()).ToArray(),
                    true);
                _productNodes[node.ProductNodeId] = node;
                InitializeCellsForNode(request.ScenarioVersionId, request.MeasureId, node);
                UpsertHierarchySubcategory(parent.Label, node.Label);
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported row level '{request.Level}'.");
        }

        return Task.FromResult(node);
    }

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken)
    {
        var node = _productNodes.Values.FirstOrDefault(candidate =>
            candidate.Path.Length == path.Length &&
            candidate.Path.Zip(path, (left, right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase)).All(match => match));

        return Task.FromResult(node);
    }

    public Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetHierarchyMappingsAsync(CancellationToken cancellationToken)
    {
        var snapshot = _hierarchyMappings.ToDictionary(
            entry => entry.Key,
            entry => (IReadOnlyList<string>)entry.Value.ToList(),
            StringComparer.OrdinalIgnoreCase);
        return Task.FromResult((IReadOnlyDictionary<string, IReadOnlyList<string>>)snapshot);
    }

    public Task UpsertHierarchyCategoryAsync(string categoryLabel, CancellationToken cancellationToken)
    {
        UpsertHierarchyCategory(categoryLabel);
        return Task.CompletedTask;
    }

    public Task UpsertHierarchySubcategoryAsync(string categoryLabel, string subcategoryLabel, CancellationToken cancellationToken)
    {
        UpsertHierarchySubcategory(categoryLabel, subcategoryLabel);
        return Task.CompletedTask;
    }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        ResetState();
        return Task.CompletedTask;
    }

    private void ResetState()
    {
        _cells.Clear();
        _audits.Clear();
        _productNodes = BuildProductNodes();
        _hierarchyMappings.Clear();
        SeedHierarchyMappings();
        SeedCells();
    }

    private void SeedCells()
    {
        const long scenarioVersionId = 1;
        const long measureId = 1;

        foreach (var productNode in _productNodes.Values)
        {
            InitializeCellsForNode(scenarioVersionId, measureId, productNode);
        }

        var leafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2110)] = new()
            {
                [202601] = 600m, [202602] = 750m, [202603] = 700m, [202604] = 710m,
                [202605] = 720m, [202606] = 735m, [202607] = 760m, [202608] = 770m,
                [202609] = 730m, [202610] = 690m, [202611] = 720m, [202612] = 780m
            },
            [(101, 2120)] = new()
            {
                [202601] = 250m, [202602] = 260m, [202603] = 255m, [202604] = 265m,
                [202605] = 270m, [202606] = 280m, [202607] = 290m, [202608] = 285m,
                [202609] = 275m, [202610] = 268m, [202611] = 272m, [202612] = 295m
            },
            [(101, 2210)] = new()
            {
                [202601] = 420m, [202602] = 430m, [202603] = 410m, [202604] = 425m,
                [202605] = 440m, [202606] = 450m, [202607] = 460m, [202608] = 470m,
                [202609] = 455m, [202610] = 440m, [202611] = 448m, [202612] = 475m
            }
        };

        foreach (var (key, monthValues) in leafValues)
        {
            foreach (var month in monthValues)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, key.StoreId, key.ProductNodeId, month.Key);
                var cell = _cells[coordinate.Key];
                cell.InputValue = month.Value;
                cell.DerivedValue = month.Value;
                cell.EffectiveValue = month.Value;
                cell.RowVersion = 2;
                cell.CellKind = "input";
                _cells[coordinate.Key] = cell;
            }
        }

        var lockedCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, 101, 2110, 202602);
        _cells[lockedCoordinate.Key].IsLocked = true;
        _cells[lockedCoordinate.Key].LockReason = "Manager-held sample lock";
        _cells[lockedCoordinate.Key].LockedBy = "demo.manager";

        RecalculateSeedTotals(scenarioVersionId, measureId);
    }

    private void InitializeCellsForNode(long scenarioVersionId, long measureId, ProductNode node)
    {
        foreach (var period in _timePeriods.Values)
        {
            var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, period.TimePeriodId);
            if (_cells.ContainsKey(coordinate.Key))
            {
                continue;
            }

            _cells[coordinate.Key] = new PlanningCell
            {
                Coordinate = coordinate,
                DerivedValue = 0,
                EffectiveValue = 0,
                RowVersion = 1,
                CellKind = node.IsLeaf && period.Grain == "month" ? "leaf" : "calculated"
            };
        }
    }

    private void CloneStoreHierarchyAndData(long scenarioVersionId, long measureId, long sourceStoreId, ProductNode targetStoreNode)
    {
        var sourceRootNode = _productNodes.Values.SingleOrDefault(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is null);
        if (sourceRootNode is null)
        {
            throw new InvalidOperationException($"Store {sourceStoreId} was not found for copy.");
        }

        var sourceNodes = _productNodes.Values
            .Where(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is not null)
            .OrderBy(node => node.Level)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .ToList();

        var nodeMap = new Dictionary<long, ProductNode>
        {
            [sourceRootNode.ProductNodeId] = targetStoreNode
        };

        foreach (var sourceNode in sourceNodes)
        {
            var parent = nodeMap[sourceNode.ParentProductNodeId!.Value];
            var clonedNode = new ProductNode(
                ++_productNodeSeed,
                targetStoreNode.StoreId,
                parent.ProductNodeId,
                sourceNode.Label,
                sourceNode.Level,
                parent.Path.Append(sourceNode.Label).ToArray(),
                sourceNode.IsLeaf);

            _productNodes[clonedNode.ProductNodeId] = clonedNode;
            nodeMap[sourceNode.ProductNodeId] = clonedNode;
            InitializeCellsForNode(scenarioVersionId, measureId, clonedNode);
        }

        foreach (var sourceNode in sourceNodes.Prepend(sourceRootNode))
        {
            var targetNode = nodeMap[sourceNode.ProductNodeId];
            foreach (var period in _timePeriods.Values)
            {
                var sourceCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, sourceStoreId, sourceNode.ProductNodeId, period.TimePeriodId);
                var targetCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, targetStoreNode.StoreId, targetNode.ProductNodeId, period.TimePeriodId);

                if (!_cells.TryGetValue(sourceCoordinate.Key, out var sourceCell))
                {
                    continue;
                }

                var clonedCell = sourceCell.Clone();
                clonedCell.Coordinate = targetCoordinate;
                clonedCell.IsLocked = false;
                clonedCell.LockReason = null;
                clonedCell.LockedBy = null;
                _cells[targetCoordinate.Key] = clonedCell;
            }
        }
    }

    private void RecalculateSeedTotals(long scenarioVersionId, long measureId)
    {
        var aggregateTimes = _timePeriods.Values
            .Where(period => _timePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId))
            .OrderBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in _productNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var aggregateTime in aggregateTimes)
            {
                var childTimeIds = _timePeriods.Values
                    .Where(period => period.ParentTimePeriodId == aggregateTime.TimePeriodId)
                    .Select(period => period.TimePeriodId)
                    .ToList();
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, leafNode.StoreId, leafNode.ProductNodeId, aggregateTime.TimePeriodId);
                var cell = _cells[coordinate.Key];
                cell.DerivedValue = childTimeIds.Sum(childTimeId => _cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    leafNode.StoreId,
                    leafNode.ProductNodeId,
                    childTimeId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
                _cells[coordinate.Key] = cell;
            }
        }

        foreach (var node in _productNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            var childIds = _productNodes.Values
                .Where(child => child.ParentProductNodeId == node.ProductNodeId)
                .Select(child => child.ProductNodeId)
                .ToList();

            foreach (var period in _timePeriods.Values)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, period.TimePeriodId);
                var cell = _cells[coordinate.Key];
                cell.DerivedValue = childIds.Sum(childId => _cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    node.StoreId,
                    childId,
                    period.TimePeriodId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
                _cells[coordinate.Key] = cell;
            }
        }
    }

    private ProductNode GetRequiredNode(long? productNodeId, int expectedLevel, string childLevel)
    {
        if (productNodeId is null || !_productNodes.TryGetValue(productNodeId.Value, out var parent))
        {
            throw new InvalidOperationException($"A parent row is required to add a {childLevel}.");
        }

        if (parent.Level != expectedLevel)
        {
            throw new InvalidOperationException($"A {childLevel} can only be added beneath a level {expectedLevel} row.");
        }

        return parent;
    }

    private void SeedHierarchyMappings()
    {
        foreach (var category in _productNodes.Values.Where(node => node.Level == 1))
        {
            UpsertHierarchyCategory(category.Label);
        }

        foreach (var subcategory in _productNodes.Values.Where(node => node.Level == 2))
        {
            var category = _productNodes[subcategory.ParentProductNodeId!.Value];
            UpsertHierarchySubcategory(category.Label, subcategory.Label);
        }
    }

    private void UpsertHierarchyCategory(string categoryLabel)
    {
        var normalizedLabel = categoryLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedLabel))
        {
            throw new InvalidOperationException("Category labels cannot be empty.");
        }

        var existingKey = _hierarchyMappings.Keys.FirstOrDefault(key => string.Equals(key, normalizedLabel, StringComparison.OrdinalIgnoreCase));
        if (existingKey is not null)
        {
            return;
        }

        _hierarchyMappings[normalizedLabel] = new List<string>();
    }

    private void UpsertHierarchySubcategory(string categoryLabel, string subcategoryLabel)
    {
        UpsertHierarchyCategory(categoryLabel);

        var categoryKey = _hierarchyMappings.Keys.First(key => string.Equals(key, categoryLabel.Trim(), StringComparison.OrdinalIgnoreCase));
        var normalizedSubcategory = subcategoryLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedSubcategory))
        {
            throw new InvalidOperationException("Subcategory labels cannot be empty.");
        }

        if (_hierarchyMappings[categoryKey].Any(existing => string.Equals(existing, normalizedSubcategory, StringComparison.OrdinalIgnoreCase)))
        {
            return;
        }

        _hierarchyMappings[categoryKey].Add(normalizedSubcategory);
        _hierarchyMappings[categoryKey].Sort(StringComparer.OrdinalIgnoreCase);
    }

    private bool IsEffectivelyLocked(PlanningCellCoordinate coordinate, IReadOnlyCollection<PlanningCell> scenarioCells)
    {
        return scenarioCells.Any(cell =>
            cell.IsLocked &&
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsAncestorOrSelf(_productNodes, cell.Coordinate.ProductNodeId, coordinate.ProductNodeId) &&
            IsAncestorOrSelf(_timePeriods, cell.Coordinate.TimePeriodId, coordinate.TimePeriodId));
    }

    private static bool IsAncestorOrSelf(IReadOnlyDictionary<long, ProductNode> nodes, long ancestorId, long descendantId)
    {
        var current = descendantId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = nodes[current];
            if (node.ParentProductNodeId is null)
            {
                return false;
            }

            current = node.ParentProductNodeId.Value;
        }
    }

    private static bool IsAncestorOrSelf(IReadOnlyDictionary<long, TimePeriodNode> nodes, long ancestorId, long descendantId)
    {
        var current = descendantId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = nodes[current];
            if (node.ParentTimePeriodId is null)
            {
                return false;
            }

            current = node.ParentTimePeriodId.Value;
        }
    }

    private static Dictionary<long, ProductNode> BuildProductNodes()
    {
        return new List<ProductNode>
        {
            new(2000, 101, null, "Store A", 0, new[] { "Store A" }, false),
            new(2100, 101, 2000, "Beverages", 1, new[] { "Store A", "Beverages" }, false),
            new(2110, 101, 2100, "Soft Drinks", 2, new[] { "Store A", "Beverages", "Soft Drinks" }, true),
            new(2120, 101, 2100, "Tea", 2, new[] { "Store A", "Beverages", "Tea" }, true),
            new(2200, 101, 2000, "Snacks", 1, new[] { "Store A", "Snacks" }, false),
            new(2210, 101, 2200, "Chips", 2, new[] { "Store A", "Snacks", "Chips" }, true)
        }.ToDictionary(node => node.ProductNodeId);
    }

    private static Dictionary<long, TimePeriodNode> BuildTimePeriods()
    {
        return new List<TimePeriodNode>
        {
            new(202600, null, "FY26", "year", 1),
            new(202601, 202600, "Jan", "month", 2),
            new(202602, 202600, "Feb", "month", 3),
            new(202603, 202600, "Mar", "month", 4),
            new(202604, 202600, "Apr", "month", 5),
            new(202605, 202600, "May", "month", 6),
            new(202606, 202600, "Jun", "month", 7),
            new(202607, 202600, "Jul", "month", 8),
            new(202608, 202600, "Aug", "month", 9),
            new(202609, 202600, "Sep", "month", 10),
            new(202610, 202600, "Oct", "month", 11),
            new(202611, 202600, "Nov", "month", 12),
            new(202612, 202600, "Dec", "month", 13)
        }.ToDictionary(period => period.TimePeriodId);
    }
}
