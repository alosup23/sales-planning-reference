using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed class InMemoryPlanningRepository : IPlanningRepository
{
    private readonly Dictionary<string, PlanningCell> _cells = new();
    private readonly List<PlanningActionAudit> _audits = new();
    private readonly PlanningMetadataSnapshot _metadata;
    private readonly IReadOnlyList<long> _year2026Months;

    public InMemoryPlanningRepository()
    {
        var productNodes = BuildProductNodes();
        var timePeriods = BuildTimePeriods();
        _metadata = new PlanningMetadataSnapshot(productNodes, timePeriods);
        _year2026Months = timePeriods.Values
            .Where(x => x.ParentTimePeriodId == 202600)
            .OrderBy(x => x.SortOrder)
            .Select(x => x.TimePeriodId)
            .ToList();

        SeedCells();
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken)
    {
        return Task.FromResult(_metadata);
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

    public Task<IReadOnlyList<PlanningCell>> GetCellsForProductAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        var cells = _cells.Values
            .Where(cell =>
                cell.Coordinate.ScenarioVersionId == scenarioVersionId &&
                cell.Coordinate.MeasureId == measureId &&
                cell.Coordinate.StoreId == storeId &&
                cell.Coordinate.ProductNodeId == productNodeId)
            .Select(cell => cell.Clone())
            .ToList();

        return Task.FromResult<IReadOnlyList<PlanningCell>>(cells);
    }

    public Task<IReadOnlyList<PlanningCell>> GetCellsForProductAndPeriodsAsync(
        long scenarioVersionId,
        long measureId,
        long storeId,
        long productNodeId,
        IEnumerable<long> timePeriodIds,
        CancellationToken cancellationToken)
    {
        var timeSet = timePeriodIds.ToHashSet();
        var cells = _cells.Values
            .Where(cell =>
                cell.Coordinate.ScenarioVersionId == scenarioVersionId &&
                cell.Coordinate.MeasureId == measureId &&
                cell.Coordinate.StoreId == storeId &&
                cell.Coordinate.ProductNodeId == productNodeId &&
                timeSet.Contains(cell.Coordinate.TimePeriodId))
            .OrderBy(cell => _metadata.TimePeriods[cell.Coordinate.TimePeriodId].SortOrder)
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
        const long storeId = 101;
        var rows = _metadata.ProductNodes.Values
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path))
            .Select(node =>
            {
                var cells = _cells.Values
                    .Where(cell =>
                        cell.Coordinate.ScenarioVersionId == scenarioVersionId &&
                        cell.Coordinate.MeasureId == measureId &&
                        cell.Coordinate.StoreId == storeId &&
                        cell.Coordinate.ProductNodeId == node.ProductNodeId)
                    .ToDictionary(
                        cell => cell.Coordinate.TimePeriodId,
                        cell => new GridCellDto(
                            cell.EffectiveValue,
                            cell.IsLocked,
                            cell.CellKind == "calculated",
                            cell.OverrideValue is not null,
                            cell.RowVersion,
                            cell.CellKind));

                return new GridRowDto(
                    storeId,
                    node.ProductNodeId,
                    node.Label,
                    node.Level,
                    node.Path,
                    node.IsLeaf,
                    cells);
            })
            .ToList();

        var periods = _metadata.TimePeriods.Values
            .OrderBy(node => node.SortOrder)
            .Select(node => new GridPeriodDto(node.TimePeriodId, node.Label, node.Grain, node.ParentTimePeriodId, node.SortOrder))
            .ToList();

        return Task.FromResult(new GridSliceResponse(scenarioVersionId, measureId, periods, rows));
    }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        _cells.Clear();
        _audits.Clear();
        SeedCells();
        return Task.CompletedTask;
    }

    private void SeedCells()
    {
        const long scenarioVersionId = 1;
        const long measureId = 1;
        const long storeId = 101;

        var leafValues = new Dictionary<long, Dictionary<long, decimal>>
        {
            [2110] = new()
            {
                [202601] = 600m, [202602] = 750m, [202603] = 700m, [202604] = 710m,
                [202605] = 720m, [202606] = 735m, [202607] = 760m, [202608] = 770m,
                [202609] = 730m, [202610] = 690m, [202611] = 720m, [202612] = 780m
            },
            [2120] = new()
            {
                [202601] = 250m, [202602] = 260m, [202603] = 255m, [202604] = 265m,
                [202605] = 270m, [202606] = 280m, [202607] = 290m, [202608] = 285m,
                [202609] = 275m, [202610] = 268m, [202611] = 272m, [202612] = 295m
            },
            [2210] = new()
            {
                [202601] = 420m, [202602] = 430m, [202603] = 410m, [202604] = 425m,
                [202605] = 440m, [202606] = 450m, [202607] = 460m, [202608] = 470m,
                [202609] = 455m, [202610] = 440m, [202611] = 448m, [202612] = 475m
            }
        };

        foreach (var productNode in _metadata.ProductNodes.Values)
        {
            foreach (var period in _metadata.TimePeriods.Values)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNode.ProductNodeId, period.TimePeriodId);
                _cells[coordinate.Key] = new PlanningCell
                {
                    Coordinate = coordinate,
                    InputValue = null,
                    OverrideValue = null,
                    DerivedValue = 0,
                    EffectiveValue = 0,
                    IsLocked = false,
                    RowVersion = 1,
                    CellKind = productNode.IsLeaf && period.Grain == "month" ? "leaf" : "calculated"
                };
            }
        }

        foreach (var leaf in leafValues)
        {
            foreach (var month in leaf.Value)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, leaf.Key, month.Key);
                var cell = _cells[coordinate.Key];
                cell.InputValue = month.Value;
                cell.DerivedValue = month.Value;
                cell.EffectiveValue = month.Value;
                cell.RowVersion = 2;
                _cells[coordinate.Key] = cell;
            }
        }

        var lockedCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, 2110, 202602);
        _cells[lockedCoordinate.Key].IsLocked = true;
        _cells[lockedCoordinate.Key].LockReason = "Manager-held sample lock";
        _cells[lockedCoordinate.Key].LockedBy = "demo.manager";

        RecalculateAll(scenarioVersionId, measureId, storeId);
    }

    private void RecalculateAll(long scenarioVersionId, long measureId, long storeId)
    {
        foreach (var productNode in _metadata.ProductNodes.Values.Where(node => node.IsLeaf))
        {
            var yearCoordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNode.ProductNodeId, 202600);
            var yearCell = _cells[yearCoordinate.Key];
            yearCell.DerivedValue = _year2026Months.Sum(month => _cells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, productNode.ProductNodeId, month).Key].EffectiveValue);
            yearCell.EffectiveValue = yearCell.OverrideValue ?? yearCell.DerivedValue;
            _cells[yearCoordinate.Key] = yearCell;
        }

        foreach (var node in _metadata.ProductNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            foreach (var period in _metadata.TimePeriods.Values)
            {
                var childIds = _metadata.ProductNodes.Values.Where(child => child.ParentProductNodeId == node.ProductNodeId).Select(child => child.ProductNodeId);
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, node.ProductNodeId, period.TimePeriodId);
                var cell = _cells[coordinate.Key];
                cell.DerivedValue = childIds.Sum(childId => _cells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, childId, period.TimePeriodId).Key].EffectiveValue);
                cell.EffectiveValue = cell.OverrideValue ?? cell.DerivedValue;
                _cells[coordinate.Key] = cell;
            }
        }
    }

    private static Dictionary<long, ProductNode> BuildProductNodes()
    {
        return new List<ProductNode>
        {
            new(2000, null, "Store A", 0, new[] { "Store A" }, false),
            new(2100, 2000, "Beverages", 1, new[] { "Store A", "Beverages" }, false),
            new(2110, 2100, "Soft Drinks", 2, new[] { "Store A", "Beverages", "Soft Drinks" }, true),
            new(2120, 2100, "Tea", 2, new[] { "Store A", "Beverages", "Tea" }, true),
            new(2200, 2000, "Snacks", 1, new[] { "Store A", "Snacks" }, false),
            new(2210, 2200, "Chips", 2, new[] { "Store A", "Snacks", "Chips" }, true)
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
