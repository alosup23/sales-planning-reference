using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using SQLitePCL;

namespace SalesPlanning.Api.Infrastructure;

public sealed partial class SqlitePlanningRepository : IPlanningRepository
{
    private static readonly IReadOnlyList<long> SupportedMeasureIds = PlanningMeasures.SupportedMeasureIds;

    static SqlitePlanningRepository()
    {
        Batteries.Init();
    }

    private readonly string _connectionString;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _initialized;

    public SqlitePlanningRepository(string databasePath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath) ?? ".");
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString();
    }

    public Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        return action(cancellationToken);
    }

    public async Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        var timePeriods = await LoadTimePeriodsAsync(connection, null, cancellationToken);
        var stores = await LoadStoreMetadataAsync(connection, null, cancellationToken);
        return new PlanningMetadataSnapshot(productNodes, timePeriods, stores);
    }

    public async Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        var coordinateSet = coordinates.Select(coordinate => coordinate.Key).ToHashSet(StringComparer.Ordinal);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var cells = await LoadCellsAsync(connection, null, cancellationToken);
        return cells
            .Where(cell => coordinateSet.Contains(cell.Coordinate.Key))
            .Select(cell => cell.Clone())
            .ToList();
    }

    public async Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   input_value,
                   override_value,
                   is_system_generated_override,
                   derived_value,
                   effective_value,
                   growth_factor,
                   is_locked,
                   lock_reason,
                   locked_by,
                   row_version,
                   cell_kind
            from planning_cells
            where scenario_version_id = $scenarioVersionId
              and measure_id = $measureId
              and store_id = $storeId
              and product_node_id = $productNodeId
              and time_period_id = $timePeriodId;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("$measureId", coordinate.MeasureId);
        command.Parameters.AddWithValue("$storeId", coordinate.StoreId);
        command.Parameters.AddWithValue("$productNodeId", coordinate.ProductNodeId);
        command.Parameters.AddWithValue("$timePeriodId", coordinate.TimePeriodId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadPlanningCell(reader);
    }

    public async Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var cells = await LoadCellsAsync(connection, null, cancellationToken);
        return cells
            .Where(cell => cell.Coordinate.ScenarioVersionId == scenarioVersionId)
            .Select(cell => cell.Clone())
            .ToList();
    }

    public async Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var cell in cells)
            {
                await UpsertCellAsync(connection, transaction, cell, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    insert into audits (action_id, action_type, method, user_id, comment, created_at)
                    values ($actionId, $actionType, $method, $userId, $comment, $createdAt);
                    """;
                command.Parameters.AddWithValue("$actionId", audit.ActionId);
                command.Parameters.AddWithValue("$actionType", audit.ActionType);
                command.Parameters.AddWithValue("$method", audit.Method);
                command.Parameters.AddWithValue("$userId", audit.UserId);
                command.Parameters.AddWithValue("$comment", (object?)audit.Comment ?? DBNull.Value);
                command.Parameters.AddWithValue("$createdAt", audit.CreatedAt.ToString("O"));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var delta in audit.Deltas)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
                    insert into audit_deltas (
                        action_id,
                        scenario_version_id,
                        measure_id,
                        store_id,
                        product_node_id,
                        time_period_id,
                        old_value,
                        new_value,
                        was_locked,
                        change_kind)
                    values (
                        $actionId,
                        $scenarioVersionId,
                        $measureId,
                        $storeId,
                        $productNodeId,
                        $timePeriodId,
                        $oldValue,
                        $newValue,
                        $wasLocked,
                        $changeKind);
                    """;
                command.Parameters.AddWithValue("$actionId", audit.ActionId);
                command.Parameters.AddWithValue("$scenarioVersionId", delta.Coordinate.ScenarioVersionId);
                command.Parameters.AddWithValue("$measureId", delta.Coordinate.MeasureId);
                command.Parameters.AddWithValue("$storeId", delta.Coordinate.StoreId);
                command.Parameters.AddWithValue("$productNodeId", delta.Coordinate.ProductNodeId);
                command.Parameters.AddWithValue("$timePeriodId", delta.Coordinate.TimePeriodId);
                command.Parameters.AddWithValue("$oldValue", delta.OldValue);
                command.Parameters.AddWithValue("$newValue", delta.NewValue);
                command.Parameters.AddWithValue("$wasLocked", delta.WasLocked ? 1 : 0);
                command.Parameters.AddWithValue("$changeKind", delta.ChangeKind);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<long> GetNextActionIdAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "select coalesce(max(action_id), 1000) + 1 from audits;";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(value);
    }

    public async Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var audits = new List<PlanningActionAudit>();

        await using var auditCommand = connection.CreateCommand();
        auditCommand.CommandText = """
            select distinct a.action_id,
                            a.action_type,
                            a.method,
                            a.user_id,
                            a.comment,
                            a.created_at
            from audits a
            inner join audit_deltas d on d.action_id = a.action_id
            where d.scenario_version_id = $scenarioVersionId
              and d.measure_id = $measureId
              and d.store_id = $storeId
              and d.product_node_id = $productNodeId
            order by a.created_at desc;
            """;
        auditCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        auditCommand.Parameters.AddWithValue("$measureId", measureId);
        auditCommand.Parameters.AddWithValue("$storeId", storeId);
        auditCommand.Parameters.AddWithValue("$productNodeId", productNodeId);

        var headers = new List<(long ActionId, string ActionType, string Method, string UserId, string? Comment, DateTimeOffset CreatedAt)>();
        await using (var reader = await auditCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                headers.Add((
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    DateTimeOffset.Parse(reader.GetString(5))));
            }
        }

        foreach (var header in headers)
        {
            var deltas = new List<PlanningCellDeltaAudit>();
            await using var deltaCommand = connection.CreateCommand();
            deltaCommand.CommandText = """
                select scenario_version_id,
                       measure_id,
                       store_id,
                       product_node_id,
                       time_period_id,
                       old_value,
                       new_value,
                       was_locked,
                       change_kind
                from audit_deltas
                where action_id = $actionId
                order by audit_delta_id asc;
                """;
            deltaCommand.Parameters.AddWithValue("$actionId", header.ActionId);

            await using var reader = await deltaCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                deltas.Add(new PlanningCellDeltaAudit(
                    new PlanningCellCoordinate(
                        reader.GetInt64(0),
                        reader.GetInt64(1),
                        reader.GetInt64(2),
                        reader.GetInt64(3),
                        reader.GetInt64(4)),
                    ReadDecimal(reader, 5),
                    ReadDecimal(reader, 6),
                    reader.GetInt64(7) == 1,
                    reader.GetString(8)));
            }

            audits.Add(new PlanningActionAudit(
                header.ActionId,
                header.ActionType,
                header.Method,
                header.UserId,
                header.Comment,
                header.CreatedAt,
                deltas));
        }

        return audits;
    }

    public async Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        var timePeriods = await LoadTimePeriodsAsync(connection, null, cancellationToken);
        var stores = await LoadStoreMetadataAsync(connection, null, cancellationToken);
        var hierarchyMappings = await LoadHierarchyMappingsAsync(connection, null, cancellationToken);

        var expandedNodeSet = expandedProductNodeIds?.ToHashSet() ?? [];

        var visibleNodes = productNodes.Values
            .Where(node => ShouldIncludeGridNode(node, selectedStoreId, selectedDepartmentLabel, expandedNodeSet, productNodes, expandAllBranches))
            .ToList();

        var visibleNodeIds = visibleNodes
            .Select(node => node.ProductNodeId)
            .ToArray();

        var scenarioCells = await LoadScenarioCellsForNodesAsync(
            connection,
            null,
            scenarioVersionId,
            visibleNodeIds,
            cancellationToken);

        var rows = BuildGridRows(visibleNodes, scenarioCells, productNodes, timePeriods, stores, hierarchyMappings);

        var periods = timePeriods.Values
            .OrderBy(node => node.SortOrder)
            .Select(node => new GridPeriodDto(node.TimePeriodId, node.Label, node.Grain, node.ParentTimePeriodId, node.SortOrder))
            .ToList();

        var measures = PlanningMeasures.Definitions
            .Select(definition => new GridMeasureDto(
                definition.MeasureId,
                definition.Label,
                definition.DecimalPlaces,
                definition.DerivedAtAggregateLevels,
                definition.DisplayAsPercent,
                definition.EditableAtLeaf,
                definition.EditableAtAggregate))
            .ToList();

        return new GridSliceResponse(scenarioVersionId, measures, periods, rows);
    }

    public async Task<GridBranchResponse> GetGridBranchRowsAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        if (!productNodes.TryGetValue(parentProductNodeId, out var parentNode))
        {
            throw new InvalidOperationException($"Branch {parentProductNodeId} was not found.");
        }

        var children = productNodes.Values
            .Where(node => node.ParentProductNodeId == parentProductNodeId)
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (children.Count == 0)
        {
            return new GridBranchResponse(scenarioVersionId, parentProductNodeId, []);
        }

        var timePeriods = await LoadTimePeriodsAsync(connection, null, cancellationToken);
        var stores = await LoadStoreMetadataAsync(connection, null, cancellationToken);
        var hierarchyMappings = await LoadHierarchyMappingsAsync(connection, null, cancellationToken);

        var relevantNodeIds = children
            .Select(node => node.ProductNodeId)
            .Concat(GetAncestorProductNodeIds(parentNode, productNodes))
            .Distinct()
            .ToArray();

        var scenarioCells = await LoadScenarioCellsForNodesAsync(
            connection,
            null,
            scenarioVersionId,
            relevantNodeIds,
            cancellationToken);

        var rows = BuildGridRows(children, scenarioCells, productNodes, timePeriods, stores, hierarchyMappings);
        return new GridBranchResponse(scenarioVersionId, parentProductNodeId, rows);
    }

    private static bool ShouldIncludeGridNode(
        ProductNode node,
        long? selectedStoreId,
        string? selectedDepartmentLabel,
        IReadOnlySet<long> expandedProductNodeIds,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        bool expandAllBranches)
    {
        if (node.Level == 0)
        {
            return true;
        }

        if (!string.IsNullOrWhiteSpace(selectedDepartmentLabel))
        {
            if (node.Level == 1)
            {
                return true;
            }

            if (!string.Equals(GetDepartmentLabel(node), selectedDepartmentLabel, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (node.Level <= 2)
            {
                return true;
            }

            if (expandAllBranches)
            {
                return true;
            }

            return node.ParentProductNodeId is long departmentParentProductNodeId
                   && expandedProductNodeIds.Contains(departmentParentProductNodeId)
                   && productNodes.ContainsKey(departmentParentProductNodeId);
        }

        if (selectedStoreId is not null && node.StoreId != selectedStoreId.Value)
        {
            return false;
        }

        if (selectedStoreId is null)
        {
            if (expandAllBranches)
            {
                return true;
            }

            return node.ParentProductNodeId is long allStoresParentProductNodeId
                   && expandedProductNodeIds.Contains(allStoresParentProductNodeId)
                   && productNodes.ContainsKey(allStoresParentProductNodeId);
        }

        if (expandAllBranches)
        {
            return true;
        }

        if (node.Level <= 1)
        {
            return true;
        }

        return node.ParentProductNodeId is long parentProductNodeId
               && expandedProductNodeIds.Contains(parentProductNodeId)
               && productNodes.ContainsKey(parentProductNodeId);
    }

    private static string GetDepartmentLabel(ProductNode node)
    {
        if (node.Level <= 1)
        {
            return node.Label;
        }

        return node.Path.ElementAtOrDefault(1) ?? node.Label;
    }

    private static IReadOnlyList<GridRowDto> BuildGridRows(
        IReadOnlyList<ProductNode> nodes,
        IReadOnlyCollection<PlanningCell> scenarioCells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        IReadOnlyDictionary<long, StoreNodeMetadata> stores,
        IReadOnlyList<HierarchyDepartmentRecord> hierarchyMappings)
    {
        return nodes
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .Select(node =>
            {
                var resolvedMetadata = ResolveNodeMetadata(node, stores, hierarchyMappings);
                var cells = scenarioCells
                    .Where(cell => cell.Coordinate.StoreId == node.StoreId && cell.Coordinate.ProductNodeId == node.ProductNodeId)
                    .GroupBy(cell => cell.Coordinate.TimePeriodId)
                    .ToDictionary(
                        group => group.Key,
                        group => new GridPeriodCellDto(
                            group.ToDictionary(
                                cell => cell.Coordinate.MeasureId,
                                cell => new GridCellDto(
                                    cell.EffectiveValue,
                                    cell.GrowthFactor,
                                    IsEffectivelyLocked(cell.Coordinate, scenarioCells, productNodes, timePeriods),
                                    cell.CellKind == "calculated",
                                    cell.OverrideValue is not null,
                                    cell.RowVersion,
                                    cell.CellKind))));

                return new GridRowDto(
                    node.StoreId,
                    node.ProductNodeId,
                    node.Label,
                    node.Level,
                    node.Path,
                    node.IsLeaf,
                    node.NodeKind,
                    stores.TryGetValue(node.StoreId, out var storeMetadata) ? storeMetadata.StoreLabel : node.Path.FirstOrDefault() ?? $"Store {node.StoreId}",
                    stores.TryGetValue(node.StoreId, out storeMetadata) ? storeMetadata.ClusterLabel : "Unassigned Cluster",
                    stores.TryGetValue(node.StoreId, out storeMetadata) ? storeMetadata.RegionLabel : "Unassigned Region",
                    resolvedMetadata.LifecycleState,
                    resolvedMetadata.RampProfileCode,
                    resolvedMetadata.EffectiveFromTimePeriodId,
                    resolvedMetadata.EffectiveToTimePeriodId,
                    cells);
            })
            .ToList();
    }

    private static IEnumerable<long> GetAncestorProductNodeIds(ProductNode node, IReadOnlyDictionary<long, ProductNode> productNodes)
    {
        var currentParentId = node.ParentProductNodeId;
        while (currentParentId is long parentId && productNodes.TryGetValue(parentId, out var parentNode))
        {
            yield return parentId;
            currentParentId = parentNode.ParentProductNodeId;
        }
    }

    public async Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
            var normalizedLevel = request.Level.Trim().ToLowerInvariant();
            var nextProductNodeId = productNodes.Keys.DefaultIfEmpty(3000L).Max();
            var nextStoreId = productNodes.Values.Select(node => node.StoreId).DefaultIfEmpty(200L).Max();

            ProductNode node;
            switch (normalizedLevel)
            {
                case "store":
                {
                    var storeId = ++nextStoreId;
                    var productNodeId = ++nextProductNodeId;
                    node = new ProductNode(productNodeId, storeId, null, request.Label.Trim(), 0, new[] { request.Label.Trim() }, false, "store", "active", request.ClusterLabel is null && request.RegionLabel is null ? "new-store-ramp" : null, null, null);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await UpsertStoreMetadataAsync(connection, transaction, new StoreNodeMetadata(
                        storeId,
                        request.Label.Trim(),
                        request.ClusterLabel?.Trim() ?? "Unassigned Cluster",
                        request.RegionLabel?.Trim() ?? "Unassigned Region",
                        "active",
                        "new-store-ramp",
                        null,
                        null,
                        new string(request.Label.Trim().Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        "Active",
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        true), cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    if (request.CopyFromStoreId is not null)
                    {
                        nextProductNodeId = await CloneStoreHierarchyAndDataAsync(
                            connection,
                            transaction,
                            request.ScenarioVersionId,
                            request.CopyFromStoreId.Value,
                            node,
                            productNodes,
                            timePeriods,
                            cells,
                            nextProductNodeId,
                            cancellationToken);
                    }

                    break;
                }
                case "category":
                case "department":
                {
                    var parent = GetRequiredNode(productNodes, request.ParentProductNodeId, 0, "department");
                    node = new ProductNode(
                        ++nextProductNodeId,
                        parent.StoreId,
                        parent.ProductNodeId,
                        request.Label.Trim(),
                        1,
                        parent.Path.Append(request.Label.Trim()).ToArray(),
                        false,
                        "department",
                        "active",
                        null,
                        null,
                        null);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    await UpsertHierarchyDepartmentInternalAsync(connection, transaction, node.Label, cancellationToken);
                    break;
                }
                case "subcategory":
                case "class":
                {
                    var parent = GetRequiredNode(productNodes, request.ParentProductNodeId, 1, "class");
                    if (parent.IsLeaf)
                    {
                        parent = parent with { IsLeaf = false };
                        await UpdateProductNodeAsync(connection, transaction, parent, cancellationToken);
                    }

                    node = new ProductNode(
                        ++nextProductNodeId,
                        parent.StoreId,
                        parent.ProductNodeId,
                        request.Label.Trim(),
                        2,
                        parent.Path.Append(request.Label.Trim()).ToArray(),
                        false,
                        "class",
                        "active",
                        null,
                        null,
                        null);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    await UpsertHierarchyClassInternalAsync(connection, transaction, parent.Label, node.Label, cancellationToken);
                    break;
                }
                case "subclass":
                {
                    var parent = GetRequiredNode(productNodes, request.ParentProductNodeId, 2, "subclass");
                    if (parent.IsLeaf)
                    {
                        parent = parent with { IsLeaf = false };
                        await UpdateProductNodeAsync(connection, transaction, parent, cancellationToken);
                    }

                    node = new ProductNode(
                        ++nextProductNodeId,
                        parent.StoreId,
                        parent.ProductNodeId,
                        request.Label.Trim(),
                        3,
                        parent.Path.Append(request.Label.Trim()).ToArray(),
                        true,
                        "subclass",
                        "active",
                        null,
                        null,
                        null);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    var classNode = parent;
                    var departmentNode = productNodes[classNode.ParentProductNodeId!.Value];
                    await UpsertHierarchySubclassInternal(connection, transaction, departmentNode.Label, classNode.Label, node.Label, cancellationToken);
                    break;
                }
                default:
                    throw new InvalidOperationException($"Unsupported row level '{request.Level}'.");
            }

            await ClearPlanningCommandHistoryAsync(connection, transaction, request.ScenarioVersionId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return node;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return (await LoadStoreMetadataAsync(connection, null, cancellationToken)).Values
            .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            var stores = await LoadStoreMetadataAsync(connection, transaction, cancellationToken);
            var normalizedProfile = NormalizePersistedStoreProfile(storeProfile, stores);
            var existingStore = ResolveExistingStore(normalizedProfile, stores);

            StoreNodeMetadata persistedStore;
            if (existingStore is null)
            {
                var nextStoreId = productNodes.Values.Select(node => node.StoreId).DefaultIfEmpty(200L).Max() + 1;
                var nextProductNodeId = productNodes.Keys.DefaultIfEmpty(3000L).Max() + 1;
                var rootNode = new ProductNode(
                    nextProductNodeId,
                    nextStoreId,
                    null,
                    normalizedProfile.StoreLabel,
                    0,
                    [normalizedProfile.StoreLabel],
                    false,
                    "store",
                    normalizedProfile.LifecycleState,
                    normalizedProfile.RampProfileCode,
                    normalizedProfile.EffectiveFromTimePeriodId,
                    normalizedProfile.EffectiveToTimePeriodId);
                await InsertProductNodeAsync(connection, transaction, rootNode, cancellationToken);
                await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, rootNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                persistedStore = normalizedProfile with { StoreId = nextStoreId };
            }
            else
            {
                persistedStore = normalizedProfile with { StoreId = existingStore.StoreId };
                if (!string.Equals(existingStore.StoreLabel, persistedStore.StoreLabel, StringComparison.Ordinal))
                {
                    await RenameStoreHierarchyAsync(connection, transaction, productNodes.Values.Where(node => node.StoreId == existingStore.StoreId).ToList(), persistedStore.StoreLabel, cancellationToken);
                }
            }

            await UpsertStoreMetadataAsync(connection, transaction, persistedStore, cancellationToken);
            await UpsertStoreProfileOptionsForMetadataAsync(connection, transaction, persistedStore, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return persistedStore;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            var rootNode = productNodes.Values.SingleOrDefault(node => node.StoreId == storeId && node.Level == 0)
                ?? throw new InvalidOperationException($"Store {storeId} was not found.");

            var nodeIdsToDelete = productNodes.Values
                .Where(node => node.StoreId == storeId)
                .Select(node => node.ProductNodeId)
                .ToList();

            await DeletePlanningCellsForNodesAsync(connection, transaction, scenarioVersionId, nodeIdsToDelete, cancellationToken);
            await DeleteProductNodesAsync(connection, transaction, nodeIdsToDelete, cancellationToken);
            await DeleteStoreMetadataAsync(connection, transaction, storeId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var stores = await LoadStoreMetadataAsync(connection, transaction, cancellationToken);
            if (!stores.TryGetValue(storeId, out var store))
            {
                throw new InvalidOperationException($"Store {storeId} was not found.");
            }

            await UpsertStoreMetadataAsync(connection, transaction, store with
            {
                IsActive = false,
                LifecycleState = "inactive",
                Status = string.IsNullOrWhiteSpace(store.Status) ? "Inactive" : store.Status
            }, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadStoreProfileOptionsAsync(connection, null, cancellationToken);
    }

    public async Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertStoreProfileOptionInternalAsync(connection, transaction, fieldName, value, isActive, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await DeleteStoreProfileOptionInternalAsync(connection, transaction, fieldName, value, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var normalizedPageNumber = Math.Max(1, pageNumber);
        var normalizedPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (normalizedPageNumber - 1) * normalizedPageSize;

        await using var connection = await OpenConnectionAsync(cancellationToken);
        var totalCount = await CountProductProfilesAsync(connection, null, normalizedSearch, cancellationToken);
        var profiles = await LoadProductProfilesAsync(connection, null, normalizedSearch, normalizedPageSize, offset, cancellationToken);
        return (profiles, totalCount);
    }

    public async Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var normalized = NormalizeProductProfile(profile);
            await UpsertProductProfileInternalAsync(connection, transaction, normalized, cancellationToken);
            await UpsertProductProfileOptionSeedsAsync(connection, transaction, normalized, cancellationToken);
            await UpsertDerivedSubclassCatalogAsync(connection, transaction, normalized, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return normalized;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "delete from product_profiles where sku_variant = $skuVariant;";
                command.Parameters.AddWithValue("$skuVariant", skuVariant.Trim());
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await RefreshProductSubclassCatalogAsync(connection, transaction, cancellationToken);
            await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    update product_profiles
                    set is_active = 0,
                        active_flag = '0'
                    where sku_variant = $skuVariant;
                    """;
                command.Parameters.AddWithValue("$skuVariant", skuVariant.Trim());
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await RefreshProductSubclassCatalogAsync(connection, transaction, cancellationToken);
            await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadProductProfileOptionsAsync(connection, null, cancellationToken);
    }

    public async Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertOptionAsync(connection, transaction, "product_profile_options", fieldName, value, isActive, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await DeleteOptionAsync(connection, transaction, "product_profile_options", fieldName, value, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadProductHierarchyCatalogAsync(connection, null, cancellationToken);
    }

    public async Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadProductSubclassCatalogAsync(connection, null, cancellationToken);
    }

    public async Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertProductHierarchyCatalogInternalAsync(connection, transaction, NormalizeProductHierarchyRecord(record), cancellationToken);
            await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "delete from product_hierarchy_catalog where dpt_no = $dptNo and clss_no = $clssNo;";
                command.Parameters.AddWithValue("$dptNo", dptNo.Trim());
                command.Parameters.AddWithValue("$clssNo", clssNo.Trim());
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = "delete from product_profiles where dpt_no = $dptNo and clss_no = $clssNo;";
                command.Parameters.AddWithValue("$dptNo", dptNo.Trim());
                command.Parameters.AddWithValue("$clssNo", clssNo.Trim());
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await RefreshProductSubclassCatalogAsync(connection, transaction, cancellationToken);
            await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var tableName in new[] { "product_subclass_catalog", "product_hierarchy_catalog", "product_profile_options", "product_profiles" })
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $"delete from {tableName};";
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var hierarchyRow in hierarchyRows.Select(NormalizeProductHierarchyRecord))
            {
                await UpsertProductHierarchyCatalogInternalAsync(connection, transaction, hierarchyRow, cancellationToken);
            }

            foreach (var profile in profiles.Select(NormalizeProductProfile))
            {
                await UpsertProductProfileInternalAsync(connection, transaction, profile, cancellationToken);
            }

            await RefreshProductSubclassCatalogAsync(connection, transaction, cancellationToken);
            await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, 1, 2026, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task RebuildPlanningFromMasterDataAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await RebuildPlanningFromMasterDataInternalAsync(connection, transaction, scenarioVersionId, fiscalYear, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadHierarchyMappingsAsync(connection, null, cancellationToken);
    }

    public async Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertHierarchyDepartmentInternalAsync(connection, transaction, departmentLabel, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertHierarchyClassInternalAsync(connection, transaction, departmentLabel, classLabel, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertHierarchySubclassInternal(connection, transaction, departmentLabel, classLabel, subclassLabel, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        return productNodes.Values.FirstOrDefault(candidate =>
            candidate.Path.Length == path.Length &&
            candidate.Path.Zip(path, (left, right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase)).All(match => match));
    }

    public async Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            if (!productNodes.TryGetValue(productNodeId, out var targetNode))
            {
                throw new InvalidOperationException($"Product node {productNodeId} was not found.");
            }

            var nodeIdsToDelete = productNodes.Values
                .Where(node => IsAncestorOrSelf(productNodes, productNodeId, node.ProductNodeId))
                .Select(node => node.ProductNodeId)
                .ToList();

            var deletedCells = 0;
            await using (var deleteCellsCommand = connection.CreateCommand())
            {
                deleteCellsCommand.Transaction = transaction;
                deleteCellsCommand.CommandText = $"""
                    delete from planning_cells
                    where scenario_version_id = $scenarioVersionId
                      and product_node_id in ({string.Join(", ", nodeIdsToDelete)});
                    """;
                deleteCellsCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                deletedCells = await deleteCellsCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var deleteAuditCommand = connection.CreateCommand())
            {
                deleteAuditCommand.Transaction = transaction;
                deleteAuditCommand.CommandText = $"""
                    delete from audit_deltas
                    where scenario_version_id = $scenarioVersionId
                      and product_node_id in ({string.Join(", ", nodeIdsToDelete)});
                    """;
                deleteAuditCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                await deleteAuditCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var deleteNodesCommand = connection.CreateCommand())
            {
                deleteNodesCommand.Transaction = transaction;
                deleteNodesCommand.CommandText = $"""
                    delete from product_nodes
                    where product_node_id in ({string.Join(", ", nodeIdsToDelete)});
                    """;
                await deleteNodesCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await RebuildHierarchyMappingsAsync(connection, transaction, cancellationToken);
            await ClearPlanningCommandHistoryAsync(connection, transaction, scenarioVersionId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return deletedCells;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            if (!timePeriods.TryGetValue(yearTimePeriodId, out var yearNode) || !string.Equals(yearNode.Grain, "year", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Year time period {yearTimePeriodId} was not found.");
            }

            var timeIdsToDelete = timePeriods.Values
                .Where(period => IsAncestorOrSelf(timePeriods, yearTimePeriodId, period.TimePeriodId))
                .Select(period => period.TimePeriodId)
                .ToList();

            var deletedCells = 0;
            await using (var deleteCellsCommand = connection.CreateCommand())
            {
                deleteCellsCommand.Transaction = transaction;
                deleteCellsCommand.CommandText = $"""
                    delete from planning_cells
                    where scenario_version_id = $scenarioVersionId
                      and time_period_id in ({string.Join(", ", timeIdsToDelete)});
                    """;
                deleteCellsCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                deletedCells = await deleteCellsCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var deleteAuditCommand = connection.CreateCommand())
            {
                deleteAuditCommand.Transaction = transaction;
                deleteAuditCommand.CommandText = $"""
                    delete from audit_deltas
                    where scenario_version_id = $scenarioVersionId
                      and time_period_id in ({string.Join(", ", timeIdsToDelete)});
                    """;
                deleteAuditCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                await deleteAuditCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var deleteTimeCommand = connection.CreateCommand())
            {
                deleteTimeCommand.Transaction = transaction;
                deleteTimeCommand.CommandText = $"""
                    delete from time_periods
                    where time_period_id in ({string.Join(", ", timeIdsToDelete)});
                    """;
                await deleteTimeCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await ClearPlanningCommandHistoryAsync(connection, transaction, scenarioVersionId, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return deletedCells;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            var desiredPeriods = BuildYearPeriods(fiscalYear);
            var addedAny = false;
            foreach (var period in desiredPeriods.Values.OrderBy(period => period.SortOrder))
            {
                if (timePeriods.ContainsKey(period.TimePeriodId))
                {
                    continue;
                }

                addedAny = true;
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
                    insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
                    values ($timePeriodId, $parentTimePeriodId, $label, $grain, $sortOrder);
                    """;
                command.Parameters.AddWithValue("$timePeriodId", period.TimePeriodId);
                command.Parameters.AddWithValue("$parentTimePeriodId", (object?)period.ParentTimePeriodId ?? DBNull.Value);
                command.Parameters.AddWithValue("$label", period.Label);
                command.Parameters.AddWithValue("$grain", period.Grain);
                command.Parameters.AddWithValue("$sortOrder", period.SortOrder);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            if (addedAny)
            {
                var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
                var refreshedTimePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
                await EnsureSupportedMeasureCellsAsync(connection, transaction, productNodes.Values, refreshedTimePeriods.Values, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var tableName in new[]
                     {
                         "planning_command_cell_deltas",
                         "planning_command_batches",
                         "audit_deltas",
                         "audits",
                         "planning_cells",
                         "vendor_supply_profiles",
                         "seasonality_event_profiles",
                         "pricing_policies",
                         "inventory_profiles",
                         "product_subclass_catalog",
                         "product_hierarchy_catalog",
                         "product_profile_options",
                         "product_profiles",
                         "store_profile_options",
                         "hierarchy_subclasses_v2",
                         "hierarchy_classes_v2",
                         "hierarchy_departments_v2",
                         "store_metadata",
                         "hierarchy_subcategories",
                         "hierarchy_categories",
                         "product_nodes",
                         "time_periods"
                     })
            {
                await using var deleteCommand = connection.CreateCommand();
                deleteCommand.Transaction = transaction;
                deleteCommand.CommandText = $"delete from {tableName};";
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await SeedAsync(connection, transaction, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    create table if not exists product_nodes (
                        product_node_id integer primary key,
                        store_id integer not null,
                        parent_product_node_id integer null,
                        label text not null,
                        level integer not null,
                        path_json text not null,
                        is_leaf integer not null
                    );
                    create table if not exists time_periods (
                        time_period_id integer primary key,
                        parent_time_period_id integer null,
                        label text not null,
                        grain text not null,
                        sort_order integer not null
                    );
                    create table if not exists planning_cells (
                        scenario_version_id integer not null,
                        measure_id integer not null,
                        store_id integer not null,
                        product_node_id integer not null,
                        time_period_id integer not null,
                        input_value real null,
                        override_value real null,
                        is_system_generated_override integer not null,
                        derived_value real not null,
                        effective_value real not null,
                        growth_factor real not null default 1.0,
                        is_locked integer not null,
                        lock_reason text null,
                        locked_by text null,
                        row_version integer not null,
                        cell_kind text not null,
                        primary key (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
                    );
                    create table if not exists audits (
                        action_id integer primary key,
                        action_type text not null,
                        method text not null,
                        user_id text not null,
                        comment text null,
                        created_at text not null
                    );
                    create table if not exists audit_deltas (
                        audit_delta_id integer primary key autoincrement,
                        action_id integer not null,
                        scenario_version_id integer not null,
                        measure_id integer not null,
                        store_id integer not null,
                        product_node_id integer not null,
                        time_period_id integer not null,
                        old_value real not null,
                        new_value real not null,
                        was_locked integer not null,
                        change_kind text not null
                    );
                    create table if not exists planning_command_batches (
                        command_batch_id integer primary key,
                        scenario_version_id integer not null,
                        user_id text not null,
                        command_kind text not null,
                        command_scope_json text null,
                        is_undone integer not null default 0,
                        superseded_by_batch_id integer null,
                        created_at text not null,
                        undone_at text null
                    );
                    create table if not exists planning_command_cell_deltas (
                        command_delta_id integer primary key autoincrement,
                        command_batch_id integer not null,
                        scenario_version_id integer not null,
                        measure_id integer not null,
                        store_id integer not null,
                        product_node_id integer not null,
                        time_period_id integer not null,
                        old_input_value real null,
                        new_input_value real null,
                        old_override_value real null,
                        new_override_value real null,
                        old_is_system_generated_override integer not null default 0,
                        new_is_system_generated_override integer not null default 0,
                        old_derived_value real not null,
                        new_derived_value real not null,
                        old_effective_value real not null,
                        new_effective_value real not null,
                        old_growth_factor real not null default 1.0,
                        new_growth_factor real not null default 1.0,
                        old_is_locked integer not null default 0,
                        new_is_locked integer not null default 0,
                        old_lock_reason text null,
                        new_lock_reason text null,
                        old_locked_by text null,
                        new_locked_by text null,
                        old_row_version integer not null,
                        new_row_version integer not null,
                        old_cell_kind text not null,
                        new_cell_kind text not null,
                        change_kind text not null
                    );
                    create table if not exists hierarchy_categories (
                        category_label text primary key
                    );
                    create table if not exists hierarchy_subcategories (
                        category_label text not null,
                        subcategory_label text not null,
                        primary key (category_label, subcategory_label)
                    );
                    create table if not exists store_metadata (
                        store_id integer primary key,
                        store_label text not null,
                        cluster_label text not null,
                        region_label text not null,
                        lifecycle_state text not null default 'active',
                        ramp_profile_code text null,
                        effective_from_time_period_id integer null,
                        effective_to_time_period_id integer null,
                        store_code text null,
                        state text null,
                        latitude real null,
                        longitude real null,
                        opening_date text null,
                        sssg text null,
                        sales_type text null,
                        status text null,
                        storey text null,
                        building_status text null,
                        gta real null,
                        nta real null,
                        rsom text null,
                        dm text null,
                        rental real null,
                        store_cluster_role text null,
                        store_capacity_sqft real null,
                        store_format_tier text null,
                        catchment_type text null,
                        demographic_segment text null,
                        climate_zone text null,
                        fulfilment_enabled integer not null default 0,
                        online_fulfilment_node integer not null default 0,
                        store_opening_season text null,
                        store_closure_date text null,
                        refurbishment_date text null,
                        store_priority text null,
                        is_active integer not null default 1
                    );
                    create table if not exists store_profile_options (
                        field_name text not null,
                        option_value text not null,
                        is_active integer not null default 1,
                        primary key (field_name, option_value)
                    );
                    create table if not exists product_profiles (
                        sku_variant text primary key,
                        description text not null,
                        description2 text null,
                        price real not null,
                        cost real not null,
                        dpt_no text not null,
                        clss_no text not null,
                        brand_no text null,
                        department text not null,
                        class text not null,
                        brand text null,
                        rev_department text null,
                        rev_class text null,
                        subclass text not null,
                        prod_group text null,
                        prod_type text null,
                        active_flag text null,
                        order_flag text null,
                        brand_type text null,
                        launch_month text null,
                        gender text null,
                        size text null,
                        collection text null,
                        promo text null,
                        ramadhan_promo text null,
                        supplier text null,
                        lifecycle_stage text null,
                        age_stage text null,
                        gender_target text null,
                        material text null,
                        pack_size text null,
                        size_range text null,
                        colour_family text null,
                        kvi_flag integer not null default 0,
                        markdown_eligible integer not null default 1,
                        markdown_floor_price real null,
                        minimum_margin_pct real null,
                        price_ladder_group text null,
                        good_better_best_tier text null,
                        season_code text null,
                        event_code text null,
                        launch_date text null,
                        end_of_life_date text null,
                        substitute_group text null,
                        companion_group text null,
                        replenishment_type text null,
                        lead_time_days integer null,
                        moq integer null,
                        case_pack integer null,
                        starting_inventory real null,
                        projected_stock_on_hand real null,
                        sell_through_target_pct real null,
                        weeks_of_cover_target real null,
                        is_active integer not null default 1
                    );
                    create table if not exists product_profile_options (
                        field_name text not null,
                        option_value text not null,
                        is_active integer not null default 1,
                        primary key (field_name, option_value)
                    );
                    create table if not exists product_hierarchy_catalog (
                        dpt_no text not null,
                        clss_no text not null,
                        department text not null,
                        class text not null,
                        prod_group text not null,
                        is_active integer not null default 1,
                        primary key (dpt_no, clss_no)
                    );
                    create table if not exists product_subclass_catalog (
                        department text not null,
                        class text not null,
                        subclass text not null,
                        is_active integer not null default 1,
                        primary key (department, class, subclass)
                    );
                    create table if not exists hierarchy_departments_v2 (
                        department_label text primary key,
                        lifecycle_state text not null default 'active',
                        ramp_profile_code text null,
                        effective_from_time_period_id integer null,
                        effective_to_time_period_id integer null
                    );
                    create table if not exists hierarchy_classes_v2 (
                        department_label text not null,
                        class_label text not null,
                        lifecycle_state text not null default 'active',
                        ramp_profile_code text null,
                        effective_from_time_period_id integer null,
                        effective_to_time_period_id integer null,
                        primary key (department_label, class_label)
                    );
                    create table if not exists hierarchy_subclasses_v2 (
                        department_label text not null,
                        class_label text not null,
                        subclass_label text not null,
                        lifecycle_state text not null default 'active',
                        ramp_profile_code text null,
                        effective_from_time_period_id integer null,
                        effective_to_time_period_id integer null,
                        primary key (department_label, class_label, subclass_label)
                    );
                    create index if not exists idx_planning_cells_scenario_measure on planning_cells (scenario_version_id, measure_id);
                    create index if not exists idx_audit_deltas_lookup on audit_deltas (scenario_version_id, measure_id, store_id, product_node_id);
                    create index if not exists idx_command_batches_lookup on planning_command_batches (scenario_version_id, user_id, command_batch_id desc);
                    create index if not exists idx_command_redo_lookup on planning_command_batches (scenario_version_id, user_id, is_undone, undone_at desc);
                    create index if not exists idx_command_deltas_lookup on planning_command_cell_deltas (command_batch_id, scenario_version_id, measure_id, store_id, product_node_id, time_period_id);
                    """;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await EnsureAdditionalMasterDataTablesAsync(connection, transaction, cancellationToken);

            await using (var countCommand = connection.CreateCommand())
            {
                countCommand.Transaction = transaction;
                countCommand.CommandText = "select count(*) from product_nodes;";
                var existingCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken));
                var needsSeed = existingCount == 0;
                if (existingCount == 0)
                {
                    await SeedAsync(connection, transaction, cancellationToken);
                }

                await EnsureSupportedTimePeriodsAsync(connection, transaction, cancellationToken);
                await EnsureGrowthFactorColumnAsync(connection, transaction, cancellationToken);
                await EnsureStoreProfileColumnsAsync(connection, transaction, cancellationToken);
                await EnsureProductProfileColumnsAsync(connection, transaction, cancellationToken);

                if (needsSeed)
                {
                    var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
                    var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
                    await EnsureSupportedMeasureCellsAsync(connection, transaction, productNodes.Values, timePeriods.Values, cancellationToken);
                    await EnsureQuantitySeedAsync(connection, transaction, productNodes, timePeriods, cancellationToken);
                    await EnsureAspSeedAsync(connection, transaction, productNodes, timePeriods, cancellationToken);
                    await EnsureExtendedMeasureSeedAsync(connection, transaction, productNodes, timePeriods, cancellationToken);
                    await EnsureStoreProfileOptionSeedAsync(connection, transaction, cancellationToken);
                    await EnsureProductProfileSeedAsync(connection, transaction, cancellationToken);
                    await EnsureProductProfileOptionSeedAsync(connection, transaction, cancellationToken);
                }
            }

            await transaction.CommitAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async Task<bool> HasInitializedSchemaAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select count(*)
            from sqlite_master
            where type = 'table'
              and name in (
                  'product_nodes',
                  'time_periods',
                  'planning_cells',
                  'store_metadata',
                  'product_profiles',
                  'product_hierarchy_catalog',
                  'product_subclass_catalog');
            """;
        var existingCount = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        return existingCount == 7;
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static async Task<Dictionary<long, ProductNode>> LoadProductNodesAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, ProductNode>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select product_node_id, store_id, parent_product_node_id, label, level, path_json, is_leaf
            from product_nodes;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var node = new ProductNode(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.GetString(3),
                reader.GetInt32(4),
                JsonSerializer.Deserialize<string[]>(reader.GetString(5)) ?? Array.Empty<string>(),
                reader.GetInt64(6) == 1,
                DeriveNodeKind(reader.GetInt32(4), reader.GetInt64(6) == 1),
                "active",
                null,
                null,
                null);
            result[node.ProductNodeId] = node;
        }

        return result;
    }

    private static async Task<Dictionary<long, StoreNodeMetadata>> LoadStoreMetadataAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, StoreNodeMetadata>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select store_id,
                   store_label,
                   cluster_label,
                   region_label,
                   lifecycle_state,
                   ramp_profile_code,
                   effective_from_time_period_id,
                   effective_to_time_period_id,
                   store_code,
                   state,
                   latitude,
                   longitude,
                   opening_date,
                   sssg,
                   sales_type,
                   status,
                   storey,
                   building_status,
                   gta,
                   nta,
                   rsom,
                   dm,
                   rental,
                   store_cluster_role,
                   store_capacity_sqft,
                   store_format_tier,
                   catchment_type,
                   demographic_segment,
                   climate_zone,
                   fulfilment_enabled,
                   online_fulfilment_node,
                   store_opening_season,
                   store_closure_date,
                   refurbishment_date,
                   store_priority,
                   is_active
            from store_metadata;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var metadata = new StoreNodeMetadata(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : Convert.ToDecimal(reader.GetDouble(10), CultureInfo.InvariantCulture),
                reader.IsDBNull(11) ? null : Convert.ToDecimal(reader.GetDouble(11), CultureInfo.InvariantCulture),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14),
                reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetString(17),
                reader.IsDBNull(18) ? null : Convert.ToDecimal(reader.GetDouble(18), CultureInfo.InvariantCulture),
                reader.IsDBNull(19) ? null : Convert.ToDecimal(reader.GetDouble(19), CultureInfo.InvariantCulture),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21),
                reader.IsDBNull(22) ? null : Convert.ToDecimal(reader.GetDouble(22), CultureInfo.InvariantCulture),
                !reader.IsDBNull(35) && reader.GetInt64(35) == 1,
                reader.IsDBNull(23) ? null : reader.GetString(23),
                reader.IsDBNull(24) ? null : Convert.ToDecimal(reader.GetDouble(24), CultureInfo.InvariantCulture),
                reader.IsDBNull(25) ? null : reader.GetString(25),
                reader.IsDBNull(26) ? null : reader.GetString(26),
                reader.IsDBNull(27) ? null : reader.GetString(27),
                reader.IsDBNull(28) ? null : reader.GetString(28),
                !reader.IsDBNull(29) && reader.GetInt64(29) == 1,
                !reader.IsDBNull(30) && reader.GetInt64(30) == 1,
                reader.IsDBNull(31) ? null : reader.GetString(31),
                reader.IsDBNull(32) ? null : reader.GetString(32),
                reader.IsDBNull(33) ? null : reader.GetString(33),
                reader.IsDBNull(34) ? null : reader.GetString(34));
            result[metadata.StoreId] = metadata;
        }

        return result;
    }

    private static async Task<Dictionary<long, TimePeriodNode>> LoadTimePeriodsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, TimePeriodNode>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select time_period_id, parent_time_period_id, label, grain, sort_order
            from time_periods;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var node = new TimePeriodNode(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt32(4));
            result[node.TimePeriodId] = node;
        }

        return result;
    }

    private static async Task<List<PlanningCell>> LoadCellsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new List<PlanningCell>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   input_value,
                   override_value,
                   is_system_generated_override,
                   derived_value,
                   effective_value,
                   growth_factor,
                   is_locked,
                   lock_reason,
                   locked_by,
                   row_version,
                   cell_kind
            from planning_cells;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadPlanningCell(reader));
        }

        return result;
    }

    private static async Task<List<PlanningCell>> LoadScenarioCellsForNodesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        long scenarioVersionId,
        IReadOnlyList<long> productNodeIds,
        CancellationToken cancellationToken)
    {
        if (productNodeIds.Count == 0)
        {
            return [];
        }

        var parameterNames = productNodeIds.Select((_, index) => $"$productNodeId{index}").ToArray();
        var result = new List<PlanningCell>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   input_value,
                   override_value,
                   is_system_generated_override,
                   derived_value,
                   effective_value,
                   growth_factor,
                   is_locked,
                   lock_reason,
                   locked_by,
                   row_version,
                   cell_kind
            from planning_cells
            where scenario_version_id = $scenarioVersionId
              and product_node_id in ({string.Join(", ", parameterNames)});
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        for (var index = 0; index < productNodeIds.Count; index += 1)
        {
            command.Parameters.AddWithValue(parameterNames[index], productNodeIds[index]);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadPlanningCell(reader));
        }

        return result;
    }

    private static PlanningCell ReadPlanningCell(SqliteDataReader reader)
    {
        return new PlanningCell
        {
            Coordinate = new PlanningCellCoordinate(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4)),
            InputValue = reader.IsDBNull(5) ? null : ReadDecimal(reader, 5),
            OverrideValue = reader.IsDBNull(6) ? null : ReadDecimal(reader, 6),
            IsSystemGeneratedOverride = reader.GetInt64(7) == 1,
            DerivedValue = ReadDecimal(reader, 8),
            EffectiveValue = ReadDecimal(reader, 9),
            GrowthFactor = reader.IsDBNull(10) ? 1.0m : ReadDecimal(reader, 10),
            IsLocked = reader.GetInt64(11) == 1,
            LockReason = reader.IsDBNull(12) ? null : reader.GetString(12),
            LockedBy = reader.IsDBNull(13) ? null : reader.GetString(13),
            RowVersion = reader.GetInt64(14),
            CellKind = reader.GetString(15)
        };
    }

    private static decimal ReadDecimal(SqliteDataReader reader, int ordinal)
    {
        return Convert.ToDecimal(reader.GetValue(ordinal));
    }

    private static async Task UpsertCellAsync(SqliteConnection connection, SqliteTransaction transaction, PlanningCell cell, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into planning_cells (
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                input_value,
                override_value,
                is_system_generated_override,
                derived_value,
                effective_value,
                growth_factor,
                is_locked,
                lock_reason,
                locked_by,
                row_version,
                cell_kind)
            values (
                $scenarioVersionId,
                $measureId,
                $storeId,
                $productNodeId,
                $timePeriodId,
                $inputValue,
                $overrideValue,
                $isSystemGeneratedOverride,
                $derivedValue,
                $effectiveValue,
                $growthFactor,
                $isLocked,
                $lockReason,
                $lockedBy,
                $rowVersion,
                $cellKind)
            on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
            do update set
                input_value = excluded.input_value,
                override_value = excluded.override_value,
                is_system_generated_override = excluded.is_system_generated_override,
                derived_value = excluded.derived_value,
                effective_value = excluded.effective_value,
                growth_factor = excluded.growth_factor,
                is_locked = excluded.is_locked,
                lock_reason = excluded.lock_reason,
                locked_by = excluded.locked_by,
                row_version = excluded.row_version,
                cell_kind = excluded.cell_kind;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", cell.Coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("$measureId", cell.Coordinate.MeasureId);
        command.Parameters.AddWithValue("$storeId", cell.Coordinate.StoreId);
        command.Parameters.AddWithValue("$productNodeId", cell.Coordinate.ProductNodeId);
        command.Parameters.AddWithValue("$timePeriodId", cell.Coordinate.TimePeriodId);
        command.Parameters.AddWithValue("$inputValue", (object?)cell.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$overrideValue", (object?)cell.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$isSystemGeneratedOverride", cell.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("$derivedValue", cell.DerivedValue);
        command.Parameters.AddWithValue("$effectiveValue", cell.EffectiveValue);
        command.Parameters.AddWithValue("$growthFactor", cell.GrowthFactor);
        command.Parameters.AddWithValue("$isLocked", cell.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("$lockReason", (object?)cell.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("$lockedBy", (object?)cell.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("$rowVersion", cell.RowVersion);
        command.Parameters.AddWithValue("$cellKind", cell.CellKind);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertProductNodeAsync(SqliteConnection connection, SqliteTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into product_nodes (
                product_node_id,
                store_id,
                parent_product_node_id,
                label,
                level,
                path_json,
                is_leaf)
            values (
                $productNodeId,
                $storeId,
                $parentProductNodeId,
                $label,
                $level,
                $pathJson,
                $isLeaf);
            """;
        command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("$storeId", node.StoreId);
        command.Parameters.AddWithValue("$parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("$label", node.Label);
        command.Parameters.AddWithValue("$level", node.Level);
        command.Parameters.AddWithValue("$pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("$isLeaf", node.IsLeaf ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateProductNodeAsync(SqliteConnection connection, SqliteTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            update product_nodes
            set store_id = $storeId,
                parent_product_node_id = $parentProductNodeId,
                label = $label,
                level = $level,
                path_json = $pathJson,
                is_leaf = $isLeaf
            where product_node_id = $productNodeId;
            """;
        command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("$storeId", node.StoreId);
        command.Parameters.AddWithValue("$parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("$label", node.Label);
        command.Parameters.AddWithValue("$level", node.Level);
        command.Parameters.AddWithValue("$pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("$isLeaf", node.IsLeaf ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertStoreMetadataAsync(SqliteConnection connection, SqliteTransaction transaction, StoreNodeMetadata metadata, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into store_metadata (
                store_id,
                store_label,
                cluster_label,
                region_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id,
                store_code,
                state,
                latitude,
                longitude,
                opening_date,
                sssg,
                sales_type,
                status,
                storey,
                building_status,
                gta,
                nta,
                rsom,
                dm,
                rental,
                store_cluster_role,
                store_capacity_sqft,
                store_format_tier,
                catchment_type,
                demographic_segment,
                climate_zone,
                fulfilment_enabled,
                online_fulfilment_node,
                store_opening_season,
                store_closure_date,
                refurbishment_date,
                store_priority,
                is_active)
            values (
                $storeId,
                $storeLabel,
                $clusterLabel,
                $regionLabel,
                $lifecycleState,
                $rampProfileCode,
                $effectiveFromTimePeriodId,
                $effectiveToTimePeriodId,
                $storeCode,
                $state,
                $latitude,
                $longitude,
                $openingDate,
                $sssg,
                $salesType,
                $status,
                $storey,
                $buildingStatus,
                $gta,
                $nta,
                $rsom,
                $dm,
                $rental,
                $storeClusterRole,
                $storeCapacitySqFt,
                $storeFormatTier,
                $catchmentType,
                $demographicSegment,
                $climateZone,
                $fulfilmentEnabled,
                $onlineFulfilmentNode,
                $storeOpeningSeason,
                $storeClosureDate,
                $refurbishmentDate,
                $storePriority,
                $isActive)
            on conflict (store_id)
            do update set
                store_label = excluded.store_label,
                cluster_label = excluded.cluster_label,
                region_label = excluded.region_label,
                lifecycle_state = excluded.lifecycle_state,
                ramp_profile_code = excluded.ramp_profile_code,
                effective_from_time_period_id = excluded.effective_from_time_period_id,
                effective_to_time_period_id = excluded.effective_to_time_period_id,
                store_code = excluded.store_code,
                state = excluded.state,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                opening_date = excluded.opening_date,
                sssg = excluded.sssg,
                sales_type = excluded.sales_type,
                status = excluded.status,
                storey = excluded.storey,
                building_status = excluded.building_status,
                gta = excluded.gta,
                nta = excluded.nta,
                rsom = excluded.rsom,
                dm = excluded.dm,
                rental = excluded.rental,
                store_cluster_role = excluded.store_cluster_role,
                store_capacity_sqft = excluded.store_capacity_sqft,
                store_format_tier = excluded.store_format_tier,
                catchment_type = excluded.catchment_type,
                demographic_segment = excluded.demographic_segment,
                climate_zone = excluded.climate_zone,
                fulfilment_enabled = excluded.fulfilment_enabled,
                online_fulfilment_node = excluded.online_fulfilment_node,
                store_opening_season = excluded.store_opening_season,
                store_closure_date = excluded.store_closure_date,
                refurbishment_date = excluded.refurbishment_date,
                store_priority = excluded.store_priority,
                is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$storeId", metadata.StoreId);
        command.Parameters.AddWithValue("$storeLabel", metadata.StoreLabel);
        command.Parameters.AddWithValue("$clusterLabel", metadata.ClusterLabel);
        command.Parameters.AddWithValue("$regionLabel", metadata.RegionLabel);
        command.Parameters.AddWithValue("$lifecycleState", metadata.LifecycleState);
        command.Parameters.AddWithValue("$rampProfileCode", (object?)metadata.RampProfileCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$effectiveFromTimePeriodId", (object?)metadata.EffectiveFromTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("$effectiveToTimePeriodId", (object?)metadata.EffectiveToTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("$storeCode", (object?)metadata.StoreCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$state", (object?)metadata.State ?? DBNull.Value);
        command.Parameters.AddWithValue("$latitude", (object?)metadata.Latitude ?? DBNull.Value);
        command.Parameters.AddWithValue("$longitude", (object?)metadata.Longitude ?? DBNull.Value);
        command.Parameters.AddWithValue("$openingDate", (object?)metadata.OpeningDate ?? DBNull.Value);
        command.Parameters.AddWithValue("$sssg", (object?)metadata.Sssg ?? DBNull.Value);
        command.Parameters.AddWithValue("$salesType", (object?)metadata.SalesType ?? DBNull.Value);
        command.Parameters.AddWithValue("$status", (object?)metadata.Status ?? DBNull.Value);
        command.Parameters.AddWithValue("$storey", (object?)metadata.Storey ?? DBNull.Value);
        command.Parameters.AddWithValue("$buildingStatus", (object?)metadata.BuildingStatus ?? DBNull.Value);
        command.Parameters.AddWithValue("$gta", (object?)metadata.Gta ?? DBNull.Value);
        command.Parameters.AddWithValue("$nta", (object?)metadata.Nta ?? DBNull.Value);
        command.Parameters.AddWithValue("$rsom", (object?)metadata.Rsom ?? DBNull.Value);
        command.Parameters.AddWithValue("$dm", (object?)metadata.Dm ?? DBNull.Value);
        command.Parameters.AddWithValue("$rental", (object?)metadata.Rental ?? DBNull.Value);
        command.Parameters.AddWithValue("$storeClusterRole", (object?)metadata.StoreClusterRole ?? DBNull.Value);
        command.Parameters.AddWithValue("$storeCapacitySqFt", (object?)metadata.StoreCapacitySqFt ?? DBNull.Value);
        command.Parameters.AddWithValue("$storeFormatTier", (object?)metadata.StoreFormatTier ?? DBNull.Value);
        command.Parameters.AddWithValue("$catchmentType", (object?)metadata.CatchmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("$demographicSegment", (object?)metadata.DemographicSegment ?? DBNull.Value);
        command.Parameters.AddWithValue("$climateZone", (object?)metadata.ClimateZone ?? DBNull.Value);
        command.Parameters.AddWithValue("$fulfilmentEnabled", metadata.FulfilmentEnabled ? 1 : 0);
        command.Parameters.AddWithValue("$onlineFulfilmentNode", metadata.OnlineFulfilmentNode ? 1 : 0);
        command.Parameters.AddWithValue("$storeOpeningSeason", (object?)metadata.StoreOpeningSeason ?? DBNull.Value);
        command.Parameters.AddWithValue("$storeClosureDate", (object?)metadata.StoreClosureDate ?? DBNull.Value);
        command.Parameters.AddWithValue("$refurbishmentDate", (object?)metadata.RefurbishmentDate ?? DBNull.Value);
        command.Parameters.AddWithValue("$storePriority", (object?)metadata.StorePriority ?? DBNull.Value);
        command.Parameters.AddWithValue("$isActive", metadata.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static StoreNodeMetadata? ResolveExistingStore(StoreNodeMetadata profile, IReadOnlyDictionary<long, StoreNodeMetadata> stores)
    {
        if (profile.StoreId > 0 && stores.TryGetValue(profile.StoreId, out var byId))
        {
            return byId;
        }

        var storeCode = profile.StoreCode?.Trim();
        if (!string.IsNullOrWhiteSpace(storeCode))
        {
            return stores.Values.FirstOrDefault(candidate =>
                string.Equals(candidate.StoreCode, storeCode, StringComparison.OrdinalIgnoreCase));
        }

        return stores.Values.FirstOrDefault(candidate =>
            string.Equals(candidate.StoreLabel, profile.StoreLabel, StringComparison.OrdinalIgnoreCase));
    }

    private static StoreNodeMetadata NormalizePersistedStoreProfile(
        StoreNodeMetadata profile,
        IReadOnlyDictionary<long, StoreNodeMetadata> stores)
    {
        var resolvedLabel = string.IsNullOrWhiteSpace(profile.StoreLabel) ? profile.StoreCode?.Trim() : profile.StoreLabel.Trim();
        if (string.IsNullOrWhiteSpace(resolvedLabel))
        {
            throw new InvalidOperationException("BranchName is required.");
        }

        var resolvedCode = string.IsNullOrWhiteSpace(profile.StoreCode)
            ? BuildGeneratedStoreCode(resolvedLabel, stores)
            : profile.StoreCode.Trim().ToUpperInvariant();

        if (stores.Values.Any(candidate => candidate.StoreId != profile.StoreId && string.Equals(candidate.StoreCode, resolvedCode, StringComparison.OrdinalIgnoreCase)))
        {
            throw new InvalidOperationException($"Store code '{resolvedCode}' already exists.");
        }

        return profile with
        {
            StoreLabel = resolvedLabel,
            StoreCode = resolvedCode,
            ClusterLabel = string.IsNullOrWhiteSpace(profile.ClusterLabel) ? "Unassigned Cluster" : profile.ClusterLabel.Trim(),
            RegionLabel = string.IsNullOrWhiteSpace(profile.RegionLabel) ? "Unassigned Region" : profile.RegionLabel.Trim(),
            LifecycleState = string.IsNullOrWhiteSpace(profile.LifecycleState) ? (profile.IsActive ? "active" : "inactive") : profile.LifecycleState.Trim(),
            RampProfileCode = string.IsNullOrWhiteSpace(profile.RampProfileCode) ? null : profile.RampProfileCode.Trim(),
            State = string.IsNullOrWhiteSpace(profile.State) ? null : profile.State.Trim(),
            OpeningDate = string.IsNullOrWhiteSpace(profile.OpeningDate) ? null : profile.OpeningDate.Trim(),
            Sssg = string.IsNullOrWhiteSpace(profile.Sssg) ? null : profile.Sssg.Trim(),
            SalesType = string.IsNullOrWhiteSpace(profile.SalesType) ? null : profile.SalesType.Trim(),
            Status = string.IsNullOrWhiteSpace(profile.Status) ? null : profile.Status.Trim(),
            Storey = string.IsNullOrWhiteSpace(profile.Storey) ? null : profile.Storey.Trim(),
            BuildingStatus = string.IsNullOrWhiteSpace(profile.BuildingStatus) ? null : profile.BuildingStatus.Trim(),
            Rsom = string.IsNullOrWhiteSpace(profile.Rsom) ? null : profile.Rsom.Trim(),
            Dm = string.IsNullOrWhiteSpace(profile.Dm) ? null : profile.Dm.Trim(),
            StoreClusterRole = string.IsNullOrWhiteSpace(profile.StoreClusterRole) ? null : profile.StoreClusterRole.Trim(),
            StoreFormatTier = string.IsNullOrWhiteSpace(profile.StoreFormatTier) ? null : profile.StoreFormatTier.Trim(),
            CatchmentType = string.IsNullOrWhiteSpace(profile.CatchmentType) ? null : profile.CatchmentType.Trim(),
            DemographicSegment = string.IsNullOrWhiteSpace(profile.DemographicSegment) ? null : profile.DemographicSegment.Trim(),
            ClimateZone = string.IsNullOrWhiteSpace(profile.ClimateZone) ? null : profile.ClimateZone.Trim(),
            StoreOpeningSeason = string.IsNullOrWhiteSpace(profile.StoreOpeningSeason) ? null : profile.StoreOpeningSeason.Trim(),
            StoreClosureDate = string.IsNullOrWhiteSpace(profile.StoreClosureDate) ? null : profile.StoreClosureDate.Trim(),
            RefurbishmentDate = string.IsNullOrWhiteSpace(profile.RefurbishmentDate) ? null : profile.RefurbishmentDate.Trim(),
            StorePriority = string.IsNullOrWhiteSpace(profile.StorePriority) ? null : profile.StorePriority.Trim()
        };
    }

    private static string BuildGeneratedStoreCode(string storeLabel, IReadOnlyDictionary<long, StoreNodeMetadata> stores)
    {
        var baseCode = new string(storeLabel.Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(baseCode))
        {
            baseCode = "STORE";
        }

        var candidate = baseCode[..Math.Min(baseCode.Length, 12)];
        var suffix = 1;
        while (stores.Values.Any(store => string.Equals(store.StoreCode, candidate, StringComparison.OrdinalIgnoreCase)))
        {
            candidate = $"{baseCode[..Math.Min(baseCode.Length, 9)]}{suffix:000}";
            suffix += 1;
        }

        return candidate;
    }

    private static async Task RenameStoreHierarchyAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyList<ProductNode> storeNodes,
        string newStoreLabel,
        CancellationToken cancellationToken)
    {
        foreach (var node in storeNodes.OrderBy(node => node.Level))
        {
            var updatedPath = node.Path.Length == 0
                ? new[] { newStoreLabel }
                : (new[] { newStoreLabel }).Concat(node.Path.Skip(1)).ToArray();
            var updatedNode = node with
            {
                Label = node.Level == 0 ? newStoreLabel : node.Label,
                Path = updatedPath
            };
            await UpdateProductNodeAsync(connection, transaction, updatedNode, cancellationToken);
        }
    }

    private static async Task DeletePlanningCellsForNodesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        IReadOnlyList<long> nodeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (nodeIdsToDelete.Count == 0)
        {
            return;
        }

        var parameterNames = nodeIdsToDelete.Select((_, index) => $"$nodeId{index}").ToArray();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            delete from planning_cells
            where scenario_version_id = $scenarioVersionId
              and product_node_id in ({string.Join(", ", parameterNames)});
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        for (var index = 0; index < nodeIdsToDelete.Count; index += 1)
        {
            command.Parameters.AddWithValue(parameterNames[index], nodeIdsToDelete[index]);
        }
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteProductNodesAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyList<long> nodeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (nodeIdsToDelete.Count == 0)
        {
            return;
        }

        var parameterNames = nodeIdsToDelete.Select((_, index) => $"$nodeId{index}").ToArray();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            delete from product_nodes
            where product_node_id in ({string.Join(", ", parameterNames)});
            """;
        for (var index = 0; index < nodeIdsToDelete.Count; index += 1)
        {
            command.Parameters.AddWithValue(parameterNames[index], nodeIdsToDelete[index]);
        }
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteStoreMetadataAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long storeId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "delete from store_metadata where store_id = $storeId;";
        command.Parameters.AddWithValue("$storeId", storeId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InitializeCellsForNodeAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        ProductNode node,
        IEnumerable<long> measureIds,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        foreach (var measureId in measureIds)
        {
            foreach (var period in timePeriods)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
                    insert into planning_cells (
                        scenario_version_id,
                        measure_id,
                        store_id,
                        product_node_id,
                        time_period_id,
                        input_value,
                        override_value,
                        is_system_generated_override,
                        derived_value,
                        effective_value,
                        growth_factor,
                        is_locked,
                        lock_reason,
                        locked_by,
                        row_version,
                        cell_kind)
                    values (
                        $scenarioVersionId,
                        $measureId,
                        $storeId,
                        $productNodeId,
                        $timePeriodId,
                        null,
                        null,
                        0,
                        0,
                        0,
                        1.0,
                        0,
                        null,
                        null,
                        1,
                        $cellKind)
                    on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id) do nothing;
                    """;
                command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                command.Parameters.AddWithValue("$measureId", measureId);
                command.Parameters.AddWithValue("$storeId", node.StoreId);
                command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
                command.Parameters.AddWithValue("$timePeriodId", period.TimePeriodId);
                command.Parameters.AddWithValue("$cellKind", node.IsLeaf && period.Grain == "month" ? "leaf" : "calculated");
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    private static async Task EnsureSupportedMeasureCellsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IEnumerable<ProductNode> productNodes,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        foreach (var productNode in productNodes)
        {
            await InitializeCellsForNodeAsync(connection, transaction, 1, productNode, SupportedMeasureIds, timePeriods, cancellationToken);
        }
    }

    private static async Task EnsureSupportedTimePeriodsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        var existingPeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
        foreach (var period in BuildTimePeriods().Values.OrderBy(period => period.SortOrder))
        {
            if (existingPeriods.ContainsKey(period.TimePeriodId))
            {
                continue;
            }

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
                values ($timePeriodId, $parentTimePeriodId, $label, $grain, $sortOrder);
                """;
            command.Parameters.AddWithValue("$timePeriodId", period.TimePeriodId);
            command.Parameters.AddWithValue("$parentTimePeriodId", (object?)period.ParentTimePeriodId ?? DBNull.Value);
            command.Parameters.AddWithValue("$label", period.Label);
            command.Parameters.AddWithValue("$grain", period.Grain);
            command.Parameters.AddWithValue("$sortOrder", period.SortOrder);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task EnsureGrowthFactorColumnAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "alter table planning_cells add column growth_factor real not null default 1.0;";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
        {
        }
    }

    private static async Task EnsureStoreProfileColumnsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        var statements = new[]
        {
            "alter table store_metadata add column store_code text null;",
            "alter table store_metadata add column state text null;",
            "alter table store_metadata add column latitude real null;",
            "alter table store_metadata add column longitude real null;",
            "alter table store_metadata add column opening_date text null;",
            "alter table store_metadata add column sssg text null;",
            "alter table store_metadata add column sales_type text null;",
            "alter table store_metadata add column status text null;",
            "alter table store_metadata add column storey text null;",
            "alter table store_metadata add column building_status text null;",
            "alter table store_metadata add column gta real null;",
            "alter table store_metadata add column nta real null;",
            "alter table store_metadata add column rsom text null;",
            "alter table store_metadata add column dm text null;",
            "alter table store_metadata add column rental real null;",
            "alter table store_metadata add column store_cluster_role text null;",
            "alter table store_metadata add column store_capacity_sqft real null;",
            "alter table store_metadata add column store_format_tier text null;",
            "alter table store_metadata add column catchment_type text null;",
            "alter table store_metadata add column demographic_segment text null;",
            "alter table store_metadata add column climate_zone text null;",
            "alter table store_metadata add column fulfilment_enabled integer not null default 0;",
            "alter table store_metadata add column online_fulfilment_node integer not null default 0;",
            "alter table store_metadata add column store_opening_season text null;",
            "alter table store_metadata add column store_closure_date text null;",
            "alter table store_metadata add column refurbishment_date text null;",
            "alter table store_metadata add column store_priority text null;",
            "alter table store_metadata add column is_active integer not null default 1;"
        };

        foreach (var statement in statements)
        {
            try
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = statement;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
            }
        }
    }

    private static async Task EnsureProductProfileColumnsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        var statements = new[]
        {
            "alter table product_profiles add column supplier text null;",
            "alter table product_profiles add column lifecycle_stage text null;",
            "alter table product_profiles add column age_stage text null;",
            "alter table product_profiles add column gender_target text null;",
            "alter table product_profiles add column material text null;",
            "alter table product_profiles add column pack_size text null;",
            "alter table product_profiles add column size_range text null;",
            "alter table product_profiles add column colour_family text null;",
            "alter table product_profiles add column kvi_flag integer not null default 0;",
            "alter table product_profiles add column markdown_eligible integer not null default 1;",
            "alter table product_profiles add column markdown_floor_price real null;",
            "alter table product_profiles add column minimum_margin_pct real null;",
            "alter table product_profiles add column price_ladder_group text null;",
            "alter table product_profiles add column good_better_best_tier text null;",
            "alter table product_profiles add column season_code text null;",
            "alter table product_profiles add column event_code text null;",
            "alter table product_profiles add column launch_date text null;",
            "alter table product_profiles add column end_of_life_date text null;",
            "alter table product_profiles add column substitute_group text null;",
            "alter table product_profiles add column companion_group text null;",
            "alter table product_profiles add column replenishment_type text null;",
            "alter table product_profiles add column lead_time_days integer null;",
            "alter table product_profiles add column moq integer null;",
            "alter table product_profiles add column case_pack integer null;",
            "alter table product_profiles add column starting_inventory real null;",
            "alter table product_profiles add column projected_stock_on_hand real null;",
            "alter table product_profiles add column sell_through_target_pct real null;",
            "alter table product_profiles add column weeks_of_cover_target real null;"
        };

        foreach (var statement in statements)
        {
            try
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = statement;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
            }
        }
    }

    private static async Task EnsureStoreProfileOptionSeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        var stores = await LoadStoreMetadataAsync(connection, transaction, cancellationToken);
        foreach (var store in stores.Values)
        {
            await UpsertStoreProfileOptionsForMetadataAsync(connection, transaction, store, cancellationToken);
        }
    }

    private static async Task<IReadOnlyList<StoreProfileOptionValue>> LoadStoreProfileOptionsAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<StoreProfileOptionValue>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select field_name, option_value, is_active
            from store_profile_options
            order by field_name asc, option_value asc;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new StoreProfileOptionValue(reader.GetString(0), reader.GetString(1), reader.GetInt64(2) == 1));
        }

        return result;
    }

    private static async Task UpsertStoreProfileOptionsForMetadataAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        StoreNodeMetadata metadata,
        CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["state"] = metadata.State,
            ["clusterLabel"] = metadata.ClusterLabel,
            ["regionLabel"] = metadata.RegionLabel,
            ["sssg"] = metadata.Sssg,
            ["salesType"] = metadata.SalesType,
            ["status"] = metadata.Status,
            ["buildingStatus"] = metadata.BuildingStatus,
            ["lifecycleState"] = metadata.LifecycleState,
            ["rampProfileCode"] = metadata.RampProfileCode,
            ["storeClusterRole"] = metadata.StoreClusterRole,
            ["storeFormatTier"] = metadata.StoreFormatTier,
            ["catchmentType"] = metadata.CatchmentType,
            ["demographicSegment"] = metadata.DemographicSegment,
            ["climateZone"] = metadata.ClimateZone,
            ["storeOpeningSeason"] = metadata.StoreOpeningSeason,
            ["storePriority"] = metadata.StorePriority
        };

        foreach (var (fieldName, value) in values)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            await UpsertStoreProfileOptionInternalAsync(connection, transaction, fieldName, value, true, cancellationToken);
        }
    }

    private static async Task UpsertStoreProfileOptionInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string fieldName,
        string value,
        bool isActive,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into store_profile_options (field_name, option_value, is_active)
            values ($fieldName, $value, $isActive)
            on conflict (field_name, option_value)
            do update set is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("$value", value.Trim());
        command.Parameters.AddWithValue("$isActive", isActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteStoreProfileOptionInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string fieldName,
        string value,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            delete from store_profile_options
            where field_name = $fieldName
              and option_value = $value;
            """;
        command.Parameters.AddWithValue("$fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("$value", value.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> CountProductProfilesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? normalizedSearch,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? "select count(*) from product_profiles;"
            : """
                select count(*)
                from product_profiles
                where sku_variant like $search
                   or description like $search
                   or department like $search
                   or class like $search
                   or subclass like $search;
                """;
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$search", normalizedSearch);
        }

        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task<IReadOnlyList<ProductProfileMetadata>> LoadProductProfilesAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string? normalizedSearch,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductProfileMetadata>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = normalizedSearch is null
            ? """
                select sku_variant, description, description2, price, cost, dpt_no, clss_no, brand_no, department, class,
                       brand, rev_department, rev_class, subclass, prod_group, prod_type, active_flag, order_flag,
                       brand_type, launch_month, gender, size, collection, promo, ramadhan_promo, supplier, lifecycle_stage,
                       age_stage, gender_target, material, pack_size, size_range, colour_family, kvi_flag, markdown_eligible,
                       markdown_floor_price, minimum_margin_pct, price_ladder_group, good_better_best_tier, season_code, event_code,
                       launch_date, end_of_life_date, substitute_group, companion_group, replenishment_type, lead_time_days, moq,
                       case_pack, starting_inventory, projected_stock_on_hand, sell_through_target_pct, weeks_of_cover_target, is_active
                from product_profiles
                order by department, class, subclass, description, sku_variant
                limit $limit offset $offset;
                """
            : """
                select sku_variant, description, description2, price, cost, dpt_no, clss_no, brand_no, department, class,
                       brand, rev_department, rev_class, subclass, prod_group, prod_type, active_flag, order_flag,
                       brand_type, launch_month, gender, size, collection, promo, ramadhan_promo, supplier, lifecycle_stage,
                       age_stage, gender_target, material, pack_size, size_range, colour_family, kvi_flag, markdown_eligible,
                       markdown_floor_price, minimum_margin_pct, price_ladder_group, good_better_best_tier, season_code, event_code,
                       launch_date, end_of_life_date, substitute_group, companion_group, replenishment_type, lead_time_days, moq,
                       case_pack, starting_inventory, projected_stock_on_hand, sell_through_target_pct, weeks_of_cover_target, is_active
                from product_profiles
                where sku_variant like $search
                   or description like $search
                   or department like $search
                   or class like $search
                   or subclass like $search
                order by department, class, subclass, description, sku_variant
                limit $limit offset $offset;
                """;
        command.Parameters.AddWithValue("$limit", limit);
        command.Parameters.AddWithValue("$offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("$search", normalizedSearch);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductProfileMetadata(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                ReadDecimal(reader, 3),
                ReadDecimal(reader, 4),
                reader.GetString(5),
                reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetString(8),
                reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14),
                reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetString(17),
                reader.IsDBNull(18) ? null : reader.GetString(18),
                reader.IsDBNull(19) ? null : reader.GetString(19),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21),
                reader.IsDBNull(22) ? null : reader.GetString(22),
                reader.IsDBNull(23) ? null : reader.GetString(23),
                reader.IsDBNull(24) ? null : reader.GetString(24),
                reader.GetInt64(53) == 1,
                reader.IsDBNull(25) ? null : reader.GetString(25),
                reader.IsDBNull(26) ? null : reader.GetString(26),
                reader.IsDBNull(27) ? null : reader.GetString(27),
                reader.IsDBNull(28) ? null : reader.GetString(28),
                reader.IsDBNull(29) ? null : reader.GetString(29),
                reader.IsDBNull(30) ? null : reader.GetString(30),
                reader.IsDBNull(31) ? null : reader.GetString(31),
                reader.IsDBNull(32) ? null : reader.GetString(32),
                !reader.IsDBNull(33) && reader.GetInt64(33) == 1,
                reader.IsDBNull(34) || reader.GetInt64(34) == 1,
                reader.IsDBNull(35) ? null : ReadDecimal(reader, 35),
                reader.IsDBNull(36) ? null : ReadDecimal(reader, 36),
                reader.IsDBNull(37) ? null : reader.GetString(37),
                reader.IsDBNull(38) ? null : reader.GetString(38),
                reader.IsDBNull(39) ? null : reader.GetString(39),
                reader.IsDBNull(40) ? null : reader.GetString(40),
                reader.IsDBNull(41) ? null : reader.GetString(41),
                reader.IsDBNull(42) ? null : reader.GetString(42),
                reader.IsDBNull(43) ? null : reader.GetString(43),
                reader.IsDBNull(44) ? null : reader.GetString(44),
                reader.IsDBNull(45) ? null : reader.GetString(45),
                reader.IsDBNull(46) ? null : reader.GetInt32(46),
                reader.IsDBNull(47) ? null : reader.GetInt32(47),
                reader.IsDBNull(48) ? null : reader.GetInt32(48),
                reader.IsDBNull(49) ? null : ReadDecimal(reader, 49),
                reader.IsDBNull(50) ? null : ReadDecimal(reader, 50),
                reader.IsDBNull(51) ? null : ReadDecimal(reader, 51),
                reader.IsDBNull(52) ? null : ReadDecimal(reader, 52)));
        }

        return result;
    }

    private static ProductProfileMetadata NormalizeProductProfile(ProductProfileMetadata profile)
    {
        string Require(string? value, string label)
        {
            var normalized = string.IsNullOrWhiteSpace(value) ? null : value.Trim();
            if (normalized is null)
            {
                throw new InvalidOperationException($"{label} is required.");
            }

            return normalized;
        }

        string? Optional(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

        return profile with
        {
            SkuVariant = Require(profile.SkuVariant, "SKU Variant"),
            Description = Require(profile.Description, "Description"),
            Description2 = Optional(profile.Description2),
            Price = PlanningMath.NormalizeAsp(profile.Price),
            Cost = PlanningMath.NormalizeUnitCost(profile.Cost),
            DptNo = Require(profile.DptNo, "DptNo"),
            ClssNo = Require(profile.ClssNo, "ClssNo"),
            BrandNo = Optional(profile.BrandNo),
            Department = Require(profile.Department, "Department"),
            Class = Require(profile.Class, "Class"),
            Brand = Optional(profile.Brand),
            RevDepartment = Optional(profile.RevDepartment),
            RevClass = Optional(profile.RevClass),
            Subclass = Require(profile.Subclass, "Subclass"),
            ProdGroup = Optional(profile.ProdGroup),
            ProdType = Optional(profile.ProdType),
            ActiveFlag = Optional(profile.ActiveFlag),
            OrderFlag = Optional(profile.OrderFlag),
            BrandType = Optional(profile.BrandType),
            LaunchMonth = Optional(profile.LaunchMonth),
            Gender = Optional(profile.Gender),
            Size = Optional(profile.Size),
            Collection = Optional(profile.Collection),
            Promo = Optional(profile.Promo),
            RamadhanPromo = Optional(profile.RamadhanPromo),
            Supplier = Optional(profile.Supplier),
            LifecycleStage = Optional(profile.LifecycleStage),
            AgeStage = Optional(profile.AgeStage),
            GenderTarget = Optional(profile.GenderTarget),
            Material = Optional(profile.Material),
            PackSize = Optional(profile.PackSize),
            SizeRange = Optional(profile.SizeRange),
            ColourFamily = Optional(profile.ColourFamily),
            PriceLadderGroup = Optional(profile.PriceLadderGroup),
            GoodBetterBestTier = Optional(profile.GoodBetterBestTier),
            SeasonCode = Optional(profile.SeasonCode),
            EventCode = Optional(profile.EventCode),
            LaunchDate = Optional(profile.LaunchDate),
            EndOfLifeDate = Optional(profile.EndOfLifeDate),
            SubstituteGroup = Optional(profile.SubstituteGroup),
            CompanionGroup = Optional(profile.CompanionGroup),
            ReplenishmentType = Optional(profile.ReplenishmentType)
        };
    }

    private static async Task UpsertProductProfileInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ProductProfileMetadata profile,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into product_profiles (
                sku_variant, description, description2, price, cost, dpt_no, clss_no, brand_no, department, class,
                brand, rev_department, rev_class, subclass, prod_group, prod_type, active_flag, order_flag, brand_type,
                launch_month, gender, size, collection, promo, ramadhan_promo, supplier, lifecycle_stage, age_stage,
                gender_target, material, pack_size, size_range, colour_family, kvi_flag, markdown_eligible, markdown_floor_price,
                minimum_margin_pct, price_ladder_group, good_better_best_tier, season_code, event_code, launch_date, end_of_life_date,
                substitute_group, companion_group, replenishment_type, lead_time_days, moq, case_pack, starting_inventory,
                projected_stock_on_hand, sell_through_target_pct, weeks_of_cover_target, is_active)
            values (
                $skuVariant, $description, $description2, $price, $cost, $dptNo, $clssNo, $brandNo, $department, $class,
                $brand, $revDepartment, $revClass, $subclass, $prodGroup, $prodType, $activeFlag, $orderFlag, $brandType,
                $launchMonth, $gender, $size, $collection, $promo, $ramadhanPromo, $supplier, $lifecycleStage, $ageStage,
                $genderTarget, $material, $packSize, $sizeRange, $colourFamily, $kviFlag, $markdownEligible, $markdownFloorPrice,
                $minimumMarginPct, $priceLadderGroup, $goodBetterBestTier, $seasonCode, $eventCode, $launchDate, $endOfLifeDate,
                $substituteGroup, $companionGroup, $replenishmentType, $leadTimeDays, $moq, $casePack, $startingInventory,
                $projectedStockOnHand, $sellThroughTargetPct, $weeksOfCoverTarget, $isActive)
            on conflict (sku_variant)
            do update set
                description = excluded.description,
                description2 = excluded.description2,
                price = excluded.price,
                cost = excluded.cost,
                dpt_no = excluded.dpt_no,
                clss_no = excluded.clss_no,
                brand_no = excluded.brand_no,
                department = excluded.department,
                class = excluded.class,
                brand = excluded.brand,
                rev_department = excluded.rev_department,
                rev_class = excluded.rev_class,
                subclass = excluded.subclass,
                prod_group = excluded.prod_group,
                prod_type = excluded.prod_type,
                active_flag = excluded.active_flag,
                order_flag = excluded.order_flag,
                brand_type = excluded.brand_type,
                launch_month = excluded.launch_month,
                gender = excluded.gender,
                size = excluded.size,
                collection = excluded.collection,
                promo = excluded.promo,
                ramadhan_promo = excluded.ramadhan_promo,
                supplier = excluded.supplier,
                lifecycle_stage = excluded.lifecycle_stage,
                age_stage = excluded.age_stage,
                gender_target = excluded.gender_target,
                material = excluded.material,
                pack_size = excluded.pack_size,
                size_range = excluded.size_range,
                colour_family = excluded.colour_family,
                kvi_flag = excluded.kvi_flag,
                markdown_eligible = excluded.markdown_eligible,
                markdown_floor_price = excluded.markdown_floor_price,
                minimum_margin_pct = excluded.minimum_margin_pct,
                price_ladder_group = excluded.price_ladder_group,
                good_better_best_tier = excluded.good_better_best_tier,
                season_code = excluded.season_code,
                event_code = excluded.event_code,
                launch_date = excluded.launch_date,
                end_of_life_date = excluded.end_of_life_date,
                substitute_group = excluded.substitute_group,
                companion_group = excluded.companion_group,
                replenishment_type = excluded.replenishment_type,
                lead_time_days = excluded.lead_time_days,
                moq = excluded.moq,
                case_pack = excluded.case_pack,
                starting_inventory = excluded.starting_inventory,
                projected_stock_on_hand = excluded.projected_stock_on_hand,
                sell_through_target_pct = excluded.sell_through_target_pct,
                weeks_of_cover_target = excluded.weeks_of_cover_target,
                is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$skuVariant", profile.SkuVariant);
        command.Parameters.AddWithValue("$description", profile.Description);
        command.Parameters.AddWithValue("$description2", (object?)profile.Description2 ?? DBNull.Value);
        command.Parameters.AddWithValue("$price", profile.Price);
        command.Parameters.AddWithValue("$cost", profile.Cost);
        command.Parameters.AddWithValue("$dptNo", profile.DptNo);
        command.Parameters.AddWithValue("$clssNo", profile.ClssNo);
        command.Parameters.AddWithValue("$brandNo", (object?)profile.BrandNo ?? DBNull.Value);
        command.Parameters.AddWithValue("$department", profile.Department);
        command.Parameters.AddWithValue("$class", profile.Class);
        command.Parameters.AddWithValue("$brand", (object?)profile.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("$revDepartment", (object?)profile.RevDepartment ?? DBNull.Value);
        command.Parameters.AddWithValue("$revClass", (object?)profile.RevClass ?? DBNull.Value);
        command.Parameters.AddWithValue("$subclass", profile.Subclass);
        command.Parameters.AddWithValue("$prodGroup", (object?)profile.ProdGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$prodType", (object?)profile.ProdType ?? DBNull.Value);
        command.Parameters.AddWithValue("$activeFlag", (object?)profile.ActiveFlag ?? DBNull.Value);
        command.Parameters.AddWithValue("$orderFlag", (object?)profile.OrderFlag ?? DBNull.Value);
        command.Parameters.AddWithValue("$brandType", (object?)profile.BrandType ?? DBNull.Value);
        command.Parameters.AddWithValue("$launchMonth", (object?)profile.LaunchMonth ?? DBNull.Value);
        command.Parameters.AddWithValue("$gender", (object?)profile.Gender ?? DBNull.Value);
        command.Parameters.AddWithValue("$size", (object?)profile.Size ?? DBNull.Value);
        command.Parameters.AddWithValue("$collection", (object?)profile.Collection ?? DBNull.Value);
        command.Parameters.AddWithValue("$promo", (object?)profile.Promo ?? DBNull.Value);
        command.Parameters.AddWithValue("$ramadhanPromo", (object?)profile.RamadhanPromo ?? DBNull.Value);
        command.Parameters.AddWithValue("$supplier", (object?)profile.Supplier ?? DBNull.Value);
        command.Parameters.AddWithValue("$lifecycleStage", (object?)profile.LifecycleStage ?? DBNull.Value);
        command.Parameters.AddWithValue("$ageStage", (object?)profile.AgeStage ?? DBNull.Value);
        command.Parameters.AddWithValue("$genderTarget", (object?)profile.GenderTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("$material", (object?)profile.Material ?? DBNull.Value);
        command.Parameters.AddWithValue("$packSize", (object?)profile.PackSize ?? DBNull.Value);
        command.Parameters.AddWithValue("$sizeRange", (object?)profile.SizeRange ?? DBNull.Value);
        command.Parameters.AddWithValue("$colourFamily", (object?)profile.ColourFamily ?? DBNull.Value);
        command.Parameters.AddWithValue("$kviFlag", profile.KviFlag ? 1 : 0);
        command.Parameters.AddWithValue("$markdownEligible", profile.MarkdownEligible ? 1 : 0);
        command.Parameters.AddWithValue("$markdownFloorPrice", (object?)profile.MarkdownFloorPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("$minimumMarginPct", (object?)profile.MinimumMarginPct ?? DBNull.Value);
        command.Parameters.AddWithValue("$priceLadderGroup", (object?)profile.PriceLadderGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$goodBetterBestTier", (object?)profile.GoodBetterBestTier ?? DBNull.Value);
        command.Parameters.AddWithValue("$seasonCode", (object?)profile.SeasonCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$eventCode", (object?)profile.EventCode ?? DBNull.Value);
        command.Parameters.AddWithValue("$launchDate", (object?)profile.LaunchDate ?? DBNull.Value);
        command.Parameters.AddWithValue("$endOfLifeDate", (object?)profile.EndOfLifeDate ?? DBNull.Value);
        command.Parameters.AddWithValue("$substituteGroup", (object?)profile.SubstituteGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$companionGroup", (object?)profile.CompanionGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$replenishmentType", (object?)profile.ReplenishmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("$leadTimeDays", (object?)profile.LeadTimeDays ?? DBNull.Value);
        command.Parameters.AddWithValue("$moq", (object?)profile.Moq ?? DBNull.Value);
        command.Parameters.AddWithValue("$casePack", (object?)profile.CasePack ?? DBNull.Value);
        command.Parameters.AddWithValue("$startingInventory", (object?)profile.StartingInventory ?? DBNull.Value);
        command.Parameters.AddWithValue("$projectedStockOnHand", (object?)profile.ProjectedStockOnHand ?? DBNull.Value);
        command.Parameters.AddWithValue("$sellThroughTargetPct", (object?)profile.SellThroughTargetPct ?? DBNull.Value);
        command.Parameters.AddWithValue("$weeksOfCoverTarget", (object?)profile.WeeksOfCoverTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("$isActive", profile.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertProductProfileOptionSeedsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ProductProfileMetadata profile,
        CancellationToken cancellationToken)
    {
        var options = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase)
        {
            ["department"] = profile.Department,
            ["class"] = profile.Class,
            ["subclass"] = profile.Subclass,
            ["prodGroup"] = profile.ProdGroup,
            ["prodType"] = profile.ProdType,
            ["brand"] = profile.Brand,
            ["brandType"] = profile.BrandType,
            ["gender"] = profile.Gender,
            ["size"] = profile.Size,
            ["collection"] = profile.Collection,
            ["promo"] = profile.Promo,
            ["ramadhanPromo"] = profile.RamadhanPromo,
            ["activeFlag"] = profile.ActiveFlag,
            ["orderFlag"] = profile.OrderFlag,
            ["launchMonth"] = profile.LaunchMonth,
            ["supplier"] = profile.Supplier,
            ["lifecycleStage"] = profile.LifecycleStage,
            ["ageStage"] = profile.AgeStage,
            ["genderTarget"] = profile.GenderTarget,
            ["material"] = profile.Material,
            ["packSize"] = profile.PackSize,
            ["sizeRange"] = profile.SizeRange,
            ["colourFamily"] = profile.ColourFamily,
            ["priceLadderGroup"] = profile.PriceLadderGroup,
            ["goodBetterBestTier"] = profile.GoodBetterBestTier,
            ["seasonCode"] = profile.SeasonCode,
            ["eventCode"] = profile.EventCode,
            ["replenishmentType"] = profile.ReplenishmentType
        };

        foreach (var (fieldName, value) in options)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            await UpsertOptionAsync(connection, transaction, "product_profile_options", fieldName, value, true, cancellationToken);
        }
    }

    private static async Task UpsertDerivedSubclassCatalogAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ProductProfileMetadata profile,
        CancellationToken cancellationToken)
    {
        await UpsertProductHierarchyCatalogInternalAsync(connection, transaction, new ProductHierarchyCatalogRecord(
            profile.DptNo,
            profile.ClssNo,
            profile.Department,
            profile.Class,
            profile.ProdGroup ?? "UNASSIGNED",
            true), cancellationToken);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into product_subclass_catalog (department, class, subclass, is_active)
            values ($department, $class, $subclass, $isActive)
            on conflict (department, class, subclass)
            do update set is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$department", profile.Department);
        command.Parameters.AddWithValue("$class", profile.Class);
        command.Parameters.AddWithValue("$subclass", profile.Subclass);
        command.Parameters.AddWithValue("$isActive", profile.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<IReadOnlyList<ProductProfileOptionValue>> LoadProductProfileOptionsAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductProfileOptionValue>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select field_name, option_value, is_active
            from product_profile_options
            order by field_name asc, option_value asc;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductProfileOptionValue(reader.GetString(0), reader.GetString(1), reader.GetInt64(2) == 1));
        }

        return result;
    }

    private static async Task UpsertOptionAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        string fieldName,
        string value,
        bool isActive,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            insert into {tableName} (field_name, option_value, is_active)
            values ($fieldName, $value, $isActive)
            on conflict (field_name, option_value)
            do update set is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("$value", value.Trim());
        command.Parameters.AddWithValue("$isActive", isActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteOptionAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string tableName,
        string fieldName,
        string value,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"""
            delete from {tableName}
            where field_name = $fieldName
              and option_value = $value;
            """;
        command.Parameters.AddWithValue("$fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("$value", value.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<IReadOnlyList<ProductHierarchyCatalogRecord>> LoadProductHierarchyCatalogAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductHierarchyCatalogRecord>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select dpt_no, clss_no, department, class, prod_group, is_active
            from product_hierarchy_catalog
            order by department asc, class asc;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductHierarchyCatalogRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetInt64(5) == 1));
        }

        return result;
    }

    private static async Task<IReadOnlyList<ProductSubclassCatalogRecord>> LoadProductSubclassCatalogAsync(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductSubclassCatalogRecord>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select department, class, subclass, is_active
            from product_subclass_catalog
            order by department asc, class asc, subclass asc;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductSubclassCatalogRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt64(3) == 1));
        }

        return result;
    }

    private static ProductHierarchyCatalogRecord NormalizeProductHierarchyRecord(ProductHierarchyCatalogRecord record)
    {
        string Require(string? value, string label)
        {
            var normalized = string.IsNullOrWhiteSpace(value) ? null : value.Trim();
            if (normalized is null)
            {
                throw new InvalidOperationException($"{label} is required.");
            }

            return normalized;
        }

        return record with
        {
            DptNo = Require(record.DptNo, "DptNo"),
            ClssNo = Require(record.ClssNo, "ClssNo"),
            Department = Require(record.Department, "Department"),
            Class = Require(record.Class, "Class"),
            ProdGroup = Require(record.ProdGroup, "Prod Group")
        };
    }

    private static async Task UpsertProductHierarchyCatalogInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        ProductHierarchyCatalogRecord record,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into product_hierarchy_catalog (dpt_no, clss_no, department, class, prod_group, is_active)
            values ($dptNo, $clssNo, $department, $class, $prodGroup, $isActive)
            on conflict (dpt_no, clss_no)
            do update set
                department = excluded.department,
                class = excluded.class,
                prod_group = excluded.prod_group,
                is_active = excluded.is_active;
            """;
        command.Parameters.AddWithValue("$dptNo", record.DptNo);
        command.Parameters.AddWithValue("$clssNo", record.ClssNo);
        command.Parameters.AddWithValue("$department", record.Department);
        command.Parameters.AddWithValue("$class", record.Class);
        command.Parameters.AddWithValue("$prodGroup", record.ProdGroup);
        command.Parameters.AddWithValue("$isActive", record.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task RefreshProductSubclassCatalogAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using (var deleteSubclasses = connection.CreateCommand())
        {
            deleteSubclasses.Transaction = transaction;
            deleteSubclasses.CommandText = "delete from product_subclass_catalog;";
            await deleteSubclasses.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var deleteHierarchy = connection.CreateCommand())
        {
            deleteHierarchy.Transaction = transaction;
            deleteHierarchy.CommandText = "delete from product_hierarchy_catalog;";
            await deleteHierarchy.ExecuteNonQueryAsync(cancellationToken);
        }

        var profiles = await LoadProductProfilesAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
        foreach (var hierarchyRow in profiles
                     .Select(profile => new ProductHierarchyCatalogRecord(profile.DptNo, profile.ClssNo, profile.Department, profile.Class, profile.ProdGroup ?? "UNASSIGNED", profile.IsActive))
                     .Distinct())
        {
            await UpsertProductHierarchyCatalogInternalAsync(connection, transaction, hierarchyRow, cancellationToken);
        }

        foreach (var subclassRow in profiles
                     .Select(profile => new ProductSubclassCatalogRecord(profile.Department, profile.Class, profile.Subclass, profile.IsActive))
                     .Distinct())
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                insert into product_subclass_catalog (department, class, subclass, is_active)
                values ($department, $class, $subclass, $isActive)
                on conflict (department, class, subclass)
                do update set is_active = excluded.is_active;
                """;
            command.Parameters.AddWithValue("$department", subclassRow.Department);
            command.Parameters.AddWithValue("$class", subclassRow.Class);
            command.Parameters.AddWithValue("$subclass", subclassRow.Subclass);
            command.Parameters.AddWithValue("$isActive", subclassRow.IsActive ? 1 : 0);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task RebuildPlanningFromMasterDataInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        int fiscalYear,
        CancellationToken cancellationToken)
    {
        var stores = (await LoadStoreMetadataAsync(connection, transaction, cancellationToken)).Values
            .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var hierarchyRows = (await LoadProductHierarchyCatalogAsync(connection, transaction, cancellationToken))
            .Where(row => row.IsActive)
            .OrderBy(row => row.Department, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Class, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var subclassRows = (await LoadProductSubclassCatalogAsync(connection, transaction, cancellationToken))
            .Where(row => row.IsActive)
            .OrderBy(row => row.Department, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Subclass, StringComparer.OrdinalIgnoreCase)
            .ToList();

        await ClearPlanningCommandHistoryAsync(connection, transaction, scenarioVersionId, cancellationToken);

        foreach (var tableName in new[] { "planning_cells", "product_nodes", "hierarchy_subclasses_v2", "hierarchy_classes_v2", "hierarchy_departments_v2", "hierarchy_subcategories", "hierarchy_categories", "time_periods" })
        {
            await using var deleteCommand = connection.CreateCommand();
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"delete from {tableName};";
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var period in BuildYearPeriods(fiscalYear).Values.OrderBy(period => period.SortOrder))
        {
            await using var insertPeriod = connection.CreateCommand();
            insertPeriod.Transaction = transaction;
            insertPeriod.CommandText = """
                insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
                values ($timePeriodId, $parentTimePeriodId, $label, $grain, $sortOrder);
                """;
            insertPeriod.Parameters.AddWithValue("$timePeriodId", period.TimePeriodId);
            insertPeriod.Parameters.AddWithValue("$parentTimePeriodId", (object?)period.ParentTimePeriodId ?? DBNull.Value);
            insertPeriod.Parameters.AddWithValue("$label", period.Label);
            insertPeriod.Parameters.AddWithValue("$grain", period.Grain);
            insertPeriod.Parameters.AddWithValue("$sortOrder", period.SortOrder);
            await insertPeriod.ExecuteNonQueryAsync(cancellationToken);
        }

        var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
        var nextProductNodeId = stores.Count == 0 ? 100 : stores.Max(store => store.StoreId);
        var classLookup = hierarchyRows.GroupBy(row => row.Department, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.ToList(), StringComparer.OrdinalIgnoreCase);
        var subclassLookup = subclassRows.GroupBy(row => (row.Department, row.Class))
            .ToDictionary(group => group.Key, group => group.ToList());

        foreach (var store in stores)
        {
            var rootNode = new ProductNode(
                store.StoreId,
                store.StoreId,
                null,
                store.StoreLabel,
                0,
                [store.StoreLabel],
                false,
                "store",
                store.LifecycleState,
                store.RampProfileCode,
                store.EffectiveFromTimePeriodId,
                store.EffectiveToTimePeriodId);
            await InsertProductNodeAsync(connection, transaction, rootNode, cancellationToken);
            await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, rootNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);

            foreach (var hierarchyGroup in classLookup)
            {
                var departmentLabel = hierarchyGroup.Key;
                var departmentNode = new ProductNode(
                    ++nextProductNodeId,
                    store.StoreId,
                    rootNode.ProductNodeId,
                    departmentLabel,
                    1,
                    [store.StoreLabel, departmentLabel],
                    false,
                    "department",
                    "active",
                    null,
                    null,
                    null);
                await InsertProductNodeAsync(connection, transaction, departmentNode, cancellationToken);
                await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, departmentNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);

                foreach (var classRow in hierarchyGroup.Value)
                {
                    var classNode = new ProductNode(
                        ++nextProductNodeId,
                        store.StoreId,
                        departmentNode.ProductNodeId,
                        classRow.Class,
                        2,
                        [store.StoreLabel, departmentLabel, classRow.Class],
                        false,
                        "class",
                        "active",
                        null,
                        null,
                        null);
                    await InsertProductNodeAsync(connection, transaction, classNode, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, classNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);

                    foreach (var subclassRow in subclassLookup.GetValueOrDefault((departmentLabel, classRow.Class)) ?? [])
                    {
                        var subclassNode = new ProductNode(
                            ++nextProductNodeId,
                            store.StoreId,
                            classNode.ProductNodeId,
                            subclassRow.Subclass,
                            3,
                            [store.StoreLabel, departmentLabel, classRow.Class, subclassRow.Subclass],
                            true,
                            "subclass",
                            "active",
                            null,
                            null,
                            null);
                        await InsertProductNodeAsync(connection, transaction, subclassNode, cancellationToken);
                        await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, subclassNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    }
                }
            }
        }

        var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
        var profiles = await LoadProductProfilesAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
        var subclassAverages = profiles
            .Where(profile => profile.IsActive)
            .GroupBy(profile => (profile.Department, profile.Class, profile.Subclass))
            .ToDictionary(
                group => group.Key,
                group => new
                {
                    Price = PlanningMath.NormalizeAsp(group.Average(item => item.Price)),
                    Cost = PlanningMath.NormalizeUnitCost(group.Average(item => item.Cost))
                });

        var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
        var workingCells = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
        var monthPeriods = timePeriods.Values.Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase)).OrderBy(period => period.SortOrder).ToList();

        foreach (var leafNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            var subclassKey = (Department: leafNode.Path[1], Class: leafNode.Path[2], Subclass: leafNode.Path[3]);
            var averages = subclassAverages.GetValueOrDefault(subclassKey) ?? new { Price = 10.00m, Cost = 6.00m };

            foreach (var period in monthPeriods)
            {
                var quantity = BuildMockQuantity(leafNode.StoreId, subclassKey.Department, subclassKey.Class, subclassKey.Subclass, period.TimePeriodId);
                var revenue = PlanningMath.CalculateRevenue(quantity, averages.Price);
                var totalCosts = PlanningMath.CalculateTotalCosts(quantity, averages.Cost);
                var grossProfit = PlanningMath.CalculateGrossProfit(quantity, averages.Price, averages.Cost);
                var grossProfitPercent = PlanningMath.CalculateGrossProfitPercent(averages.Price, averages.Cost);

                SetSeedInputValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], quantity);
                SetSeedInputValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], averages.Price);
                SetSeedInputValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], averages.Cost);
                SetSeedInputValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], revenue);
                SetSeedCalculatedValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], totalCosts, true);
                SetSeedCalculatedValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfit, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], grossProfit, true);
                SetSeedCalculatedValue(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfitPercent, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], grossProfitPercent, true);
            }
        }

        RecalculateSeedTotals(scenarioVersionId, PlanningMeasures.SalesRevenue, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(scenarioVersionId, PlanningMeasures.SoldQuantity, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(scenarioVersionId, PlanningMeasures.TotalCosts, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(scenarioVersionId, PlanningMeasures.GrossProfit, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(scenarioVersionId, PlanningMeasures.AverageSellingPrice, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(scenarioVersionId, PlanningMeasures.UnitCost, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(scenarioVersionId, PlanningMeasures.GrossProfitPercent, workingCells, productNodes, timePeriods);

        await RebuildHierarchyMappingsAsync(connection, transaction, cancellationToken);
        foreach (var cell in workingCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static decimal BuildMockQuantity(long storeId, string department, string classLabel, string subclass, long timePeriodId)
    {
        var seed = $"{storeId}|{department}|{classLabel}|{subclass}|{timePeriodId}";
        unchecked
        {
            var hash = 23;
            foreach (var character in seed)
            {
                hash = (hash * 31) + character;
            }

            var index = Math.Abs(hash % 5);
            return (index + 1) * 100m;
        }
    }

    private static async Task EnsureProductProfileSeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var countCommand = connection.CreateCommand();
        countCommand.Transaction = transaction;
        countCommand.CommandText = "select count(*) from product_profiles;";
        var existingCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        if (existingCount > 0)
        {
            return;
        }

        var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
        var classCodes = new Dictionary<(string Department, string Class), (string DptNo, string ClssNo)>();
        var departmentNumbers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var leafNode in productNodes.Values.Where(node => node.IsLeaf).OrderBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase))
        {
            var department = leafNode.Path[1];
            var classLabel = leafNode.Path[2];
            if (!departmentNumbers.ContainsKey(department))
            {
                departmentNumbers[department] = departmentNumbers.Count + 10;
            }

            if (!classCodes.ContainsKey((department, classLabel)))
            {
                var dptNo = departmentNumbers[department].ToString("000", CultureInfo.InvariantCulture);
                var clssNo = $"{departmentNumbers[department]}{classCodes.Count(code => code.Key.Department.Equals(department, StringComparison.OrdinalIgnoreCase)) + 1:00}";
                classCodes[(department, classLabel)] = (dptNo, clssNo);
            }

            var codes = classCodes[(department, classLabel)];
            var profile = new ProductProfileMetadata(
                $"SKU-{leafNode.ProductNodeId}",
                leafNode.Label,
                null,
                10.00m + (leafNode.ProductNodeId % 5),
                6.00m + (leafNode.ProductNodeId % 3),
                codes.DptNo,
                codes.ClssNo,
                null,
                department,
                classLabel,
                null,
                null,
                null,
                leafNode.Label,
                "UNASSIGNED",
                null,
                "1",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                true);
            await UpsertProductProfileInternalAsync(connection, transaction, profile, cancellationToken);
            await UpsertDerivedSubclassCatalogAsync(connection, transaction, profile, cancellationToken);
        }
    }

    private static async Task EnsureProductProfileOptionSeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using (var deleteCommand = connection.CreateCommand())
        {
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = "delete from product_profile_options;";
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var profiles = await LoadProductProfilesAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
        foreach (var profile in profiles)
        {
            await UpsertProductProfileOptionSeedsAsync(connection, transaction, profile, cancellationToken);
        }
    }

    private static async Task EnsureQuantitySeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "select count(*) from planning_cells where measure_id = 2 and input_value is not null;";
        var existingQuantityInputs = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        if (existingQuantityInputs > 0)
        {
            return;
        }

        var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
        var quantityCells = cells
            .Where(cell => cell.Coordinate.ScenarioVersionId == 1 && cell.Coordinate.MeasureId == 2)
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

        var quantityLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2110)] = new()
            {
                [202601] = 120m, [202602] = 150m, [202603] = 142m, [202604] = 144m,
                [202605] = 146m, [202606] = 149m, [202607] = 153m, [202608] = 155m,
                [202609] = 148m, [202610] = 140m, [202611] = 146m, [202612] = 158m
            },
            [(101, 2120)] = new()
            {
                [202601] = 55m, [202602] = 58m, [202603] = 57m, [202604] = 59m,
                [202605] = 60m, [202606] = 62m, [202607] = 64m, [202608] = 63m,
                [202609] = 61m, [202610] = 60m, [202611] = 61m, [202612] = 66m
            },
            [(101, 2210)] = new()
            {
                [202601] = 80m, [202602] = 84m, [202603] = 82m, [202604] = 85m,
                [202605] = 88m, [202606] = 90m, [202607] = 92m, [202608] = 94m,
                [202609] = 91m, [202610] = 88m, [202611] = 89m, [202612] = 96m
            }
        };

        foreach (var (key, monthValues) in quantityLeafValues)
        {
            foreach (var monthValue in monthValues)
            {
                var coordinate = new PlanningCellCoordinate(1, 2, key.StoreId, key.ProductNodeId, monthValue.Key);
                if (!quantityCells.TryGetValue(coordinate.Key, out var cell))
                {
                    continue;
                }

                cell.InputValue = monthValue.Value;
                cell.DerivedValue = monthValue.Value;
                cell.EffectiveValue = monthValue.Value;
                cell.RowVersion = Math.Max(cell.RowVersion, 2);
                cell.CellKind = "input";
            }
        }

        RecalculateSeedTotals(1, 2, quantityCells, productNodes, timePeriods);

        foreach (var cell in quantityCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static async Task EnsureAspSeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "select count(*) from planning_cells where measure_id = 3 and input_value is not null;";
        var existingAspInputs = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        if (existingAspInputs > 0)
        {
            return;
        }

        var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
        var revenueCells = cells.Where(cell => cell.Coordinate.MeasureId == PlanningMeasures.SalesRevenue)
            .ToDictionary(cell => (cell.Coordinate.StoreId, cell.Coordinate.ProductNodeId, cell.Coordinate.TimePeriodId), cell => cell);
        var quantityCells = cells.Where(cell => cell.Coordinate.MeasureId == PlanningMeasures.SoldQuantity)
            .ToDictionary(cell => (cell.Coordinate.StoreId, cell.Coordinate.ProductNodeId, cell.Coordinate.TimePeriodId), cell => cell);
        var aspCells = cells.Where(cell => cell.Coordinate.MeasureId == PlanningMeasures.AverageSellingPrice)
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

        foreach (var productNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var period in timePeriods.Values.Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase)))
            {
                var coordinateKey = (productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId);
                var quantity = quantityCells[coordinateKey].EffectiveValue;
                var currentRevenue = revenueCells[coordinateKey].EffectiveValue;
                var asp = quantity > 0 ? Math.Round(currentRevenue / quantity, 2, MidpointRounding.AwayFromZero) : 1.00m;
                var normalizedRevenue = decimal.Ceiling(quantity * asp);
                var revenueCell = revenueCells[coordinateKey];
                revenueCell.InputValue = normalizedRevenue;
                revenueCell.DerivedValue = normalizedRevenue;
                revenueCell.EffectiveValue = normalizedRevenue;
                revenueCell.RowVersion = Math.Max(revenueCell.RowVersion, 2);
                revenueCell.CellKind = "input";
            }
        }

        RecalculateSeedTotals(
            1,
            PlanningMeasures.SalesRevenue,
            revenueCells.ToDictionary(entry => entry.Value.Coordinate.Key, entry => entry.Value),
            productNodes,
            timePeriods);

        foreach (var aspCell in aspCells.Values)
        {
            var coordinateKey = (aspCell.Coordinate.StoreId, aspCell.Coordinate.ProductNodeId, aspCell.Coordinate.TimePeriodId);
            var revenue = revenueCells[coordinateKey].EffectiveValue;
            var quantity = quantityCells[coordinateKey].EffectiveValue;
            var asp = quantity > 0 ? Math.Round(revenue / quantity, 2, MidpointRounding.AwayFromZero) : 1.00m;
            var isLeafMonth = productNodes[aspCell.Coordinate.ProductNodeId].IsLeaf &&
                              string.Equals(timePeriods[aspCell.Coordinate.TimePeriodId].Grain, "month", StringComparison.OrdinalIgnoreCase);
            aspCell.InputValue = isLeafMonth ? asp : null;
            aspCell.DerivedValue = asp;
            aspCell.EffectiveValue = asp;
            aspCell.RowVersion = Math.Max(aspCell.RowVersion, 2);
            aspCell.CellKind = isLeafMonth ? "input" : "calculated";
        }

        foreach (var cell in aspCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }

        foreach (var cell in revenueCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static async Task EnsureExtendedMeasureSeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
        var workingCells = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

        foreach (var productNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var period in timePeriods.Values.Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase)))
            {
                var revenueCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var quantityCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var aspCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.AverageSellingPrice, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var unitCostCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.UnitCost, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var totalCostsCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.TotalCosts, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var grossProfitCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.GrossProfit, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];
                var grossProfitPercentCell = workingCells[new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, productNode.StoreId, productNode.ProductNodeId, period.TimePeriodId).Key];

                var quantity = PlanningMath.NormalizeQuantity(quantityCell.InputValue ?? quantityCell.EffectiveValue);
                var revenue = PlanningMath.NormalizeRevenue(revenueCell.InputValue ?? revenueCell.EffectiveValue);
                var asp = aspCell.InputValue is not null || aspCell.EffectiveValue > 0m
                    ? PlanningMath.NormalizeAsp(aspCell.InputValue ?? aspCell.EffectiveValue)
                    : PlanningMath.DeriveAspFromRevenue(revenue, quantity);
                var unitCost = unitCostCell.InputValue is not null || unitCostCell.EffectiveValue > 0m
                    ? PlanningMath.NormalizeUnitCost(unitCostCell.InputValue ?? unitCostCell.EffectiveValue)
                    : PlanningMath.DefaultSeedUnitCost(asp);

                revenue = PlanningMath.CalculateRevenue(quantity, asp);
                var totalCosts = PlanningMath.CalculateTotalCosts(quantity, unitCost);
                var grossProfit = PlanningMath.CalculateGrossProfit(quantity, asp, unitCost);
                var grossProfitPercent = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);

                SetSeedInputValue(quantityCell, quantity);
                SetSeedInputValue(aspCell, asp);
                SetSeedInputValue(unitCostCell, unitCost);
                SetSeedInputValue(revenueCell, revenue);
                SetSeedCalculatedValue(totalCostsCell, totalCosts, true);
                SetSeedCalculatedValue(grossProfitCell, grossProfit, true);
                SetSeedCalculatedValue(grossProfitPercentCell, grossProfitPercent, true);
            }
        }

        RecalculateSeedTotals(1, PlanningMeasures.SalesRevenue, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(1, PlanningMeasures.SoldQuantity, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(1, PlanningMeasures.TotalCosts, workingCells, productNodes, timePeriods);
        RecalculateSeedTotals(1, PlanningMeasures.GrossProfit, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(1, PlanningMeasures.AverageSellingPrice, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(1, PlanningMeasures.UnitCost, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotals(1, PlanningMeasures.GrossProfitPercent, workingCells, productNodes, timePeriods);

        foreach (var cell in workingCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static ProductNode GetRequiredNode(
        IReadOnlyDictionary<long, ProductNode> productNodes,
        long? productNodeId,
        int expectedLevel,
        string childLevel)
    {
        if (productNodeId is null || !productNodes.TryGetValue(productNodeId.Value, out var parent))
        {
            throw new InvalidOperationException($"A parent row is required to add a {childLevel}.");
        }

        if (parent.Level != expectedLevel)
        {
            throw new InvalidOperationException($"A {childLevel} can only be added beneath a level {expectedLevel} row.");
        }

        return parent;
    }

    private static async Task<long> CloneStoreHierarchyAndDataAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        long sourceStoreId,
        ProductNode targetStoreNode,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        IReadOnlyList<PlanningCell> cells,
        long nextProductNodeId,
        CancellationToken cancellationToken)
    {
        var sourceRootNode = productNodes.Values.SingleOrDefault(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is null)
            ?? throw new InvalidOperationException($"Store {sourceStoreId} was not found for copy.");
        var sourceNodes = productNodes.Values
            .Where(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is not null)
            .OrderBy(node => node.Level)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .ToList();
        var cellLookup = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var nodeMap = new Dictionary<long, ProductNode>
        {
            [sourceRootNode.ProductNodeId] = targetStoreNode
        };

        foreach (var sourceNode in sourceNodes)
        {
            var parent = nodeMap[sourceNode.ParentProductNodeId!.Value];
            var clonedNode = new ProductNode(
                ++nextProductNodeId,
                targetStoreNode.StoreId,
                parent.ProductNodeId,
                sourceNode.Label,
                sourceNode.Level,
                parent.Path.Append(sourceNode.Label).ToArray(),
                sourceNode.IsLeaf,
                sourceNode.NodeKind,
                sourceNode.LifecycleState,
                sourceNode.RampProfileCode,
                sourceNode.EffectiveFromTimePeriodId,
                sourceNode.EffectiveToTimePeriodId);
            await InsertProductNodeAsync(connection, transaction, clonedNode, cancellationToken);
            await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, clonedNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
            nodeMap[sourceNode.ProductNodeId] = clonedNode;
        }

        foreach (var sourceNode in sourceNodes.Prepend(sourceRootNode))
        {
            var targetNode = nodeMap[sourceNode.ProductNodeId];
            foreach (var supportedMeasureId in SupportedMeasureIds)
            {
                foreach (var period in timePeriods.Values)
                {
                    var sourceCoordinate = new PlanningCellCoordinate(scenarioVersionId, supportedMeasureId, sourceStoreId, sourceNode.ProductNodeId, period.TimePeriodId);
                    var targetCoordinate = new PlanningCellCoordinate(scenarioVersionId, supportedMeasureId, targetStoreNode.StoreId, targetNode.ProductNodeId, period.TimePeriodId);
                    if (!cellLookup.TryGetValue(sourceCoordinate.Key, out var sourceCell))
                    {
                        continue;
                    }

                    var clonedCell = sourceCell.Clone();
                    clonedCell.Coordinate = targetCoordinate;
                    clonedCell.IsLocked = false;
                    clonedCell.LockReason = null;
                    clonedCell.LockedBy = null;
                    await UpsertCellAsync(connection, transaction, clonedCell, cancellationToken);
                }
            }
        }

        return nextProductNodeId;
    }

    private static async Task<IReadOnlyList<HierarchyDepartmentRecord>> LoadHierarchyMappingsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var departments = new Dictionary<string, HierarchyDepartmentRecord>(StringComparer.OrdinalIgnoreCase);
        var classes = new Dictionary<(string DepartmentLabel, string ClassLabel), HierarchyClassRecord>();
        var subclasses = new Dictionary<(string DepartmentLabel, string ClassLabel), List<HierarchySubclassRecord>>();

        await using (var departmentCommand = connection.CreateCommand())
        {
            departmentCommand.Transaction = transaction;
            departmentCommand.CommandText = """
                select department_label,
                       lifecycle_state,
                       ramp_profile_code,
                       effective_from_time_period_id,
                       effective_to_time_period_id
                from hierarchy_departments_v2
                order by department_label;
                """;
            await using var reader = await departmentCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var record = new HierarchyDepartmentRecord(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetInt64(3),
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    []);
                departments[record.DepartmentLabel] = record;
            }
        }

        await using (var classCommand = connection.CreateCommand())
        {
            classCommand.Transaction = transaction;
            classCommand.CommandText = """
                select department_label,
                       class_label,
                       lifecycle_state,
                       ramp_profile_code,
                       effective_from_time_period_id,
                       effective_to_time_period_id
                from hierarchy_classes_v2
                order by department_label, class_label;
                """;
            await using var reader = await classCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var departmentLabel = reader.GetString(0);
                var record = new HierarchyClassRecord(
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    []);
                classes[(departmentLabel, record.ClassLabel)] = record;
            }
        }

        await using (var subclassCommand = connection.CreateCommand())
        {
            subclassCommand.Transaction = transaction;
            subclassCommand.CommandText = """
                select department_label,
                       class_label,
                       subclass_label,
                       lifecycle_state,
                       ramp_profile_code,
                       effective_from_time_period_id,
                       effective_to_time_period_id
                from hierarchy_subclasses_v2
                order by department_label, class_label, subclass_label;
                """;
            await using var reader = await subclassCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var key = (reader.GetString(0), reader.GetString(1));
                var list = subclasses.GetValueOrDefault(key);
                if (list is null)
                {
                    list = [];
                    subclasses[key] = list;
                }

                list.Add(new HierarchySubclassRecord(
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetInt64(6)));
            }
        }

        return departments.Values
            .OrderBy(record => record.DepartmentLabel, StringComparer.OrdinalIgnoreCase)
            .Select(department => department with
            {
                Classes = classes
                    .Where(entry => string.Equals(entry.Key.DepartmentLabel, department.DepartmentLabel, StringComparison.OrdinalIgnoreCase))
                    .OrderBy(entry => entry.Key.ClassLabel, StringComparer.OrdinalIgnoreCase)
                    .Select(entry => entry.Value with
                    {
                        Subclasses = (subclasses.GetValueOrDefault((department.DepartmentLabel, entry.Value.ClassLabel)) ?? [])
                            .OrderBy(value => value.SubclassLabel, StringComparer.OrdinalIgnoreCase)
                            .ToList()
                    })
                    .ToList()
            })
            .ToList();
    }

    private static async Task RebuildHierarchyMappingsAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        foreach (var tableName in new[] { "hierarchy_subclasses_v2", "hierarchy_classes_v2", "hierarchy_departments_v2", "hierarchy_subcategories", "hierarchy_categories" })
        {
            await using var deleteCommand = connection.CreateCommand();
            deleteCommand.Transaction = transaction;
            deleteCommand.CommandText = $"delete from {tableName};";
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
        foreach (var department in productNodes.Values.Where(node => node.Level == 1))
        {
            await UpsertHierarchyDepartmentInternalAsync(connection, transaction, department.Label, cancellationToken);
        }

        foreach (var classNode in productNodes.Values.Where(node => node.Level == 2))
        {
            var department = productNodes[classNode.ParentProductNodeId!.Value];
            await UpsertHierarchyClassInternalAsync(connection, transaction, department.Label, classNode.Label, cancellationToken);
        }

        foreach (var subclassNode in productNodes.Values.Where(node => node.Level == 3))
        {
            var classNode = productNodes[subclassNode.ParentProductNodeId!.Value];
            var departmentNode = productNodes[classNode.ParentProductNodeId!.Value];
            await UpsertHierarchySubclassInternal(connection, transaction, departmentNode.Label, classNode.Label, subclassNode.Label, cancellationToken);
        }
    }

    private static async Task UpsertHierarchyDepartmentInternalAsync(SqliteConnection connection, SqliteTransaction transaction, string departmentLabel, CancellationToken cancellationToken)
    {
        var normalizedLabel = departmentLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedLabel))
        {
            throw new InvalidOperationException("Department labels cannot be empty.");
        }

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into hierarchy_categories (category_label)
            values ($categoryLabel)
            on conflict (category_label) do nothing;
            """;
        command.Parameters.AddWithValue("$categoryLabel", normalizedLabel);
        await command.ExecuteNonQueryAsync(cancellationToken);

        await using var modernCommand = connection.CreateCommand();
        modernCommand.Transaction = transaction;
        modernCommand.CommandText = """
            insert into hierarchy_departments_v2 (
                department_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values ($departmentLabel, 'active', null, null, null)
            on conflict (department_label) do nothing;
            """;
        modernCommand.Parameters.AddWithValue("$departmentLabel", normalizedLabel);
        await modernCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertHierarchyClassInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string departmentLabel,
        string classLabel,
        CancellationToken cancellationToken)
    {
        var normalizedClass = classLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedClass))
        {
            throw new InvalidOperationException("Class labels cannot be empty.");
        }

        await UpsertHierarchyDepartmentInternalAsync(connection, transaction, departmentLabel, cancellationToken);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into hierarchy_subcategories (category_label, subcategory_label)
            values ($categoryLabel, $subcategoryLabel)
            on conflict (category_label, subcategory_label) do nothing;
            """;
        command.Parameters.AddWithValue("$categoryLabel", departmentLabel.Trim());
        command.Parameters.AddWithValue("$subcategoryLabel", normalizedClass);
        await command.ExecuteNonQueryAsync(cancellationToken);

        await using var modernCommand = connection.CreateCommand();
        modernCommand.Transaction = transaction;
        modernCommand.CommandText = """
            insert into hierarchy_classes_v2 (
                department_label,
                class_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values ($departmentLabel, $classLabel, 'active', null, null, null)
            on conflict (department_label, class_label) do nothing;
            """;
        modernCommand.Parameters.AddWithValue("$departmentLabel", departmentLabel.Trim());
        modernCommand.Parameters.AddWithValue("$classLabel", normalizedClass);
        await modernCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertHierarchySubclassInternal(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string departmentLabel,
        string classLabel,
        string subclassLabel,
        CancellationToken cancellationToken)
    {
        var normalizedSubclass = subclassLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedSubclass))
        {
            throw new InvalidOperationException("Subclass labels cannot be empty.");
        }

        await UpsertHierarchyClassInternalAsync(connection, transaction, departmentLabel, classLabel, cancellationToken);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into hierarchy_subclasses_v2 (
                department_label,
                class_label,
                subclass_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values ($departmentLabel, $classLabel, $subclassLabel, 'active', null, null, null)
            on conflict (department_label, class_label, subclass_label) do nothing;
            """;
        command.Parameters.AddWithValue("$departmentLabel", departmentLabel.Trim());
        command.Parameters.AddWithValue("$classLabel", classLabel.Trim());
        command.Parameters.AddWithValue("$subclassLabel", normalizedSubclass);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static bool IsEffectivelyLocked(
        PlanningCellCoordinate coordinate,
        IReadOnlyCollection<PlanningCell> scenarioCells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        return scenarioCells.Any(cell =>
            cell.IsLocked &&
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsAncestorOrSelf(productNodes, cell.Coordinate.ProductNodeId, coordinate.ProductNodeId) &&
            IsAncestorOrSelf(timePeriods, cell.Coordinate.TimePeriodId, coordinate.TimePeriodId));
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

    private static async Task SeedAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        var timePeriods = BuildTimePeriods();
        foreach (var timePeriod in timePeriods.Values)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
                values ($timePeriodId, $parentTimePeriodId, $label, $grain, $sortOrder);
                """;
            command.Parameters.AddWithValue("$timePeriodId", timePeriod.TimePeriodId);
            command.Parameters.AddWithValue("$parentTimePeriodId", (object?)timePeriod.ParentTimePeriodId ?? DBNull.Value);
            command.Parameters.AddWithValue("$label", timePeriod.Label);
            command.Parameters.AddWithValue("$grain", timePeriod.Grain);
            command.Parameters.AddWithValue("$sortOrder", timePeriod.SortOrder);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        var productNodes = BuildProductNodes();
        foreach (var productNode in productNodes.Values)
        {
            await InsertProductNodeAsync(connection, transaction, productNode, cancellationToken);
            await InitializeCellsForNodeAsync(connection, transaction, 1, productNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
        }

        foreach (var store in BuildStoreMetadata())
        {
            await UpsertStoreMetadataAsync(connection, transaction, store, cancellationToken);
        }

        var cells = new Dictionary<string, PlanningCell>();
        foreach (var measureId in SupportedMeasureIds)
        {
            foreach (var productNode in productNodes.Values)
            {
                foreach (var timePeriod in timePeriods.Values)
                {
                    var cell = new PlanningCell
                    {
                        Coordinate = new PlanningCellCoordinate(1, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId),
                        DerivedValue = 0m,
                        EffectiveValue = 0m,
                        RowVersion = 1,
                        CellKind = productNode.IsLeaf && timePeriod.Grain == "month" ? "leaf" : "calculated"
                    };
                    cells[cell.Coordinate.Key] = cell;
                }
            }
        }

        var revenueLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2111)] = new()
            {
                [202601] = 600m, [202602] = 750m, [202603] = 700m, [202604] = 710m,
                [202605] = 720m, [202606] = 735m, [202607] = 760m, [202608] = 770m,
                [202609] = 730m, [202610] = 690m, [202611] = 720m, [202612] = 780m
            },
            [(101, 2112)] = new()
            {
                [202601] = 250m, [202602] = 260m, [202603] = 255m, [202604] = 265m,
                [202605] = 270m, [202606] = 280m, [202607] = 290m, [202608] = 285m,
                [202609] = 275m, [202610] = 268m, [202611] = 272m, [202612] = 295m
            },
            [(101, 2121)] = new()
            {
                [202601] = 420m, [202602] = 430m, [202603] = 410m, [202604] = 425m,
                [202605] = 440m, [202606] = 450m, [202607] = 460m, [202608] = 470m,
                [202609] = 455m, [202610] = 440m, [202611] = 448m, [202612] = 475m
            },
            [(101, 2122)] = new()
            {
                [202601] = 390m, [202602] = 405m, [202603] = 398m, [202604] = 410m,
                [202605] = 420m, [202606] = 430m, [202607] = 438m, [202608] = 442m,
                [202609] = 435m, [202610] = 425m, [202611] = 430m, [202612] = 445m
            },
            [(101, 2211)] = new()
            {
                [202601] = 510m, [202602] = 525m, [202603] = 505m, [202604] = 515m,
                [202605] = 530m, [202606] = 540m, [202607] = 550m, [202608] = 565m,
                [202609] = 555m, [202610] = 542m, [202611] = 548m, [202612] = 572m
            },
            [(101, 2212)] = new()
            {
                [202601] = 300m, [202602] = 315m, [202603] = 308m, [202604] = 318m,
                [202605] = 324m, [202606] = 336m, [202607] = 344m, [202608] = 352m,
                [202609] = 348m, [202610] = 339m, [202611] = 342m, [202612] = 356m
            },
            [(102, 3111)] = new()
            {
                [202601] = 560m, [202602] = 610m, [202603] = 590m, [202604] = 602m,
                [202605] = 612m, [202606] = 625m, [202607] = 645m, [202608] = 652m,
                [202609] = 630m, [202610] = 608m, [202611] = 620m, [202612] = 660m
            },
            [(102, 3112)] = new()
            {
                [202601] = 240m, [202602] = 255m, [202603] = 248m, [202604] = 258m,
                [202605] = 262m, [202606] = 272m, [202607] = 280m, [202608] = 286m,
                [202609] = 278m, [202610] = 270m, [202611] = 275m, [202612] = 290m
            },
            [(102, 3211)] = new()
            {
                [202601] = 480m, [202602] = 495m, [202603] = 488m, [202604] = 500m,
                [202605] = 514m, [202606] = 522m, [202607] = 534m, [202608] = 548m,
                [202609] = 540m, [202610] = 528m, [202611] = 533m, [202612] = 552m
            },
            [(102, 3212)] = new()
            {
                [202601] = 285m, [202602] = 295m, [202603] = 292m, [202604] = 298m,
                [202605] = 306m, [202606] = 312m, [202607] = 320m, [202608] = 327m,
                [202609] = 322m, [202610] = 315m, [202611] = 318m, [202612] = 330m
            }
        };

        var quantityLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2111)] = new()
            {
                [202601] = 120m, [202602] = 150m, [202603] = 142m, [202604] = 144m,
                [202605] = 146m, [202606] = 149m, [202607] = 153m, [202608] = 155m,
                [202609] = 148m, [202610] = 140m, [202611] = 146m, [202612] = 158m
            },
            [(101, 2112)] = new()
            {
                [202601] = 55m, [202602] = 58m, [202603] = 57m, [202604] = 59m,
                [202605] = 60m, [202606] = 62m, [202607] = 64m, [202608] = 63m,
                [202609] = 61m, [202610] = 60m, [202611] = 61m, [202612] = 66m
            },
            [(101, 2121)] = new()
            {
                [202601] = 80m, [202602] = 84m, [202603] = 82m, [202604] = 85m,
                [202605] = 88m, [202606] = 90m, [202607] = 92m, [202608] = 94m,
                [202609] = 91m, [202610] = 88m, [202611] = 89m, [202612] = 96m
            },
            [(101, 2122)] = new()
            {
                [202601] = 76m, [202602] = 79m, [202603] = 78m, [202604] = 80m,
                [202605] = 82m, [202606] = 84m, [202607] = 85m, [202608] = 87m,
                [202609] = 86m, [202610] = 83m, [202611] = 84m, [202612] = 88m
            },
            [(101, 2211)] = new()
            {
                [202601] = 95m, [202602] = 98m, [202603] = 96m, [202604] = 98m,
                [202605] = 101m, [202606] = 103m, [202607] = 105m, [202608] = 108m,
                [202609] = 106m, [202610] = 104m, [202611] = 105m, [202612] = 109m
            },
            [(101, 2212)] = new()
            {
                [202601] = 61m, [202602] = 63m, [202603] = 62m, [202604] = 63m,
                [202605] = 64m, [202606] = 66m, [202607] = 67m, [202608] = 69m,
                [202609] = 68m, [202610] = 66m, [202611] = 67m, [202612] = 70m
            },
            [(102, 3111)] = new()
            {
                [202601] = 111m, [202602] = 121m, [202603] = 117m, [202604] = 119m,
                [202605] = 121m, [202606] = 123m, [202607] = 127m, [202608] = 128m,
                [202609] = 125m, [202610] = 121m, [202611] = 123m, [202612] = 131m
            },
            [(102, 3112)] = new()
            {
                [202601] = 52m, [202602] = 55m, [202603] = 53m, [202604] = 55m,
                [202605] = 56m, [202606] = 58m, [202607] = 60m, [202608] = 61m,
                [202609] = 59m, [202610] = 58m, [202611] = 58m, [202612] = 62m
            },
            [(102, 3211)] = new()
            {
                [202601] = 89m, [202602] = 91m, [202603] = 90m, [202604] = 92m,
                [202605] = 94m, [202606] = 95m, [202607] = 98m, [202608] = 100m,
                [202609] = 99m, [202610] = 97m, [202611] = 98m, [202612] = 102m
            },
            [(102, 3212)] = new()
            {
                [202601] = 58m, [202602] = 60m, [202603] = 59m, [202604] = 60m,
                [202605] = 62m, [202606] = 63m, [202607] = 65m, [202608] = 66m,
                [202609] = 65m, [202610] = 64m, [202611] = 64m, [202612] = 67m
            }
        };

        ApplyLeafSeedValues(cells, 1, revenueLeafValues);
        ApplyLeafSeedValues(cells, 2, quantityLeafValues);

        var lockedCoordinate = new PlanningCellCoordinate(1, 1, 101, 2111, 202602);
        cells[lockedCoordinate.Key].IsLocked = true;
        cells[lockedCoordinate.Key].LockReason = "Manager-held sample lock";
        cells[lockedCoordinate.Key].LockedBy = "demo.manager";

        RecalculateSeedTotals(
            1,
            1,
            cells.Where(entry => entry.Value.Coordinate.MeasureId == 1).ToDictionary(entry => entry.Key, entry => entry.Value),
            productNodes,
            timePeriods);
        RecalculateSeedTotals(
            1,
            2,
            cells.Where(entry => entry.Value.Coordinate.MeasureId == 2).ToDictionary(entry => entry.Key, entry => entry.Value),
            productNodes,
            timePeriods);

        await RebuildHierarchyMappingsAsync(connection, transaction, cancellationToken);

        foreach (var cell in cells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static void ApplyLeafSeedValues(
        IDictionary<string, PlanningCell> cells,
        long measureId,
        IReadOnlyDictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>> leafValues)
    {
        foreach (var (key, monthValues) in leafValues)
        {
            foreach (var monthValue in monthValues)
            {
                var coordinate = new PlanningCellCoordinate(1, measureId, key.StoreId, key.ProductNodeId, monthValue.Key);
                var cell = cells[coordinate.Key];
                cell.InputValue = monthValue.Value;
                cell.DerivedValue = monthValue.Value;
                cell.EffectiveValue = monthValue.Value;
                cell.RowVersion = 2;
                cell.CellKind = "input";
            }
        }
    }

    private static void SetSeedInputValue(PlanningCell cell, decimal value)
    {
        cell.InputValue = value;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.RowVersion = Math.Max(cell.RowVersion, 2);
        cell.CellKind = "input";
    }

    private static void SetSeedCalculatedValue(PlanningCell cell, decimal value, bool isLeafMonth)
    {
        cell.InputValue = null;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.RowVersion = Math.Max(cell.RowVersion, 2);
        cell.CellKind = isLeafMonth ? "calculated" : "calculated";
    }

    private static void RecalculateSeedTotals(
        long scenarioVersionId,
        long measureId,
        IDictionary<string, PlanningCell> cells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        var aggregateTimes = timePeriods.Values
            .Where(period => timePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId))
            .OrderBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var aggregateTime in aggregateTimes)
            {
                var childTimeIds = timePeriods.Values
                    .Where(period => period.ParentTimePeriodId == aggregateTime.TimePeriodId)
                    .Select(period => period.TimePeriodId)
                    .ToList();
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, leafNode.StoreId, leafNode.ProductNodeId, aggregateTime.TimePeriodId);
                var cell = cells[coordinate.Key];
                cell.DerivedValue = childTimeIds.Sum(childTimeId => cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    leafNode.StoreId,
                    leafNode.ProductNodeId,
                    childTimeId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
            }
        }

        foreach (var node in productNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            var childIds = productNodes.Values
                .Where(child => child.ParentProductNodeId == node.ProductNodeId)
                .Select(child => child.ProductNodeId)
                .ToList();

            foreach (var period in timePeriods.Values)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, period.TimePeriodId);
                var cell = cells[coordinate.Key];
                cell.DerivedValue = childIds.Sum(childId => cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    node.StoreId,
                    childId,
                    period.TimePeriodId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
            }
        }
    }

    private static void RecalculateSeedDerivedRateTotals(
        long scenarioVersionId,
        long measureId,
        IDictionary<string, PlanningCell> cells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        foreach (var productNode in productNodes.Values)
        {
            foreach (var timePeriod in timePeriods.Values)
            {
                var rateCell = cells[new PlanningCellCoordinate(scenarioVersionId, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key];
                var quantity = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;
                var revenue = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;
                var totalCosts = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;

                var value = measureId switch
                {
                    PlanningMeasures.AverageSellingPrice => quantity > 0m ? PlanningMath.NormalizeAsp(revenue / quantity) : 1.00m,
                    PlanningMeasures.UnitCost => quantity > 0m ? PlanningMath.NormalizeUnitCost(totalCosts / quantity) : 0m,
                    PlanningMeasures.GrossProfitPercent => PlanningMath.CalculateGrossProfitPercent(
                        quantity > 0m ? revenue / quantity : 1.00m,
                        quantity > 0m ? totalCosts / quantity : 0m),
                    _ => 0m
                };

                var isLeafMonth = productNode.IsLeaf && string.Equals(timePeriod.Grain, "month", StringComparison.OrdinalIgnoreCase);
                if (isLeafMonth && measureId is PlanningMeasures.AverageSellingPrice or PlanningMeasures.UnitCost)
                {
                    SetSeedInputValue(rateCell, value);
                }
                else
                {
                    SetSeedCalculatedValue(rateCell, value, isLeafMonth);
                }
            }
        }
    }

    private static (string LifecycleState, string? RampProfileCode, long? EffectiveFromTimePeriodId, long? EffectiveToTimePeriodId) ResolveNodeMetadata(
        ProductNode node,
        IReadOnlyDictionary<long, StoreNodeMetadata> stores,
        IReadOnlyList<HierarchyDepartmentRecord> hierarchyMappings)
    {
        if (node.NodeKind == "store" && stores.TryGetValue(node.StoreId, out var storeMetadata))
        {
            return (storeMetadata.LifecycleState, storeMetadata.RampProfileCode, storeMetadata.EffectiveFromTimePeriodId, storeMetadata.EffectiveToTimePeriodId);
        }

        var departmentLabel = node.Path.Length > 1 ? node.Path[1] : null;
        if (departmentLabel is null)
        {
            return (node.LifecycleState, node.RampProfileCode, node.EffectiveFromTimePeriodId, node.EffectiveToTimePeriodId);
        }

        var department = hierarchyMappings.FirstOrDefault(entry => string.Equals(entry.DepartmentLabel, departmentLabel, StringComparison.OrdinalIgnoreCase));
        if (department is null)
        {
            return (node.LifecycleState, node.RampProfileCode, node.EffectiveFromTimePeriodId, node.EffectiveToTimePeriodId);
        }

        if (node.NodeKind == "department")
        {
            return (department.LifecycleState, department.RampProfileCode, department.EffectiveFromTimePeriodId, department.EffectiveToTimePeriodId);
        }

        var classLabel = node.Path.Length > 2 ? node.Path[2] : null;
        var classRecord = department.Classes.FirstOrDefault(entry => string.Equals(entry.ClassLabel, classLabel, StringComparison.OrdinalIgnoreCase));
        if (classRecord is null)
        {
            return (node.LifecycleState, node.RampProfileCode, node.EffectiveFromTimePeriodId, node.EffectiveToTimePeriodId);
        }

        if (node.NodeKind == "class")
        {
            return (classRecord.LifecycleState, classRecord.RampProfileCode, classRecord.EffectiveFromTimePeriodId, classRecord.EffectiveToTimePeriodId);
        }

        var subclassLabel = node.Path.Length > 3 ? node.Path[3] : null;
        var subclassRecord = classRecord.Subclasses.FirstOrDefault(entry => string.Equals(entry.SubclassLabel, subclassLabel, StringComparison.OrdinalIgnoreCase));
        return subclassRecord is null
            ? (node.LifecycleState, node.RampProfileCode, node.EffectiveFromTimePeriodId, node.EffectiveToTimePeriodId)
            : (subclassRecord.LifecycleState, subclassRecord.RampProfileCode, subclassRecord.EffectiveFromTimePeriodId, subclassRecord.EffectiveToTimePeriodId);
    }

    private static string DeriveNodeKind(int level, bool isLeaf)
    {
        if (level <= 0)
        {
            return "store";
        }

        return level switch
        {
            1 => "department",
            2 when isLeaf => "subclass",
            2 => "class",
            3 => "subclass",
            _ => isLeaf ? "subclass" : "class"
        };
    }

    private static Dictionary<long, ProductNode> BuildProductNodes()
    {
        return new List<ProductNode>
        {
            new(2000, 101, null, "Store A", 0, new[] { "Store A" }, false, "store", "active", null, null, null),
            new(2100, 101, 2000, "Beverages", 1, new[] { "Store A", "Beverages" }, false, "department", "active", "new-line-ramp", null, null),
            new(2110, 101, 2100, "Soft Drinks", 2, new[] { "Store A", "Beverages", "Soft Drinks" }, false, "class", "active", null, null, null),
            new(2111, 101, 2110, "Cola", 3, new[] { "Store A", "Beverages", "Soft Drinks", "Cola" }, true, "subclass", "active", "standard-ramp", null, null),
            new(2112, 101, 2110, "Sparkling Fruit", 3, new[] { "Store A", "Beverages", "Soft Drinks", "Sparkling Fruit" }, true, "subclass", "active", "standard-ramp", null, null),
            new(2120, 101, 2100, "Tea", 2, new[] { "Store A", "Beverages", "Tea" }, false, "class", "active", null, null, null),
            new(2121, 101, 2120, "Green Tea", 3, new[] { "Store A", "Beverages", "Tea", "Green Tea" }, true, "subclass", "active", null, null, null),
            new(2122, 101, 2120, "Milk Tea", 3, new[] { "Store A", "Beverages", "Tea", "Milk Tea" }, true, "subclass", "active", null, null, null),
            new(2200, 101, 2000, "Snacks", 1, new[] { "Store A", "Snacks" }, false, "department", "active", null, null, null),
            new(2210, 101, 2200, "Chips", 2, new[] { "Store A", "Snacks", "Chips" }, false, "class", "active", null, null, null),
            new(2211, 101, 2210, "Potato Chips", 3, new[] { "Store A", "Snacks", "Chips", "Potato Chips" }, true, "subclass", "active", null, null, null),
            new(2212, 101, 2210, "Corn Chips", 3, new[] { "Store A", "Snacks", "Chips", "Corn Chips" }, true, "subclass", "active", null, null, null),
            new(3000, 102, null, "Store B", 0, new[] { "Store B" }, false, "store", "active", null, null, null),
            new(3100, 102, 3000, "Beverages", 1, new[] { "Store B", "Beverages" }, false, "department", "active", null, null, null),
            new(3110, 102, 3100, "Soft Drinks", 2, new[] { "Store B", "Beverages", "Soft Drinks" }, false, "class", "active", null, null, null),
            new(3111, 102, 3110, "Cola", 3, new[] { "Store B", "Beverages", "Soft Drinks", "Cola" }, true, "subclass", "active", null, null, null),
            new(3112, 102, 3110, "Sparkling Fruit", 3, new[] { "Store B", "Beverages", "Soft Drinks", "Sparkling Fruit" }, true, "subclass", "active", null, null, null),
            new(3200, 102, 3000, "Snacks", 1, new[] { "Store B", "Snacks" }, false, "department", "active", null, null, null),
            new(3210, 102, 3200, "Chips", 2, new[] { "Store B", "Snacks", "Chips" }, false, "class", "active", null, null, null),
            new(3211, 102, 3210, "Potato Chips", 3, new[] { "Store B", "Snacks", "Chips", "Potato Chips" }, true, "subclass", "active", null, null, null),
            new(3212, 102, 3210, "Corn Chips", 3, new[] { "Store B", "Snacks", "Chips", "Corn Chips" }, true, "subclass", "active", null, null, null)
        }.ToDictionary(node => node.ProductNodeId);
    }

    private static IReadOnlyList<StoreNodeMetadata> BuildStoreMetadata()
    {
        return
        [
            new StoreNodeMetadata(101, "Store A", "Baby Mart", "Central", "active", "new-store-ramp", null, null),
            new StoreNodeMetadata(102, "Store B", "Baby Mall", "South", "active", "new-store-ramp", null, null)
        ];
    }

    private static Dictionary<long, TimePeriodNode> BuildTimePeriods()
    {
        return new[] { 2026, 2027 }
            .SelectMany(year => BuildYearPeriods(year).Values)
            .ToDictionary(period => period.TimePeriodId);
    }

    private static Dictionary<long, TimePeriodNode> BuildYearPeriods(int fiscalYear)
    {
        var yearId = fiscalYear * 100;
        var fiscalSuffix = fiscalYear % 100;
        var months = new[]
        {
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        };

        var periods = new Dictionary<long, TimePeriodNode>
        {
            [yearId] = new(yearId, null, $"FY{fiscalSuffix:00}", "year", (fiscalYear - 2000) * 100)
        };

        for (var monthIndex = 0; monthIndex < months.Length; monthIndex += 1)
        {
            periods[yearId + monthIndex + 1] = new(
                yearId + monthIndex + 1,
                yearId,
                months[monthIndex],
                "month",
                ((fiscalYear - 2000) * 100) + monthIndex + 1);
        }

        return periods;
    }
}
