using System.Globalization;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository
{
    private async Task<PlanningMetadataSnapshot> GetMetadataDirectAsync(CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(
            (connection, transaction, ct) => GetMetadataCachedDirectAsync(connection, transaction, ct),
            cancellationToken);
    }

    private async Task<PlanningMetadataSnapshot> GetMetadataCachedDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var cached = GetCachedMetadataSnapshot();
        if (cached is not null)
        {
            return cached;
        }

        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        var stores = await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken);
        var snapshot = new PlanningMetadataSnapshot(productNodes, timePeriods, stores);
        SetCachedMetadataSnapshot(snapshot);
        return snapshot;
    }

    private async Task<IReadOnlyList<StoreNodeMetadata>> GetStoresDirectAsync(CancellationToken cancellationToken)
    {
        var cached = GetCachedStoreList();
        if (cached is not null)
        {
            return cached;
        }

        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var stores = (await LoadStoreMetadataDirectAsync(connection, transaction, ct)).Values
                .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
                .ToList();
            lock (_readCacheGate)
            {
                _storeListCache = stores;
            }

            return stores;
        }, cancellationToken);
    }

    private async Task<IReadOnlyDictionary<long, long>> GetStoreRootProductNodeIdsDirectAsync(CancellationToken cancellationToken)
    {
        var cached = GetCachedStoreRoots();
        if (cached is not null)
        {
            return cached;
        }

        var metadata = GetCachedMetadataSnapshot();
        if (metadata is not null)
        {
            var roots = metadata.ProductNodes.Values
                .Where(node => node.Level == 0)
                .GroupBy(node => node.StoreId)
                .ToDictionary(
                    group => group.Key,
                    group => group.OrderBy(node => node.ProductNodeId).First().ProductNodeId);
            lock (_readCacheGate)
            {
                _storeRootProductNodeIdsCache = roots;
            }

            return roots;
        }

        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var roots = new Dictionary<long, long>();
            await using var command = new NpgsqlCommand(
            """
            select distinct on (store_id)
                   store_id,
                   product_node_id
            from product_nodes
            where level = 0
            order by store_id, product_node_id;
            """,
            connection,
            transaction);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                roots[reader.GetInt64(0)] = reader.GetInt64(1);
            }

            lock (_readCacheGate)
            {
                _storeRootProductNodeIdsCache = roots;
            }

            return roots;
        }, cancellationToken);
    }

    private async Task<IReadOnlyList<PlanningCell>> GetCellsDirectAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken)
    {
        var coordinateList = coordinates
            .DistinctBy(coordinate => coordinate.Key)
            .ToList();
        if (coordinateList.Count == 0)
        {
            return [];
        }

        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var cells = new List<PlanningCell>();
            const string sql = """
                select p.scenario_version_id,
                       p.measure_id,
                       p.store_id,
                       p.product_node_id,
                       p.time_period_id,
                       p.input_value,
                       p.override_value,
                       p.is_system_generated_override,
                       p.derived_value,
                       p.effective_value,
                       p.growth_factor,
                       p.is_locked,
                       p.lock_reason,
                       p.locked_by,
                       p.row_version,
                       p.cell_kind
                from planning_cells p
                inner join unnest(
                    @scenarioVersionIds,
                    @measureIds,
                    @storeIds,
                    @productNodeIds,
                    @timePeriodIds)
                    as requested(
                        scenario_version_id,
                        measure_id,
                        store_id,
                        product_node_id,
                        time_period_id)
                    on requested.scenario_version_id = p.scenario_version_id
                   and requested.measure_id = p.measure_id
                   and requested.store_id = p.store_id
                   and requested.product_node_id = p.product_node_id
                   and requested.time_period_id = p.time_period_id;
                """;

            foreach (var coordinateChunk in coordinateList.Chunk(BulkWriteChunkSize))
            {
                await using var command = new NpgsqlCommand(sql, connection, transaction)
                {
                    CommandTimeout = 300
                };
                command.Parameters.Add(CreateArrayParameter("@scenarioVersionIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.ScenarioVersionId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.MeasureId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.StoreId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.ProductNodeId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.TimePeriodId).ToArray()));

                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    cells.Add(ReadPlanningCellDirect(reader));
                }
            }

            return cells.Select(cell => cell.Clone()).ToList();
        }, cancellationToken);
    }

    private async Task<PlanningCell?> GetCellDirectAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            await using var command = new NpgsqlCommand(
            """
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
            where scenario_version_id = @scenarioVersionId
              and measure_id = @measureId
              and store_id = @storeId
              and product_node_id = @productNodeId
              and time_period_id = @timePeriodId;
            """,
            connection);
            command.Transaction = transaction;
            command.Parameters.AddWithValue("@scenarioVersionId", coordinate.ScenarioVersionId);
            command.Parameters.AddWithValue("@measureId", coordinate.MeasureId);
            command.Parameters.AddWithValue("@storeId", coordinate.StoreId);
            command.Parameters.AddWithValue("@productNodeId", coordinate.ProductNodeId);
            command.Parameters.AddWithValue("@timePeriodId", coordinate.TimePeriodId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return null;
            }

            return ReadPlanningCellDirect(reader);
        }, cancellationToken);
    }

    private async Task<IReadOnlyList<PlanningCell>> GetScenarioCellsDirectAsync(long scenarioVersionId, CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(
            (connection, transaction, ct) => LoadScenarioCellsDirectAsync(connection, transaction, scenarioVersionId, ct),
            cancellationToken);
    }

    private async Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityDirectAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            await using var command = new NpgsqlCommand(
            """
            with retained_history as (
                select is_undone
                from planning_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select
                coalesce(sum(case when is_undone = 0 then 1 else 0 end), 0),
                coalesce(sum(case when is_undone = 1 then 1 else 0 end), 0)
            from retained_history;
            """,
            connection);
            command.Transaction = transaction;
            command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
            command.Parameters.AddWithValue("@userId", userId);
            command.Parameters.AddWithValue("@limit", limit);
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return new PlanningUndoRedoAvailability(false, false, 0, 0, limit);
            }

            var undoDepth = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
            var redoDepth = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            return new PlanningUndoRedoAvailability(undoDepth > 0, redoDepth > 0, undoDepth, redoDepth, limit);
        }, cancellationToken);
    }

    private async Task<GridSliceResponse> GetGridSliceDirectAsync(
        long scenarioVersionId,
        long? selectedStoreId,
        string? selectedDepartmentLabel,
        IReadOnlyCollection<long>? expandedProductNodeIds,
        bool expandAllBranches,
        CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var metadata = await GetMetadataCachedDirectAsync(connection, transaction, ct);
            var productNodes = metadata.ProductNodes;
            var timePeriods = metadata.TimePeriods;
            var stores = metadata.Stores;
            var hierarchyMappings = await GetHierarchyMappingsCachedDirectAsync(connection, transaction, ct);

            var expandedNodeSet = expandedProductNodeIds?.ToHashSet() ?? [];
            var visibleNodes = productNodes.Values
                .Where(node => ShouldIncludeGridNodeDirect(node, selectedStoreId, selectedDepartmentLabel, expandedNodeSet, productNodes, expandAllBranches))
                .ToList();

            var scenarioCells = await LoadScenarioCellsForNodesDirectAsync(
                connection,
                transaction,
                scenarioVersionId,
                visibleNodes.Select(node => node.ProductNodeId).ToArray(),
                ct);

            var rows = BuildGridRowsDirect(visibleNodes, scenarioCells, productNodes, timePeriods, stores, hierarchyMappings);
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
        }, cancellationToken);
    }

    private async Task<GridBranchResponse> GetGridBranchRowsDirectAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken)
    {
        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var metadata = await GetMetadataCachedDirectAsync(connection, transaction, ct);
            var productNodes = metadata.ProductNodes;
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

            var timePeriods = metadata.TimePeriods;
            var stores = metadata.Stores;
            var hierarchyMappings = await GetHierarchyMappingsCachedDirectAsync(connection, transaction, ct);

            var relevantNodeIds = children
                .Select(node => node.ProductNodeId)
                .Concat(GetAncestorProductNodeIdsDirect(parentNode, productNodes))
                .Distinct()
                .ToArray();

            var scenarioCells = await LoadScenarioCellsForNodesDirectAsync(
                connection,
                transaction,
                scenarioVersionId,
                relevantNodeIds,
                ct);

            var rows = BuildGridRowsDirect(children, scenarioCells, productNodes, timePeriods, stores, hierarchyMappings);
            return new GridBranchResponse(scenarioVersionId, parentProductNodeId, rows);
        }, cancellationToken);
    }

    private async Task<ProductNode?> FindProductNodeByPathDirectAsync(string[] path, CancellationToken cancellationToken)
    {
        var metadata = await GetMetadataDirectAsync(cancellationToken);
        return metadata.ProductNodes.Values.FirstOrDefault(candidate =>
            candidate.Path.Length == path.Length &&
            candidate.Path.Zip(path, (left, right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase)).All(match => match));
    }

    private async Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsCachedDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var cached = GetCachedHierarchyMappings();
        if (cached is not null)
        {
            return cached;
        }

        var mappings = await LoadHierarchyMappingsDirectAsync(connection, transaction, cancellationToken);
        SetCachedHierarchyMappings(mappings);
        return mappings;
    }

    private static async Task<Dictionary<long, ProductNode>> LoadProductNodesDirectAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, ProductNode>();
        await using var command = new NpgsqlCommand(
            """
            select product_node_id,
                   store_id,
                   parent_product_node_id,
                   label,
                   level,
                   path_json,
                   is_leaf,
                   node_kind,
                   lifecycle_state,
                   ramp_profile_code,
                   effective_from_time_period_id,
                   effective_to_time_period_id
            from product_nodes;
            """,
            connection,
            transaction);
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
                Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture) == 1,
                reader.IsDBNull(7) ? DeriveNodeKindDirect(reader.GetInt32(4), Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture) == 1) : reader.GetString(7),
                reader.IsDBNull(8) ? "active" : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt64(11));
            result[node.ProductNodeId] = node;
        }

        return result;
    }

    private static async Task<Dictionary<long, StoreNodeMetadata>> LoadStoreMetadataDirectAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, StoreNodeMetadata>();
        await using var command = new NpgsqlCommand(
            """
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
            """,
            connection,
            transaction);
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
                reader.IsDBNull(10) ? null : ReadDecimalDirect(reader, 10),
                reader.IsDBNull(11) ? null : ReadDecimalDirect(reader, 11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14),
                reader.IsDBNull(15) ? null : reader.GetString(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? null : reader.GetString(17),
                reader.IsDBNull(18) ? null : ReadDecimalDirect(reader, 18),
                reader.IsDBNull(19) ? null : ReadDecimalDirect(reader, 19),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21),
                reader.IsDBNull(22) ? null : ReadDecimalDirect(reader, 22),
                Convert.ToInt64(reader.GetValue(35), CultureInfo.InvariantCulture) == 1,
                reader.IsDBNull(23) ? null : reader.GetString(23),
                reader.IsDBNull(24) ? null : ReadDecimalDirect(reader, 24),
                reader.IsDBNull(25) ? null : reader.GetString(25),
                reader.IsDBNull(26) ? null : reader.GetString(26),
                reader.IsDBNull(27) ? null : reader.GetString(27),
                reader.IsDBNull(28) ? null : reader.GetString(28),
                !reader.IsDBNull(29) && Convert.ToInt64(reader.GetValue(29), CultureInfo.InvariantCulture) == 1,
                !reader.IsDBNull(30) && Convert.ToInt64(reader.GetValue(30), CultureInfo.InvariantCulture) == 1,
                reader.IsDBNull(31) ? null : reader.GetString(31),
                reader.IsDBNull(32) ? null : reader.GetString(32),
                reader.IsDBNull(33) ? null : reader.GetString(33),
                reader.IsDBNull(34) ? null : reader.GetString(34));
            result[metadata.StoreId] = metadata;
        }

        return result;
    }

    private static async Task<Dictionary<long, TimePeriodNode>> LoadTimePeriodsDirectAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, TimePeriodNode>();
        await using var command = new NpgsqlCommand(
            """
            select time_period_id, parent_time_period_id, label, grain, sort_order
            from time_periods;
            """,
            connection,
            transaction);
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

    private static async Task<List<PlanningCell>> LoadScenarioCellsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long scenarioVersionId,
        CancellationToken cancellationToken)
    {
        var result = new List<PlanningCell>();
        await using var command = new NpgsqlCommand(
            """
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
            where scenario_version_id = @scenarioVersionId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadPlanningCellDirect(reader));
        }

        return result;
    }

    private static async Task<List<PlanningCell>> LoadScenarioCellsForNodesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        long scenarioVersionId,
        IReadOnlyList<long> productNodeIds,
        CancellationToken cancellationToken)
    {
        if (productNodeIds.Count == 0)
        {
            return [];
        }

        var parameterNames = productNodeIds.Select((_, index) => $"@productNodeId{index}").ToArray();
        var result = new List<PlanningCell>();
        await using var command = new NpgsqlCommand(
            $"""
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
            where scenario_version_id = @scenarioVersionId
              and product_node_id in ({string.Join(", ", parameterNames)});
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        for (var index = 0; index < productNodeIds.Count; index += 1)
        {
            command.Parameters.AddWithValue(parameterNames[index], productNodeIds[index]);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadPlanningCellDirect(reader));
        }

        return result;
    }

    private static async Task<IReadOnlyList<HierarchyDepartmentRecord>> LoadHierarchyMappingsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var departments = new Dictionary<string, HierarchyDepartmentRecord>(StringComparer.OrdinalIgnoreCase);
        var classes = new Dictionary<(string DepartmentLabel, string ClassLabel), HierarchyClassRecord>();
        var subclasses = new Dictionary<(string DepartmentLabel, string ClassLabel), List<HierarchySubclassRecord>>();

        await using (var departmentCommand = new NpgsqlCommand(
            """
            select department_label,
                   lifecycle_state,
                   ramp_profile_code,
                   effective_from_time_period_id,
                   effective_to_time_period_id
            from hierarchy_departments_v2
            order by department_label;
            """,
            connection,
            transaction))
        await using (var reader = await departmentCommand.ExecuteReaderAsync(cancellationToken))
        {
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

        await using (var classCommand = new NpgsqlCommand(
            """
            select department_label,
                   class_label,
                   lifecycle_state,
                   ramp_profile_code,
                   effective_from_time_period_id,
                   effective_to_time_period_id
            from hierarchy_classes_v2
            order by department_label, class_label;
            """,
            connection,
            transaction))
        await using (var reader = await classCommand.ExecuteReaderAsync(cancellationToken))
        {
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

        await using (var subclassCommand = new NpgsqlCommand(
            """
            select department_label,
                   class_label,
                   subclass_label,
                   lifecycle_state,
                   ramp_profile_code,
                   effective_from_time_period_id,
                   effective_to_time_period_id
            from hierarchy_subclasses_v2
            order by department_label, class_label, subclass_label;
            """,
            connection,
            transaction))
        await using (var reader = await subclassCommand.ExecuteReaderAsync(cancellationToken))
        {
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

    private static PlanningCell ReadPlanningCellDirect(NpgsqlDataReader reader)
    {
        return new PlanningCell
        {
            Coordinate = new PlanningCellCoordinate(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4)),
            InputValue = reader.IsDBNull(5) ? null : ReadDecimalDirect(reader, 5),
            OverrideValue = reader.IsDBNull(6) ? null : ReadDecimalDirect(reader, 6),
            IsSystemGeneratedOverride = Convert.ToInt64(reader.GetValue(7), CultureInfo.InvariantCulture) == 1,
            DerivedValue = ReadDecimalDirect(reader, 8),
            EffectiveValue = ReadDecimalDirect(reader, 9),
            GrowthFactor = reader.IsDBNull(10) ? 1.0m : ReadDecimalDirect(reader, 10),
            IsLocked = Convert.ToInt64(reader.GetValue(11), CultureInfo.InvariantCulture) == 1,
            LockReason = reader.IsDBNull(12) ? null : reader.GetString(12),
            LockedBy = reader.IsDBNull(13) ? null : reader.GetString(13),
            RowVersion = reader.GetInt64(14),
            CellKind = reader.GetString(15)
        };
    }

    private static decimal ReadDecimalDirect(NpgsqlDataReader reader, int ordinal)
    {
        return Convert.ToDecimal(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static bool ShouldIncludeGridNodeDirect(
        ProductNode node,
        long? selectedStoreId,
        string? selectedDepartmentLabel,
        IReadOnlySet<long> expandedProductNodeIds,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        bool expandAllBranches)
    {
        if (node.Level == 0)
        {
            if (!string.IsNullOrWhiteSpace(selectedDepartmentLabel))
            {
                return false;
            }

            return selectedStoreId is null || node.StoreId == selectedStoreId.Value;
        }

        if (!string.IsNullOrWhiteSpace(selectedDepartmentLabel))
        {
            if (node.Level == 1)
            {
                return true;
            }

            if (!string.Equals(GetDepartmentLabelDirect(node), selectedDepartmentLabel, StringComparison.OrdinalIgnoreCase))
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

    private static string GetDepartmentLabelDirect(ProductNode node)
    {
        if (node.Level <= 1)
        {
            return node.Label;
        }

        return node.Path.ElementAtOrDefault(1) ?? node.Label;
    }

    private static IEnumerable<long> GetAncestorProductNodeIdsDirect(ProductNode node, IReadOnlyDictionary<long, ProductNode> productNodes)
    {
        var currentParentId = node.ParentProductNodeId;
        while (currentParentId is long parentId && productNodes.TryGetValue(parentId, out var parentNode))
        {
            yield return parentId;
            currentParentId = parentNode.ParentProductNodeId;
        }
    }

    private static IReadOnlyList<GridRowDto> BuildGridRowsDirect(
        IReadOnlyList<ProductNode> nodes,
        IReadOnlyCollection<PlanningCell> scenarioCells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        IReadOnlyDictionary<long, StoreNodeMetadata> stores,
        IReadOnlyList<HierarchyDepartmentRecord> hierarchyMappings)
    {
        var lockedCells = scenarioCells.Where(cell => cell.IsLocked).ToList();
        var cellsByNode = scenarioCells
            .GroupBy(cell => (cell.Coordinate.StoreId, cell.Coordinate.ProductNodeId))
            .ToDictionary(group => group.Key, group => group.ToList());

        return nodes
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .Select(node =>
            {
                var resolvedMetadata = ResolveNodeMetadataDirect(node, stores, hierarchyMappings);
                var nodeCells = cellsByNode.GetValueOrDefault((node.StoreId, node.ProductNodeId)) ?? [];
                var cells = nodeCells
                    .GroupBy(cell => cell.Coordinate.TimePeriodId)
                    .ToDictionary(
                        group => group.Key,
                        group => new GridPeriodCellDto(
                            group.ToDictionary(
                                cell => cell.Coordinate.MeasureId,
                                cell => new GridCellDto(
                                    cell.EffectiveValue,
                                    cell.GrowthFactor,
                                    IsEffectivelyLockedDirect(cell.Coordinate, lockedCells, productNodes, timePeriods),
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

    private static bool IsEffectivelyLockedDirect(
        PlanningCellCoordinate coordinate,
        IReadOnlyCollection<PlanningCell> scenarioCells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        return scenarioCells.Any(cell =>
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsAncestorOrSelfDirect(productNodes, cell.Coordinate.ProductNodeId, coordinate.ProductNodeId) &&
            IsAncestorOrSelfDirect(timePeriods, cell.Coordinate.TimePeriodId, coordinate.TimePeriodId));
    }

    private static bool IsAncestorOrSelfDirect(IReadOnlyDictionary<long, ProductNode> nodes, long ancestorId, long descendantId)
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

    private static bool IsAncestorOrSelfDirect(IReadOnlyDictionary<long, TimePeriodNode> nodes, long ancestorId, long descendantId)
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

    private static (string LifecycleState, string? RampProfileCode, long? EffectiveFromTimePeriodId, long? EffectiveToTimePeriodId) ResolveNodeMetadataDirect(
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

    private static string DeriveNodeKindDirect(int level, bool isLeaf)
    {
        return level switch
        {
            0 => "store",
            1 => "department",
            2 => "class",
            _ => isLeaf ? "subclass" : "subclass"
        };
    }
}
