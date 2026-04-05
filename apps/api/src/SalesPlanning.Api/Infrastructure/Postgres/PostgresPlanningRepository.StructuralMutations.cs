using System.Globalization;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresPlanningRepository
{
    private static readonly IReadOnlyList<long> SupportedMeasureIdsDirect = PlanningMeasures.SupportedMeasureIds;

    private Task<ProductNode> AddRowDirectAsync(AddRowRequest request, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => AddRowInternalDirectAsync(connection, transaction, request, ct),
            cancellationToken);

    private Task<int> DeleteRowDirectAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => DeleteRowInternalDirectAsync(connection, transaction, scenarioVersionId, productNodeId, ct),
            cancellationToken);

    private Task<int> DeleteYearDirectAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => DeleteYearInternalDirectAsync(connection, transaction, scenarioVersionId, yearTimePeriodId, ct),
            cancellationToken);

    private Task EnsureYearDirectAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => EnsureYearInternalDirectAsync(connection, transaction, scenarioVersionId, fiscalYear, ct),
            cancellationToken);

    private static async Task<ProductNode> AddRowInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        AddRowRequest request,
        CancellationToken cancellationToken)
    {
        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        var stores = await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken);
        var normalizedLevel = request.Level.Trim().ToLowerInvariant();
        var nextProductNodeId = productNodes.Keys.DefaultIfEmpty(3000L).Max();
        var nextStoreId = productNodes.Values.Select(node => node.StoreId).DefaultIfEmpty(200L).Max();
        var label = request.Label.Trim();
        if (string.IsNullOrWhiteSpace(label))
        {
            throw new InvalidOperationException("Row label is required.");
        }

        ProductNode node;
        switch (normalizedLevel)
        {
            case "store":
            {
                var storeId = ++nextStoreId;
                var productNodeId = ++nextProductNodeId;
                node = new ProductNode(
                    productNodeId,
                    storeId,
                    null,
                    label,
                    0,
                    [label],
                    false,
                    "store",
                    "active",
                    request.ClusterLabel is null && request.RegionLabel is null ? "new-store-ramp" : null,
                    null,
                    null);
                await InsertProductNodeDirectAsync(connection, transaction, node, cancellationToken);
                await UpsertStoreMetadataDirectAsync(
                    connection,
                    transaction,
                    new StoreNodeMetadata(
                        storeId,
                        label,
                        request.ClusterLabel?.Trim() ?? "Unassigned Cluster",
                        request.RegionLabel?.Trim() ?? "Unassigned Region",
                        "active",
                        "new-store-ramp",
                        null,
                        null,
                        BuildGeneratedStoreCodeDirect(label, stores),
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
                        true),
                    cancellationToken);
                await InitializeCellsForNodeDirectAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
                if (request.CopyFromStoreId is not null)
                {
                    var cells = await LoadScenarioCellsDirectAsync(connection, transaction, request.ScenarioVersionId, cancellationToken);
                    nextProductNodeId = await CloneStoreHierarchyAndDataDirectAsync(
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
                var parent = GetRequiredNodeDirect(productNodes, request.ParentProductNodeId, 0, "department");
                node = new ProductNode(
                    ++nextProductNodeId,
                    parent.StoreId,
                    parent.ProductNodeId,
                    label,
                    1,
                    parent.Path.Append(label).ToArray(),
                    false,
                    "department",
                    "active",
                    null,
                    null,
                    null);
                await InsertProductNodeDirectAsync(connection, transaction, node, cancellationToken);
                await InitializeCellsForNodeDirectAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
                await UpsertHierarchyDepartmentInternalDirectAsync(connection, transaction, node.Label, cancellationToken);
                break;
            }
            case "subcategory":
            case "class":
            {
                var parent = GetRequiredNodeDirect(productNodes, request.ParentProductNodeId, 1, "class");
                if (parent.IsLeaf)
                {
                    parent = parent with { IsLeaf = false };
                    await UpdateProductNodeDirectAsync(connection, transaction, parent, cancellationToken);
                    productNodes[parent.ProductNodeId] = parent;
                }

                node = new ProductNode(
                    ++nextProductNodeId,
                    parent.StoreId,
                    parent.ProductNodeId,
                    label,
                    2,
                    parent.Path.Append(label).ToArray(),
                    false,
                    "class",
                    "active",
                    null,
                    null,
                    null);
                await InsertProductNodeDirectAsync(connection, transaction, node, cancellationToken);
                await InitializeCellsForNodeDirectAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
                await UpsertHierarchyClassInternalDirectAsync(connection, transaction, parent.Label, node.Label, cancellationToken);
                break;
            }
            case "subclass":
            {
                var parent = GetRequiredNodeDirect(productNodes, request.ParentProductNodeId, 2, "subclass");
                if (parent.IsLeaf)
                {
                    parent = parent with { IsLeaf = false };
                    await UpdateProductNodeDirectAsync(connection, transaction, parent, cancellationToken);
                    productNodes[parent.ProductNodeId] = parent;
                }

                node = new ProductNode(
                    ++nextProductNodeId,
                    parent.StoreId,
                    parent.ProductNodeId,
                    label,
                    3,
                    parent.Path.Append(label).ToArray(),
                    true,
                    "subclass",
                    "active",
                    null,
                    null,
                    null);
                await InsertProductNodeDirectAsync(connection, transaction, node, cancellationToken);
                await InitializeCellsForNodeDirectAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
                var classNode = parent;
                var departmentNode = productNodes[classNode.ParentProductNodeId!.Value];
                await UpsertHierarchySubclassInternalDirectAsync(connection, transaction, departmentNode.Label, classNode.Label, node.Label, cancellationToken);
                break;
            }
            default:
                throw new InvalidOperationException($"Unsupported row level '{request.Level}'.");
        }

        await ClearPlanningCommandHistoryDirectAsync(connection, transaction, request.ScenarioVersionId, cancellationToken);
        return node;
    }

    private static async Task<int> DeleteRowInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        long productNodeId,
        CancellationToken cancellationToken)
    {
        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        if (!productNodes.ContainsKey(productNodeId))
        {
            throw new InvalidOperationException($"Product node {productNodeId} was not found.");
        }

        var nodeIdsToDelete = productNodes.Values
            .Where(node => IsAncestorOrSelfDirect(productNodes, productNodeId, node.ProductNodeId))
            .Select(node => node.ProductNodeId)
            .ToArray();

        var deletedCells = await DeletePlanningCellsForNodesDirectAsync(connection, transaction, scenarioVersionId, nodeIdsToDelete, cancellationToken);
        await DeleteAuditDeltasForNodesDirectAsync(connection, transaction, scenarioVersionId, nodeIdsToDelete, cancellationToken);
        await DeleteProductNodesDirectAsync(connection, transaction, nodeIdsToDelete, cancellationToken);
        await RebuildHierarchyMappingsDirectAsync(connection, transaction, cancellationToken);
        await ClearPlanningCommandHistoryDirectAsync(connection, transaction, scenarioVersionId, cancellationToken);
        return deletedCells;
    }

    private static async Task<int> DeleteYearInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        long yearTimePeriodId,
        CancellationToken cancellationToken)
    {
        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        if (!timePeriods.TryGetValue(yearTimePeriodId, out var yearNode) || !string.Equals(yearNode.Grain, "year", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Year time period {yearTimePeriodId} was not found.");
        }

        var timeIdsToDelete = timePeriods.Values
            .Where(period => IsAncestorOrSelfDirect(timePeriods, yearTimePeriodId, period.TimePeriodId))
            .Select(period => period.TimePeriodId)
            .ToArray();

        var deletedCells = await DeletePlanningCellsForTimePeriodsDirectAsync(connection, transaction, scenarioVersionId, timeIdsToDelete, cancellationToken);
        await DeleteAuditDeltasForTimePeriodsDirectAsync(connection, transaction, scenarioVersionId, timeIdsToDelete, cancellationToken);
        await DeleteTimePeriodsDirectAsync(connection, transaction, timeIdsToDelete, cancellationToken);
        await ClearPlanningCommandHistoryDirectAsync(connection, transaction, scenarioVersionId, cancellationToken);
        return deletedCells;
    }

    private static async Task EnsureYearInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        int fiscalYear,
        CancellationToken cancellationToken)
    {
        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        var desiredPeriods = BuildYearPeriodsDirect(fiscalYear);
        var addedAny = false;
        foreach (var period in desiredPeriods.Values.OrderBy(period => period.SortOrder))
        {
            if (timePeriods.ContainsKey(period.TimePeriodId))
            {
                continue;
            }

            addedAny = true;
            await InsertTimePeriodDirectAsync(connection, transaction, period, cancellationToken);
        }

        if (!addedAny)
        {
            return;
        }

        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        var refreshedTimePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        await EnsureSupportedMeasureCellsDirectAsync(connection, transaction, scenarioVersionId, productNodes.Values, refreshedTimePeriods.Values, cancellationToken);
    }

    private static ProductNode GetRequiredNodeDirect(
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

    private static string BuildGeneratedStoreCodeDirect(string storeLabel, IReadOnlyDictionary<long, StoreNodeMetadata> stores)
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

    private static async Task InsertProductNodeDirectAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into product_nodes (
                product_node_id,
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
                effective_to_time_period_id)
            values (
                @productNodeId,
                @storeId,
                @parentProductNodeId,
                @label,
                @level,
                @pathJson,
                @isLeaf,
                @nodeKind,
                @lifecycleState,
                @rampProfileCode,
                @effectiveFromTimePeriodId,
                @effectiveToTimePeriodId);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("@storeId", node.StoreId);
        command.Parameters.AddWithValue("@parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("@label", node.Label);
        command.Parameters.AddWithValue("@level", node.Level);
        command.Parameters.AddWithValue("@pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("@isLeaf", node.IsLeaf ? 1 : 0);
        command.Parameters.AddWithValue("@nodeKind", node.NodeKind);
        command.Parameters.AddWithValue("@lifecycleState", node.LifecycleState);
        command.Parameters.AddWithValue("@rampProfileCode", (object?)node.RampProfileCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveFromTimePeriodId", (object?)node.EffectiveFromTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveToTimePeriodId", (object?)node.EffectiveToTimePeriodId ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateProductNodeDirectAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            update product_nodes
            set store_id = @storeId,
                parent_product_node_id = @parentProductNodeId,
                label = @label,
                level = @level,
                path_json = @pathJson,
                is_leaf = @isLeaf,
                node_kind = @nodeKind,
                lifecycle_state = @lifecycleState,
                ramp_profile_code = @rampProfileCode,
                effective_from_time_period_id = @effectiveFromTimePeriodId,
                effective_to_time_period_id = @effectiveToTimePeriodId
            where product_node_id = @productNodeId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("@storeId", node.StoreId);
        command.Parameters.AddWithValue("@parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("@label", node.Label);
        command.Parameters.AddWithValue("@level", node.Level);
        command.Parameters.AddWithValue("@pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("@isLeaf", node.IsLeaf ? 1 : 0);
        command.Parameters.AddWithValue("@nodeKind", node.NodeKind);
        command.Parameters.AddWithValue("@lifecycleState", node.LifecycleState);
        command.Parameters.AddWithValue("@rampProfileCode", (object?)node.RampProfileCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveFromTimePeriodId", (object?)node.EffectiveFromTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveToTimePeriodId", (object?)node.EffectiveToTimePeriodId ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertStoreMetadataDirectAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, StoreNodeMetadata metadata, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
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
                @storeId,
                @storeLabel,
                @clusterLabel,
                @regionLabel,
                @lifecycleState,
                @rampProfileCode,
                @effectiveFromTimePeriodId,
                @effectiveToTimePeriodId,
                @storeCode,
                @state,
                @latitude,
                @longitude,
                @openingDate,
                @sssg,
                @salesType,
                @status,
                @storey,
                @buildingStatus,
                @gta,
                @nta,
                @rsom,
                @dm,
                @rental,
                @storeClusterRole,
                @storeCapacitySqFt,
                @storeFormatTier,
                @catchmentType,
                @demographicSegment,
                @climateZone,
                @fulfilmentEnabled,
                @onlineFulfilmentNode,
                @storeOpeningSeason,
                @storeClosureDate,
                @refurbishmentDate,
                @storePriority,
                @isActive)
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
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@storeId", metadata.StoreId);
        command.Parameters.AddWithValue("@storeLabel", metadata.StoreLabel);
        command.Parameters.AddWithValue("@clusterLabel", metadata.ClusterLabel);
        command.Parameters.AddWithValue("@regionLabel", metadata.RegionLabel);
        command.Parameters.AddWithValue("@lifecycleState", metadata.LifecycleState);
        command.Parameters.AddWithValue("@rampProfileCode", (object?)metadata.RampProfileCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveFromTimePeriodId", (object?)metadata.EffectiveFromTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("@effectiveToTimePeriodId", (object?)metadata.EffectiveToTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("@storeCode", (object?)metadata.StoreCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@state", (object?)metadata.State ?? DBNull.Value);
        command.Parameters.AddWithValue("@latitude", (object?)metadata.Latitude ?? DBNull.Value);
        command.Parameters.AddWithValue("@longitude", (object?)metadata.Longitude ?? DBNull.Value);
        command.Parameters.AddWithValue("@openingDate", (object?)metadata.OpeningDate ?? DBNull.Value);
        command.Parameters.AddWithValue("@sssg", (object?)metadata.Sssg ?? DBNull.Value);
        command.Parameters.AddWithValue("@salesType", (object?)metadata.SalesType ?? DBNull.Value);
        command.Parameters.AddWithValue("@status", (object?)metadata.Status ?? DBNull.Value);
        command.Parameters.AddWithValue("@storey", (object?)metadata.Storey ?? DBNull.Value);
        command.Parameters.AddWithValue("@buildingStatus", (object?)metadata.BuildingStatus ?? DBNull.Value);
        command.Parameters.AddWithValue("@gta", (object?)metadata.Gta ?? DBNull.Value);
        command.Parameters.AddWithValue("@nta", (object?)metadata.Nta ?? DBNull.Value);
        command.Parameters.AddWithValue("@rsom", (object?)metadata.Rsom ?? DBNull.Value);
        command.Parameters.AddWithValue("@dm", (object?)metadata.Dm ?? DBNull.Value);
        command.Parameters.AddWithValue("@rental", (object?)metadata.Rental ?? DBNull.Value);
        command.Parameters.AddWithValue("@storeClusterRole", (object?)metadata.StoreClusterRole ?? DBNull.Value);
        command.Parameters.AddWithValue("@storeCapacitySqFt", (object?)metadata.StoreCapacitySqFt ?? DBNull.Value);
        command.Parameters.AddWithValue("@storeFormatTier", (object?)metadata.StoreFormatTier ?? DBNull.Value);
        command.Parameters.AddWithValue("@catchmentType", (object?)metadata.CatchmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("@demographicSegment", (object?)metadata.DemographicSegment ?? DBNull.Value);
        command.Parameters.AddWithValue("@climateZone", (object?)metadata.ClimateZone ?? DBNull.Value);
        command.Parameters.AddWithValue("@fulfilmentEnabled", metadata.FulfilmentEnabled ? 1 : 0);
        command.Parameters.AddWithValue("@onlineFulfilmentNode", metadata.OnlineFulfilmentNode ? 1 : 0);
        command.Parameters.AddWithValue("@storeOpeningSeason", (object?)metadata.StoreOpeningSeason ?? DBNull.Value);
        command.Parameters.AddWithValue("@storeClosureDate", (object?)metadata.StoreClosureDate ?? DBNull.Value);
        command.Parameters.AddWithValue("@refurbishmentDate", (object?)metadata.RefurbishmentDate ?? DBNull.Value);
        command.Parameters.AddWithValue("@storePriority", (object?)metadata.StorePriority ?? DBNull.Value);
        command.Parameters.AddWithValue("@isActive", metadata.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InitializeCellsForNodeDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        ProductNode node,
        IEnumerable<long> measureIds,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        const string sql = """
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
                @scenarioVersionId,
                @measureId,
                @storeId,
                @productNodeId,
                @timePeriodId,
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
                @cellKind)
            on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id) do nothing;
            """;

        foreach (var measureId in measureIds)
        {
            foreach (var period in timePeriods)
            {
                await using var command = new NpgsqlCommand(sql, connection, transaction);
                command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                command.Parameters.AddWithValue("@measureId", measureId);
                command.Parameters.AddWithValue("@storeId", node.StoreId);
                command.Parameters.AddWithValue("@productNodeId", node.ProductNodeId);
                command.Parameters.AddWithValue("@timePeriodId", period.TimePeriodId);
                command.Parameters.AddWithValue("@cellKind", node.IsLeaf && string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase) ? "leaf" : "calculated");
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    private static async Task EnsureSupportedMeasureCellsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        IEnumerable<ProductNode> productNodes,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        var periods = timePeriods.ToList();
        foreach (var productNode in productNodes)
        {
            await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, productNode, SupportedMeasureIdsDirect, periods, cancellationToken);
        }
    }

    private static async Task<int> DeletePlanningCellsForNodesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        IReadOnlyList<long> nodeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (nodeIdsToDelete.Count == 0)
        {
            return 0;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from planning_cells
            where scenario_version_id = @scenarioVersionId
              and product_node_id = any(@productNodeIds);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@productNodeIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = nodeIdsToDelete.ToArray()
        });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteAuditDeltasForNodesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        IReadOnlyList<long> nodeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (nodeIdsToDelete.Count == 0)
        {
            return;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from audit_deltas
            where scenario_version_id = @scenarioVersionId
              and product_node_id = any(@productNodeIds);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@productNodeIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = nodeIdsToDelete.ToArray()
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteProductNodesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<long> nodeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (nodeIdsToDelete.Count == 0)
        {
            return;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from product_nodes
            where product_node_id = any(@productNodeIds);
            """,
            connection,
            transaction);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@productNodeIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = nodeIdsToDelete.ToArray()
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> DeletePlanningCellsForTimePeriodsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        IReadOnlyList<long> timeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (timeIdsToDelete.Count == 0)
        {
            return 0;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from planning_cells
            where scenario_version_id = @scenarioVersionId
              and time_period_id = any(@timePeriodIds);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@timePeriodIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = timeIdsToDelete.ToArray()
        });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteAuditDeltasForTimePeriodsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        IReadOnlyList<long> timeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (timeIdsToDelete.Count == 0)
        {
            return;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from audit_deltas
            where scenario_version_id = @scenarioVersionId
              and time_period_id = any(@timePeriodIds);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@timePeriodIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = timeIdsToDelete.ToArray()
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteTimePeriodsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<long> timeIdsToDelete,
        CancellationToken cancellationToken)
    {
        if (timeIdsToDelete.Count == 0)
        {
            return;
        }

        await using var command = new NpgsqlCommand(
            """
            delete from time_periods
            where time_period_id = any(@timePeriodIds);
            """,
            connection,
            transaction);
        command.Parameters.Add(new NpgsqlParameter<long[]>("@timePeriodIds", NpgsqlDbType.Array | NpgsqlDbType.Bigint)
        {
            Value = timeIdsToDelete.ToArray()
        });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertTimePeriodDirectAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, TimePeriodNode period, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
            values (@timePeriodId, @parentTimePeriodId, @label, @grain, @sortOrder);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@timePeriodId", period.TimePeriodId);
        command.Parameters.AddWithValue("@parentTimePeriodId", (object?)period.ParentTimePeriodId ?? DBNull.Value);
        command.Parameters.AddWithValue("@label", period.Label);
        command.Parameters.AddWithValue("@grain", period.Grain);
        command.Parameters.AddWithValue("@sortOrder", period.SortOrder);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task ClearPlanningCommandHistoryDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long? scenarioVersionId,
        CancellationToken cancellationToken)
    {
        if (scenarioVersionId is null)
        {
            foreach (var tableName in new[] { "planning_command_cell_deltas", "planning_command_batches" })
            {
                await using var command = new NpgsqlCommand($"delete from {tableName};", connection, transaction);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return;
        }

        await using (var deleteDeltaCommand = new NpgsqlCommand(
            """
            delete from planning_command_cell_deltas
            where scenario_version_id = @scenarioVersionId;
            """,
            connection,
            transaction))
        {
            deleteDeltaCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId.Value);
            await deleteDeltaCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var deleteBatchCommand = new NpgsqlCommand(
            """
            delete from planning_command_batches
            where scenario_version_id = @scenarioVersionId;
            """,
            connection,
            transaction))
        {
            deleteBatchCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId.Value);
            await deleteBatchCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task<long> CloneStoreHierarchyAndDataDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
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
            await InsertProductNodeDirectAsync(connection, transaction, clonedNode, cancellationToken);
            await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, clonedNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
            nodeMap[sourceNode.ProductNodeId] = clonedNode;
        }

        foreach (var sourceNode in sourceNodes.Prepend(sourceRootNode))
        {
            var targetNode = nodeMap[sourceNode.ProductNodeId];
            foreach (var supportedMeasureId in SupportedMeasureIdsDirect)
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
                    await UpsertPlanningCellsAsync(connection, transaction, [clonedCell], cancellationToken);
                }
            }
        }

        return nextProductNodeId;
    }

    private static async Task RebuildHierarchyMappingsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        foreach (var tableName in new[] { "hierarchy_subclasses_v2", "hierarchy_classes_v2", "hierarchy_departments_v2", "hierarchy_subcategories", "hierarchy_categories" })
        {
            await using var deleteCommand = new NpgsqlCommand($"delete from {tableName};", connection, transaction);
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        foreach (var department in productNodes.Values.Where(node => node.Level == 1))
        {
            await UpsertHierarchyDepartmentInternalDirectAsync(connection, transaction, department.Label, cancellationToken);
        }

        foreach (var classNode in productNodes.Values.Where(node => node.Level == 2))
        {
            var department = productNodes[classNode.ParentProductNodeId!.Value];
            await UpsertHierarchyClassInternalDirectAsync(connection, transaction, department.Label, classNode.Label, cancellationToken);
        }

        foreach (var subclassNode in productNodes.Values.Where(node => node.Level == 3))
        {
            var classNode = productNodes[subclassNode.ParentProductNodeId!.Value];
            var departmentNode = productNodes[classNode.ParentProductNodeId!.Value];
            await UpsertHierarchySubclassInternalDirectAsync(connection, transaction, departmentNode.Label, classNode.Label, subclassNode.Label, cancellationToken);
        }
    }

    private static async Task UpsertHierarchyDepartmentInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string departmentLabel,
        CancellationToken cancellationToken)
    {
        var normalizedLabel = departmentLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedLabel))
        {
            throw new InvalidOperationException("Department labels cannot be empty.");
        }

        await using (var command = new NpgsqlCommand(
            """
            insert into hierarchy_categories (category_label)
            values (@categoryLabel)
            on conflict (category_label) do nothing;
            """,
            connection,
            transaction))
        {
            command.Parameters.AddWithValue("@categoryLabel", normalizedLabel);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var modernCommand = new NpgsqlCommand(
            """
            insert into hierarchy_departments_v2 (
                department_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values (@departmentLabel, 'active', null, null, null)
            on conflict (department_label) do nothing;
            """,
            connection,
            transaction))
        {
            modernCommand.Parameters.AddWithValue("@departmentLabel", normalizedLabel);
            await modernCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task UpsertHierarchyClassInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string departmentLabel,
        string classLabel,
        CancellationToken cancellationToken)
    {
        var normalizedDepartment = departmentLabel.Trim();
        var normalizedClass = classLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedClass))
        {
            throw new InvalidOperationException("Class labels cannot be empty.");
        }

        await UpsertHierarchyDepartmentInternalDirectAsync(connection, transaction, normalizedDepartment, cancellationToken);
        await using (var command = new NpgsqlCommand(
            """
            insert into hierarchy_subcategories (category_label, subcategory_label)
            values (@categoryLabel, @subcategoryLabel)
            on conflict (category_label, subcategory_label) do nothing;
            """,
            connection,
            transaction))
        {
            command.Parameters.AddWithValue("@categoryLabel", normalizedDepartment);
            command.Parameters.AddWithValue("@subcategoryLabel", normalizedClass);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var modernCommand = new NpgsqlCommand(
            """
            insert into hierarchy_classes_v2 (
                department_label,
                class_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values (@departmentLabel, @classLabel, 'active', null, null, null)
            on conflict (department_label, class_label) do nothing;
            """,
            connection,
            transaction))
        {
            modernCommand.Parameters.AddWithValue("@departmentLabel", normalizedDepartment);
            modernCommand.Parameters.AddWithValue("@classLabel", normalizedClass);
            await modernCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task UpsertHierarchySubclassInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string departmentLabel,
        string classLabel,
        string subclassLabel,
        CancellationToken cancellationToken)
    {
        var normalizedDepartment = departmentLabel.Trim();
        var normalizedClass = classLabel.Trim();
        var normalizedSubclass = subclassLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedSubclass))
        {
            throw new InvalidOperationException("Subclass labels cannot be empty.");
        }

        await UpsertHierarchyClassInternalDirectAsync(connection, transaction, normalizedDepartment, normalizedClass, cancellationToken);
        await using var command = new NpgsqlCommand(
            """
            insert into hierarchy_subclasses_v2 (
                department_label,
                class_label,
                subclass_label,
                lifecycle_state,
                ramp_profile_code,
                effective_from_time_period_id,
                effective_to_time_period_id)
            values (@departmentLabel, @classLabel, @subclassLabel, 'active', null, null, null)
            on conflict (department_label, class_label, subclass_label) do nothing;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@departmentLabel", normalizedDepartment);
        command.Parameters.AddWithValue("@classLabel", normalizedClass);
        command.Parameters.AddWithValue("@subclassLabel", normalizedSubclass);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static Dictionary<long, TimePeriodNode> BuildYearPeriodsDirect(int fiscalYear)
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
