using System.Globalization;
using Npgsql;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresPlanningRepository
{
    private Task<StoreNodeMetadata> UpsertStoreProfileDirectAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertStoreProfileInternalDirectAsync(connection, transaction, scenarioVersionId, storeProfile, ct),
            cancellationToken);

    private Task DeleteStoreProfileDirectAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => DeleteStoreProfileInternalDirectAsync(connection, transaction, scenarioVersionId, storeId, ct),
            cancellationToken);

    private Task InactivateStoreProfileDirectAsync(long storeId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => InactivateStoreProfileInternalDirectAsync(connection, transaction, storeId, ct),
            cancellationToken);

    private Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            (connection, transaction, ct) => LoadStoreProfileOptionsDirectAsync(connection, transaction, ct),
            cancellationToken);

    private Task UpsertStoreProfileOptionDirectAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertStoreProfileOptionInternalDirectAsync(connection, transaction, fieldName, value, isActive, ct),
            cancellationToken);

    private Task DeleteStoreProfileOptionDirectAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => DeleteStoreProfileOptionInternalDirectAsync(connection, transaction, fieldName, value, ct),
            cancellationToken);

    private Task UpsertHierarchyDepartmentDirectAsync(string departmentLabel, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertHierarchyDepartmentInternalDirectAsync(connection, transaction, departmentLabel, ct),
            cancellationToken);

    private Task UpsertHierarchyClassDirectAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertHierarchyClassInternalDirectAsync(connection, transaction, departmentLabel, classLabel, ct),
            cancellationToken);

    private Task UpsertHierarchySubclassDirectAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertHierarchySubclassInternalDirectAsync(connection, transaction, departmentLabel, classLabel, subclassLabel, ct),
            cancellationToken);

    private async Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesDirectAsync(
        string? searchTerm,
        int pageNumber,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var normalizedSearch = string.IsNullOrWhiteSpace(searchTerm) ? null : $"%{searchTerm.Trim()}%";
        var normalizedPageNumber = Math.Max(1, pageNumber);
        var normalizedPageSize = Math.Clamp(pageSize, 25, 500);
        var offset = (normalizedPageNumber - 1) * normalizedPageSize;

        return await ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                var totalCount = await CountProductProfilesDirectAsync(connection, transaction, normalizedSearch, ct);
                var profiles = await LoadProductProfilesPageDirectAsync(connection, transaction, normalizedSearch, normalizedPageSize, offset, ct);
                return ((IReadOnlyList<ProductProfileMetadata>)profiles, totalCount);
            },
            cancellationToken);
    }

    private Task<ProductProfileMetadata> UpsertProductProfileDirectAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                var normalized = NormalizeProductProfileDirect(profile);
                await UpsertProductProfileInternalDirectAsync(connection, transaction, normalized, ct);
                await UpsertProductProfileOptionSeedsDirectAsync(connection, transaction, normalized, ct);
                await UpsertDerivedSubclassCatalogDirectAsync(connection, transaction, normalized, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
                return normalized;
            },
            cancellationToken);

    private Task DeleteProductProfileDirectAsync(string skuVariant, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using (var command = new NpgsqlCommand("delete from product_profiles where sku_variant = @skuVariant;", connection, transaction))
                {
                    command.Parameters.AddWithValue("@skuVariant", skuVariant.Trim());
                    await command.ExecuteNonQueryAsync(ct);
                }

                await RefreshProductSubclassCatalogDirectAsync(connection, transaction, ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
            },
            cancellationToken);

    private Task InactivateProductProfileDirectAsync(string skuVariant, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using (var command = new NpgsqlCommand(
                    """
                    update product_profiles
                    set is_active = 0,
                        active_flag = '0'
                    where sku_variant = @skuVariant;
                    """,
                    connection,
                    transaction))
                {
                    command.Parameters.AddWithValue("@skuVariant", skuVariant.Trim());
                    await command.ExecuteNonQueryAsync(ct);
                }

                await RefreshProductSubclassCatalogDirectAsync(connection, transaction, ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
            },
            cancellationToken);

    private Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            (connection, transaction, ct) => LoadProductProfileOptionsDirectAsync(connection, transaction, ct),
            cancellationToken);

    private Task UpsertProductProfileOptionDirectAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertOptionDirectAsync(connection, transaction, "product_profile_options", fieldName, value, isActive, ct),
            cancellationToken);

    private Task DeleteProductProfileOptionDirectAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => DeleteOptionDirectAsync(connection, transaction, "product_profile_options", fieldName, value, ct),
            cancellationToken);

    private Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            (connection, transaction, ct) => LoadProductHierarchyCatalogRecordsDirectAsync(connection, transaction, ct),
            cancellationToken);

    private Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            (connection, transaction, ct) => LoadProductSubclassCatalogRecordsDirectAsync(connection, transaction, ct),
            cancellationToken);

    private Task UpsertProductHierarchyCatalogDirectAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await UpsertProductHierarchyCatalogInternalDirectAsync(connection, transaction, NormalizeProductHierarchyRecordDirect(record), ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
            },
            cancellationToken);

    private Task DeleteProductHierarchyCatalogDirectAsync(string dptNo, string clssNo, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await using (var command = new NpgsqlCommand(
                    "delete from product_hierarchy_catalog where dpt_no = @dptNo and clss_no = @clssNo;",
                    connection,
                    transaction))
                {
                    command.Parameters.AddWithValue("@dptNo", dptNo.Trim());
                    command.Parameters.AddWithValue("@clssNo", clssNo.Trim());
                    await command.ExecuteNonQueryAsync(ct);
                }

                await using (var command = new NpgsqlCommand(
                    "delete from product_profiles where dpt_no = @dptNo and clss_no = @clssNo;",
                    connection,
                    transaction))
                {
                    command.Parameters.AddWithValue("@dptNo", dptNo.Trim());
                    command.Parameters.AddWithValue("@clssNo", clssNo.Trim());
                    await command.ExecuteNonQueryAsync(ct);
                }

                await RefreshProductSubclassCatalogDirectAsync(connection, transaction, ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
            },
            cancellationToken);

    private Task ReplaceProductMasterDataDirectAsync(
        IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows,
        IReadOnlyList<ProductProfileMetadata> profiles,
        CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                foreach (var tableName in new[] { "product_subclass_catalog", "product_hierarchy_catalog", "product_profile_options", "product_profiles" })
                {
                    await using var command = new NpgsqlCommand($"delete from {tableName};", connection, transaction);
                    await command.ExecuteNonQueryAsync(ct);
                }

                foreach (var hierarchyRow in hierarchyRows.Select(NormalizeProductHierarchyRecordDirect))
                {
                    await UpsertProductHierarchyCatalogInternalDirectAsync(connection, transaction, hierarchyRow, ct);
                }

                foreach (var profile in profiles.Select(NormalizeProductProfileDirect))
                {
                    await UpsertProductProfileInternalDirectAsync(connection, transaction, profile, ct);
                }

                await RefreshProductSubclassCatalogDirectAsync(connection, transaction, ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
            },
            cancellationToken);

    private Task RebuildPlanningFromMasterDataDirectAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, scenarioVersionId, fiscalYear, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, scenarioVersionId, fiscalYear + 1, ct);
            },
            cancellationToken);

    private Task ResetDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                foreach (var tableName in new[]
                         {
                             "planning_draft_command_cell_deltas",
                             "planning_draft_command_batches",
                             "planning_draft_cells",
                             "planning_save_checkpoints",
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
                    await using var deleteCommand = new NpgsqlCommand($"delete from {tableName};", connection, transaction);
                    await deleteCommand.ExecuteNonQueryAsync(ct);
                }

                foreach (var store in BuildStoreMetadataSeedDirect())
                {
                    await UpsertStoreMetadataDirectAsync(connection, transaction, store, ct);
                }

                foreach (var profile in BuildSeedProductProfilesDirect())
                {
                    await UpsertProductProfileInternalDirectAsync(connection, transaction, profile, ct);
                    await UpsertDerivedSubclassCatalogDirectAsync(connection, transaction, profile, ct);
                }

                await EnsureStoreProfileOptionSeedDirectAsync(connection, transaction, ct);
                await EnsureProductProfileOptionSeedDirectAsync(connection, transaction, ct);
                await RebuildPlanningFromMasterDataInternalDirectAsync(connection, transaction, 1, 2026, ct);
                await EnsureYearInternalDirectAsync(connection, transaction, 1, 2027, ct);
                await ClearPlanningCommandHistoryDirectAsync(connection, transaction, null, ct);
            },
            cancellationToken);

    private static async Task<StoreNodeMetadata> UpsertStoreProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        StoreNodeMetadata storeProfile,
        CancellationToken cancellationToken)
    {
        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        var stores = await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken);
        var normalizedProfile = NormalizePersistedStoreProfileDirect(storeProfile, stores);
        var existingStore = ResolveExistingStoreDirect(normalizedProfile, stores);

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
            await InsertProductNodeDirectAsync(connection, transaction, rootNode, cancellationToken);
            await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, rootNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
            persistedStore = normalizedProfile with { StoreId = nextStoreId };
        }
        else
        {
            persistedStore = normalizedProfile with { StoreId = existingStore.StoreId };
            if (!string.Equals(existingStore.StoreLabel, persistedStore.StoreLabel, StringComparison.Ordinal))
            {
                await RenameStoreHierarchyDirectAsync(
                    connection,
                    transaction,
                    productNodes.Values.Where(node => node.StoreId == existingStore.StoreId).ToList(),
                    persistedStore.StoreLabel,
                    cancellationToken);
            }
        }

        await UpsertStoreMetadataDirectAsync(connection, transaction, persistedStore, cancellationToken);
        await UpsertStoreProfileOptionsForMetadataDirectAsync(connection, transaction, persistedStore, cancellationToken);
        return persistedStore;
    }

    private static async Task DeleteStoreProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        long storeId,
        CancellationToken cancellationToken)
    {
        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        _ = productNodes.Values.SingleOrDefault(node => node.StoreId == storeId && node.Level == 0)
            ?? throw new InvalidOperationException($"Store {storeId} was not found.");

        var nodeIdsToDelete = productNodes.Values
            .Where(node => node.StoreId == storeId)
            .Select(node => node.ProductNodeId)
            .ToList();

        await DeletePlanningCellsForNodesDirectAsync(connection, transaction, scenarioVersionId, nodeIdsToDelete, cancellationToken);
        await DeleteAuditDeltasForNodesDirectAsync(connection, transaction, scenarioVersionId, nodeIdsToDelete, cancellationToken);
        await DeleteProductNodesDirectAsync(connection, transaction, nodeIdsToDelete, cancellationToken);
        await DeleteStoreMetadataDirectAsync(connection, transaction, storeId, cancellationToken);
        await RebuildHierarchyMappingsDirectAsync(connection, transaction, cancellationToken);
        await ClearPlanningCommandHistoryDirectAsync(connection, transaction, scenarioVersionId, cancellationToken);
    }

    private static async Task InactivateStoreProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long storeId,
        CancellationToken cancellationToken)
    {
        var stores = await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken);
        if (!stores.TryGetValue(storeId, out var store))
        {
            throw new InvalidOperationException($"Store {storeId} was not found.");
        }

        await UpsertStoreMetadataDirectAsync(connection, transaction, store with
        {
            IsActive = false,
            LifecycleState = "inactive",
            Status = string.IsNullOrWhiteSpace(store.Status) ? "Inactive" : store.Status
        }, cancellationToken);
    }

    private static async Task DeleteStoreMetadataDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long storeId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("delete from store_metadata where store_id = @storeId;", connection, transaction);
        command.Parameters.AddWithValue("@storeId", storeId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static StoreNodeMetadata? ResolveExistingStoreDirect(StoreNodeMetadata profile, IReadOnlyDictionary<long, StoreNodeMetadata> stores)
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

    private static StoreNodeMetadata NormalizePersistedStoreProfileDirect(
        StoreNodeMetadata profile,
        IReadOnlyDictionary<long, StoreNodeMetadata> stores)
    {
        var resolvedLabel = string.IsNullOrWhiteSpace(profile.StoreLabel) ? profile.StoreCode?.Trim() : profile.StoreLabel.Trim();
        if (string.IsNullOrWhiteSpace(resolvedLabel))
        {
            throw new InvalidOperationException("BranchName is required.");
        }

        var resolvedCode = string.IsNullOrWhiteSpace(profile.StoreCode)
            ? BuildGeneratedStoreCodeDirect(resolvedLabel, stores)
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

    private static async Task RenameStoreHierarchyDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
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
            await UpdateProductNodeDirectAsync(connection, transaction, updatedNode, cancellationToken);
        }
    }

    private static async Task<IReadOnlyList<StoreProfileOptionValue>> LoadStoreProfileOptionsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<StoreProfileOptionValue>();
        await using var command = new NpgsqlCommand(
            """
            select field_name, option_value, is_active
            from store_profile_options
            order by field_name asc, option_value asc;
            """,
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new StoreProfileOptionValue(reader.GetString(0), reader.GetString(1), reader.GetInt32(2) == 1));
        }

        return result;
    }

    private static async Task EnsureStoreProfileOptionSeedDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        var stores = await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken);
        foreach (var store in stores.Values)
        {
            await UpsertStoreProfileOptionsForMetadataDirectAsync(connection, transaction, store, cancellationToken);
        }
    }

    private static async Task UpsertStoreProfileOptionsForMetadataDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
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

            await UpsertStoreProfileOptionInternalDirectAsync(connection, transaction, fieldName, value, true, cancellationToken);
        }
    }

    private static async Task UpsertStoreProfileOptionInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string fieldName,
        string value,
        bool isActive,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into store_profile_options (field_name, option_value, is_active)
            values (@fieldName, @value, @isActive)
            on conflict (field_name, option_value)
            do update set is_active = excluded.is_active;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("@value", value.Trim());
        command.Parameters.AddWithValue("@isActive", isActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteStoreProfileOptionInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string fieldName,
        string value,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            delete from store_profile_options
            where field_name = @fieldName
              and option_value = @value;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("@value", value.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<int> CountProductProfilesDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? normalizedSearch,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
                ? "select count(*) from product_profiles;"
                : """
                  select count(*)
                  from product_profiles
                  where sku_variant ilike @search
                     or description ilike @search
                     or department ilike @search
                     or class ilike @search
                     or subclass ilike @search;
                  """,
            connection,
            transaction);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@search", normalizedSearch);
        }

        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private static async Task<List<ProductProfileMetadata>> LoadProductProfilesPageDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string? normalizedSearch,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            normalizedSearch is null
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
                  limit @limit offset @offset;
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
                  where sku_variant ilike @search
                     or description ilike @search
                     or department ilike @search
                     or class ilike @search
                     or subclass ilike @search
                  order by department, class, subclass, description, sku_variant
                  limit @limit offset @offset;
                  """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@limit", limit);
        command.Parameters.AddWithValue("@offset", offset);
        if (normalizedSearch is not null)
        {
            command.Parameters.AddWithValue("@search", normalizedSearch);
        }

        var result = new List<ProductProfileMetadata>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadProductProfileDirect(reader));
        }

        return result;
    }

    private static ProductProfileMetadata ReadProductProfileDirect(NpgsqlDataReader reader) => new(
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
        reader.GetInt32(53) == 1,
        reader.IsDBNull(25) ? null : reader.GetString(25),
        reader.IsDBNull(26) ? null : reader.GetString(26),
        reader.IsDBNull(27) ? null : reader.GetString(27),
        reader.IsDBNull(28) ? null : reader.GetString(28),
        reader.IsDBNull(29) ? null : reader.GetString(29),
        reader.IsDBNull(30) ? null : reader.GetString(30),
        reader.IsDBNull(31) ? null : reader.GetString(31),
        reader.IsDBNull(32) ? null : reader.GetString(32),
        !reader.IsDBNull(33) && reader.GetInt32(33) == 1,
        reader.IsDBNull(34) || reader.GetInt32(34) == 1,
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
        reader.IsDBNull(52) ? null : ReadDecimal(reader, 52));

    private static ProductProfileMetadata NormalizeProductProfileDirect(ProductProfileMetadata profile)
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

        static string? Optional(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

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

    private static async Task UpsertProductProfileInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ProductProfileMetadata profile,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into product_profiles (
                sku_variant, description, description2, price, cost, dpt_no, clss_no, brand_no, department, class,
                brand, rev_department, rev_class, subclass, prod_group, prod_type, active_flag, order_flag, brand_type,
                launch_month, gender, size, collection, promo, ramadhan_promo, supplier, lifecycle_stage, age_stage,
                gender_target, material, pack_size, size_range, colour_family, kvi_flag, markdown_eligible, markdown_floor_price,
                minimum_margin_pct, price_ladder_group, good_better_best_tier, season_code, event_code, launch_date, end_of_life_date,
                substitute_group, companion_group, replenishment_type, lead_time_days, moq, case_pack, starting_inventory,
                projected_stock_on_hand, sell_through_target_pct, weeks_of_cover_target, is_active)
            values (
                @skuVariant, @description, @description2, @price, @cost, @dptNo, @clssNo, @brandNo, @department, @class,
                @brand, @revDepartment, @revClass, @subclass, @prodGroup, @prodType, @activeFlag, @orderFlag, @brandType,
                @launchMonth, @gender, @size, @collection, @promo, @ramadhanPromo, @supplier, @lifecycleStage, @ageStage,
                @genderTarget, @material, @packSize, @sizeRange, @colourFamily, @kviFlag, @markdownEligible, @markdownFloorPrice,
                @minimumMarginPct, @priceLadderGroup, @goodBetterBestTier, @seasonCode, @eventCode, @launchDate, @endOfLifeDate,
                @substituteGroup, @companionGroup, @replenishmentType, @leadTimeDays, @moq, @casePack, @startingInventory,
                @projectedStockOnHand, @sellThroughTargetPct, @weeksOfCoverTarget, @isActive)
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
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@skuVariant", profile.SkuVariant);
        command.Parameters.AddWithValue("@description", profile.Description);
        command.Parameters.AddWithValue("@description2", (object?)profile.Description2 ?? DBNull.Value);
        command.Parameters.AddWithValue("@price", profile.Price);
        command.Parameters.AddWithValue("@cost", profile.Cost);
        command.Parameters.AddWithValue("@dptNo", profile.DptNo);
        command.Parameters.AddWithValue("@clssNo", profile.ClssNo);
        command.Parameters.AddWithValue("@brandNo", (object?)profile.BrandNo ?? DBNull.Value);
        command.Parameters.AddWithValue("@department", profile.Department);
        command.Parameters.AddWithValue("@class", profile.Class);
        command.Parameters.AddWithValue("@brand", (object?)profile.Brand ?? DBNull.Value);
        command.Parameters.AddWithValue("@revDepartment", (object?)profile.RevDepartment ?? DBNull.Value);
        command.Parameters.AddWithValue("@revClass", (object?)profile.RevClass ?? DBNull.Value);
        command.Parameters.AddWithValue("@subclass", profile.Subclass);
        command.Parameters.AddWithValue("@prodGroup", (object?)profile.ProdGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("@prodType", (object?)profile.ProdType ?? DBNull.Value);
        command.Parameters.AddWithValue("@activeFlag", (object?)profile.ActiveFlag ?? DBNull.Value);
        command.Parameters.AddWithValue("@orderFlag", (object?)profile.OrderFlag ?? DBNull.Value);
        command.Parameters.AddWithValue("@brandType", (object?)profile.BrandType ?? DBNull.Value);
        command.Parameters.AddWithValue("@launchMonth", (object?)profile.LaunchMonth ?? DBNull.Value);
        command.Parameters.AddWithValue("@gender", (object?)profile.Gender ?? DBNull.Value);
        command.Parameters.AddWithValue("@size", (object?)profile.Size ?? DBNull.Value);
        command.Parameters.AddWithValue("@collection", (object?)profile.Collection ?? DBNull.Value);
        command.Parameters.AddWithValue("@promo", (object?)profile.Promo ?? DBNull.Value);
        command.Parameters.AddWithValue("@ramadhanPromo", (object?)profile.RamadhanPromo ?? DBNull.Value);
        command.Parameters.AddWithValue("@supplier", (object?)profile.Supplier ?? DBNull.Value);
        command.Parameters.AddWithValue("@lifecycleStage", (object?)profile.LifecycleStage ?? DBNull.Value);
        command.Parameters.AddWithValue("@ageStage", (object?)profile.AgeStage ?? DBNull.Value);
        command.Parameters.AddWithValue("@genderTarget", (object?)profile.GenderTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("@material", (object?)profile.Material ?? DBNull.Value);
        command.Parameters.AddWithValue("@packSize", (object?)profile.PackSize ?? DBNull.Value);
        command.Parameters.AddWithValue("@sizeRange", (object?)profile.SizeRange ?? DBNull.Value);
        command.Parameters.AddWithValue("@colourFamily", (object?)profile.ColourFamily ?? DBNull.Value);
        command.Parameters.AddWithValue("@kviFlag", profile.KviFlag ? 1 : 0);
        command.Parameters.AddWithValue("@markdownEligible", profile.MarkdownEligible ? 1 : 0);
        command.Parameters.AddWithValue("@markdownFloorPrice", (object?)profile.MarkdownFloorPrice ?? DBNull.Value);
        command.Parameters.AddWithValue("@minimumMarginPct", (object?)profile.MinimumMarginPct ?? DBNull.Value);
        command.Parameters.AddWithValue("@priceLadderGroup", (object?)profile.PriceLadderGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("@goodBetterBestTier", (object?)profile.GoodBetterBestTier ?? DBNull.Value);
        command.Parameters.AddWithValue("@seasonCode", (object?)profile.SeasonCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@eventCode", (object?)profile.EventCode ?? DBNull.Value);
        command.Parameters.AddWithValue("@launchDate", (object?)profile.LaunchDate ?? DBNull.Value);
        command.Parameters.AddWithValue("@endOfLifeDate", (object?)profile.EndOfLifeDate ?? DBNull.Value);
        command.Parameters.AddWithValue("@substituteGroup", (object?)profile.SubstituteGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("@companionGroup", (object?)profile.CompanionGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("@replenishmentType", (object?)profile.ReplenishmentType ?? DBNull.Value);
        command.Parameters.AddWithValue("@leadTimeDays", (object?)profile.LeadTimeDays ?? DBNull.Value);
        command.Parameters.AddWithValue("@moq", (object?)profile.Moq ?? DBNull.Value);
        command.Parameters.AddWithValue("@casePack", (object?)profile.CasePack ?? DBNull.Value);
        command.Parameters.AddWithValue("@startingInventory", (object?)profile.StartingInventory ?? DBNull.Value);
        command.Parameters.AddWithValue("@projectedStockOnHand", (object?)profile.ProjectedStockOnHand ?? DBNull.Value);
        command.Parameters.AddWithValue("@sellThroughTargetPct", (object?)profile.SellThroughTargetPct ?? DBNull.Value);
        command.Parameters.AddWithValue("@weeksOfCoverTarget", (object?)profile.WeeksOfCoverTarget ?? DBNull.Value);
        command.Parameters.AddWithValue("@isActive", profile.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertProductProfileOptionSeedsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
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

            await UpsertOptionDirectAsync(connection, transaction, "product_profile_options", fieldName, value, true, cancellationToken);
        }
    }

    private static async Task UpsertDerivedSubclassCatalogDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ProductProfileMetadata profile,
        CancellationToken cancellationToken)
    {
        await UpsertProductHierarchyCatalogInternalDirectAsync(
            connection,
            transaction,
            new ProductHierarchyCatalogRecord(
                profile.DptNo,
                profile.ClssNo,
                profile.Department,
                profile.Class,
                profile.ProdGroup ?? "UNASSIGNED",
                true),
            cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            insert into product_subclass_catalog (department, class, subclass, is_active)
            values (@department, @class, @subclass, @isActive)
            on conflict (department, class, subclass)
            do update set is_active = excluded.is_active;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@department", profile.Department);
        command.Parameters.AddWithValue("@class", profile.Class);
        command.Parameters.AddWithValue("@subclass", profile.Subclass);
        command.Parameters.AddWithValue("@isActive", profile.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<IReadOnlyList<ProductProfileOptionValue>> LoadProductProfileOptionsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductProfileOptionValue>();
        await using var command = new NpgsqlCommand(
            """
            select field_name, option_value, is_active
            from product_profile_options
            order by field_name asc, option_value asc;
            """,
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductProfileOptionValue(reader.GetString(0), reader.GetString(1), reader.GetInt32(2) == 1));
        }

        return result;
    }

    private static async Task UpsertOptionDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tableName,
        string fieldName,
        string value,
        bool isActive,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
            insert into {tableName} (field_name, option_value, is_active)
            values (@fieldName, @value, @isActive)
            on conflict (field_name, option_value)
            do update set is_active = excluded.is_active;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("@value", value.Trim());
        command.Parameters.AddWithValue("@isActive", isActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task DeleteOptionDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tableName,
        string fieldName,
        string value,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
            delete from {tableName}
            where field_name = @fieldName
              and option_value = @value;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@fieldName", fieldName.Trim());
        command.Parameters.AddWithValue("@value", value.Trim());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<IReadOnlyList<ProductHierarchyCatalogRecord>> LoadProductHierarchyCatalogRecordsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductHierarchyCatalogRecord>();
        await using var command = new NpgsqlCommand(
            """
            select dpt_no, clss_no, department, class, prod_group, is_active
            from product_hierarchy_catalog
            order by department asc, class asc;
            """,
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductHierarchyCatalogRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetString(4),
                reader.GetInt32(5) == 1));
        }

        return result;
    }

    private static async Task<IReadOnlyList<ProductSubclassCatalogRecord>> LoadProductSubclassCatalogRecordsDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        CancellationToken cancellationToken)
    {
        var result = new List<ProductSubclassCatalogRecord>();
        await using var command = new NpgsqlCommand(
            """
            select department, class, subclass, is_active
            from product_subclass_catalog
            order by department asc, class asc, subclass asc;
            """,
            connection,
            transaction);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new ProductSubclassCatalogRecord(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt32(3) == 1));
        }

        return result;
    }

    private static ProductHierarchyCatalogRecord NormalizeProductHierarchyRecordDirect(ProductHierarchyCatalogRecord record)
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

    private static async Task UpsertProductHierarchyCatalogInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        ProductHierarchyCatalogRecord record,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into product_hierarchy_catalog (dpt_no, clss_no, department, class, prod_group, is_active)
            values (@dptNo, @clssNo, @department, @class, @prodGroup, @isActive)
            on conflict (dpt_no, clss_no)
            do update set
                department = excluded.department,
                class = excluded.class,
                prod_group = excluded.prod_group,
                is_active = excluded.is_active;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@dptNo", record.DptNo);
        command.Parameters.AddWithValue("@clssNo", record.ClssNo);
        command.Parameters.AddWithValue("@department", record.Department);
        command.Parameters.AddWithValue("@class", record.Class);
        command.Parameters.AddWithValue("@prodGroup", record.ProdGroup);
        command.Parameters.AddWithValue("@isActive", record.IsActive ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task RefreshProductSubclassCatalogDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using (var deleteSubclasses = new NpgsqlCommand("delete from product_subclass_catalog;", connection, transaction))
        {
            await deleteSubclasses.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var deleteHierarchy = new NpgsqlCommand("delete from product_hierarchy_catalog;", connection, transaction))
        {
            await deleteHierarchy.ExecuteNonQueryAsync(cancellationToken);
        }

        var profiles = await LoadProductProfilesPageDirectAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
        foreach (var hierarchyRow in profiles
                     .Select(profile => new ProductHierarchyCatalogRecord(profile.DptNo, profile.ClssNo, profile.Department, profile.Class, profile.ProdGroup ?? "UNASSIGNED", profile.IsActive))
                     .Distinct())
        {
            await UpsertProductHierarchyCatalogInternalDirectAsync(connection, transaction, hierarchyRow, cancellationToken);
        }

        foreach (var subclassRow in profiles
                     .Select(profile => new ProductSubclassCatalogRecord(profile.Department, profile.Class, profile.Subclass, profile.IsActive))
                     .Distinct())
        {
            await using var command = new NpgsqlCommand(
                """
                insert into product_subclass_catalog (department, class, subclass, is_active)
                values (@department, @class, @subclass, @isActive)
                on conflict (department, class, subclass)
                do update set is_active = excluded.is_active;
                """,
                connection,
                transaction);
            command.Parameters.AddWithValue("@department", subclassRow.Department);
            command.Parameters.AddWithValue("@class", subclassRow.Class);
            command.Parameters.AddWithValue("@subclass", subclassRow.Subclass);
            command.Parameters.AddWithValue("@isActive", subclassRow.IsActive ? 1 : 0);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task EnsureProductProfileOptionSeedDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using (var deleteCommand = new NpgsqlCommand("delete from product_profile_options;", connection, transaction))
        {
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var profiles = await LoadProductProfilesPageDirectAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
        foreach (var profile in profiles)
        {
            await UpsertProductProfileOptionSeedsDirectAsync(connection, transaction, profile, cancellationToken);
        }
    }

    private static async Task RebuildPlanningFromMasterDataInternalDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        int fiscalYear,
        CancellationToken cancellationToken)
    {
        var stores = (await LoadStoreMetadataDirectAsync(connection, transaction, cancellationToken)).Values
            .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var hierarchyRows = (await LoadProductHierarchyCatalogRecordsDirectAsync(connection, transaction, cancellationToken))
            .Where(row => row.IsActive)
            .OrderBy(row => row.Department, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Class, StringComparer.OrdinalIgnoreCase)
            .ToList();
        var subclassRows = (await LoadProductSubclassCatalogRecordsDirectAsync(connection, transaction, cancellationToken))
            .Where(row => row.IsActive)
            .OrderBy(row => row.Department, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Class, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Subclass, StringComparer.OrdinalIgnoreCase)
            .ToList();

        await ClearPlanningCommandHistoryDirectAsync(connection, transaction, scenarioVersionId, cancellationToken);

        foreach (var tableName in new[] { "planning_cells", "product_nodes", "hierarchy_subclasses_v2", "hierarchy_classes_v2", "hierarchy_departments_v2", "hierarchy_subcategories", "hierarchy_categories", "time_periods" })
        {
            await using var deleteCommand = new NpgsqlCommand($"delete from {tableName};", connection, transaction);
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var period in BuildYearPeriodsDirect(fiscalYear).Values.OrderBy(period => period.SortOrder))
        {
            await InsertTimePeriodDirectAsync(connection, transaction, period, cancellationToken);
        }

        var timePeriods = await LoadTimePeriodsDirectAsync(connection, transaction, cancellationToken);
        var nextProductNodeId = stores.Count == 0 ? 100L : stores.Max(store => store.StoreId);
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
            await InsertProductNodeDirectAsync(connection, transaction, rootNode, cancellationToken);
            await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, rootNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);

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
                await InsertProductNodeDirectAsync(connection, transaction, departmentNode, cancellationToken);
                await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, departmentNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);

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
                    await InsertProductNodeDirectAsync(connection, transaction, classNode, cancellationToken);
                    await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, classNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);

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
                        await InsertProductNodeDirectAsync(connection, transaction, subclassNode, cancellationToken);
                        await InitializeCellsForNodeDirectAsync(connection, transaction, scenarioVersionId, subclassNode, SupportedMeasureIdsDirect, timePeriods.Values, cancellationToken);
                    }
                }
            }
        }

        var productNodes = await LoadProductNodesDirectAsync(connection, transaction, cancellationToken);
        var profiles = await LoadProductProfilesPageDirectAsync(connection, transaction, null, int.MaxValue, 0, cancellationToken);
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

        var workingCells = (await LoadScenarioCellsDirectAsync(connection, transaction, scenarioVersionId, cancellationToken))
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
        var monthPeriods = timePeriods.Values
            .Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
            .OrderBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            var subclassKey = (Department: leafNode.Path[1], Class: leafNode.Path[2], Subclass: leafNode.Path[3]);
            var averages = subclassAverages.GetValueOrDefault(subclassKey) ?? new { Price = 10.00m, Cost = 6.00m };

            foreach (var period in monthPeriods)
            {
                var quantity = BuildMockQuantityDirect(leafNode.StoreId, subclassKey.Department, subclassKey.Class, subclassKey.Subclass, period.TimePeriodId);
                var revenue = PlanningMath.CalculateRevenue(quantity, averages.Price);
                var totalCosts = PlanningMath.CalculateTotalCosts(quantity, averages.Cost);
                var grossProfit = PlanningMath.CalculateGrossProfit(quantity, averages.Price, averages.Cost);
                var grossProfitPercent = PlanningMath.CalculateGrossProfitPercent(averages.Price, averages.Cost);

                SetSeedInputValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], quantity);
                SetSeedInputValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], averages.Price);
                SetSeedInputValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], averages.Cost);
                SetSeedInputValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], revenue);
                SetSeedCalculatedValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], totalCosts, true);
                SetSeedCalculatedValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfit, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], grossProfit, true);
                SetSeedCalculatedValueDirect(workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfitPercent, leafNode.StoreId, leafNode.ProductNodeId, period.TimePeriodId).Key], grossProfitPercent, true);
            }
        }

        RecalculateSeedTotalsDirect(scenarioVersionId, PlanningMeasures.SalesRevenue, workingCells, productNodes, timePeriods);
        RecalculateSeedTotalsDirect(scenarioVersionId, PlanningMeasures.SoldQuantity, workingCells, productNodes, timePeriods);
        RecalculateSeedTotalsDirect(scenarioVersionId, PlanningMeasures.TotalCosts, workingCells, productNodes, timePeriods);
        RecalculateSeedTotalsDirect(scenarioVersionId, PlanningMeasures.GrossProfit, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotalsDirect(scenarioVersionId, PlanningMeasures.AverageSellingPrice, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotalsDirect(scenarioVersionId, PlanningMeasures.UnitCost, workingCells, productNodes, timePeriods);
        RecalculateSeedDerivedRateTotalsDirect(scenarioVersionId, PlanningMeasures.GrossProfitPercent, workingCells, productNodes, timePeriods);

        await RebuildHierarchyMappingsDirectAsync(connection, transaction, cancellationToken);
        await UpsertPlanningCellsAsync(connection, transaction, workingCells.Values, cancellationToken);
    }

    private static decimal BuildMockQuantityDirect(long storeId, string department, string classLabel, string subclass, long timePeriodId)
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

    private static void SetSeedInputValueDirect(PlanningCell cell, decimal value)
    {
        cell.InputValue = value;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.RowVersion = Math.Max(cell.RowVersion, 2);
        cell.CellKind = "input";
    }

    private static void SetSeedCalculatedValueDirect(PlanningCell cell, decimal value, bool keepCalculated)
    {
        cell.InputValue = null;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.RowVersion = Math.Max(cell.RowVersion, 2);
        cell.CellKind = keepCalculated ? "calculated" : "input";
    }

    private static void RecalculateSeedTotalsDirect(
        long scenarioVersionId,
        long measureId,
        IDictionary<string, PlanningCell> cells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        foreach (var storeId in productNodes.Values.Select(node => node.StoreId).Distinct())
        {
            foreach (var period in timePeriods.Values.OrderByDescending(period => period.SortOrder))
            {
                foreach (var node in productNodes.Values.Where(node => node.StoreId == storeId).OrderByDescending(node => node.Level))
                {
                    var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, node.ProductNodeId, period.TimePeriodId);
                    if (!cells.TryGetValue(coordinate.Key, out var cell))
                    {
                        continue;
                    }

                    if (node.IsLeaf && string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    decimal total;
                    if (string.Equals(period.Grain, "year", StringComparison.OrdinalIgnoreCase))
                    {
                        total = timePeriods.Values
                            .Where(candidate => candidate.ParentTimePeriodId == period.TimePeriodId)
                            .Select(candidate => cells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, node.ProductNodeId, candidate.TimePeriodId).Key].EffectiveValue)
                            .Sum();
                    }
                    else
                    {
                        total = productNodes.Values
                            .Where(candidate => candidate.StoreId == storeId && candidate.ParentProductNodeId == node.ProductNodeId)
                            .Select(candidate => cells[new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, candidate.ProductNodeId, period.TimePeriodId).Key].EffectiveValue)
                            .Sum();
                    }

                    SetSeedCalculatedValueDirect(cell, total, true);
                }
            }
        }
    }

    private static void RecalculateSeedDerivedRateTotalsDirect(
        long scenarioVersionId,
        long measureId,
        IDictionary<string, PlanningCell> cells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        foreach (var storeId in productNodes.Values.Select(node => node.StoreId).Distinct())
        {
            foreach (var period in timePeriods.Values.OrderByDescending(period => period.SortOrder))
            {
                foreach (var node in productNodes.Values.Where(node => node.StoreId == storeId).OrderByDescending(node => node.Level))
                {
                    var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, storeId, node.ProductNodeId, period.TimePeriodId);
                    if (!cells.TryGetValue(coordinate.Key, out var cell))
                    {
                        continue;
                    }

                    if (node.IsLeaf && string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    var quantity = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, storeId, node.ProductNodeId, period.TimePeriodId).Key].EffectiveValue;
                    var revenue = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, storeId, node.ProductNodeId, period.TimePeriodId).Key].EffectiveValue;
                    var totalCosts = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, storeId, node.ProductNodeId, period.TimePeriodId).Key].EffectiveValue;
                    var grossProfit = cells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfit, storeId, node.ProductNodeId, period.TimePeriodId).Key].EffectiveValue;

                    var value = measureId switch
                    {
                        var id when id == PlanningMeasures.AverageSellingPrice => PlanningMath.DeriveAspFromRevenue(revenue, quantity),
                        var id when id == PlanningMeasures.UnitCost => quantity <= 0m ? 0m : PlanningMath.NormalizeUnitCost(totalCosts / quantity),
                        var id when id == PlanningMeasures.GrossProfitPercent => revenue <= 0m ? 0m : PlanningMath.NormalizeGrossProfitPercent((grossProfit / revenue) * 100m),
                        _ => cell.EffectiveValue
                    };

                    SetSeedCalculatedValueDirect(cell, value, true);
                }
            }
        }
    }

    private static IEnumerable<ProductProfileMetadata> BuildSeedProductProfilesDirect()
    {
        var classCodes = new Dictionary<(string Department, string Class), (string DptNo, string ClssNo)>();
        var departmentNumbers = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var leafNode in BuildProductNodesSeedDirect().Values
                     .Where(node => node.IsLeaf)
                     .OrderBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase))
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
                var classIndex = classCodes.Count(code => code.Key.Department.Equals(department, StringComparison.OrdinalIgnoreCase)) + 1;
                var clssNo = $"{departmentNumbers[department]}{classIndex:00}";
                classCodes[(department, classLabel)] = (dptNo, clssNo);
            }

            var codes = classCodes[(department, classLabel)];
            yield return new ProductProfileMetadata(
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
        }
    }

    private static IReadOnlyList<StoreNodeMetadata> BuildStoreMetadataSeedDirect() =>
    [
        new StoreNodeMetadata(101, "Store A", "Baby Mart", "Central", "active", "new-store-ramp", null, null),
        new StoreNodeMetadata(102, "Store B", "Baby Mall", "South", "active", "new-store-ramp", null, null)
    ];

    private static Dictionary<long, ProductNode> BuildProductNodesSeedDirect()
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
}
