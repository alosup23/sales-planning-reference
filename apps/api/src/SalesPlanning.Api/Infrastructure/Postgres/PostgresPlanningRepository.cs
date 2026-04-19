using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using SalesPlanning.Api.Security;
using System.Diagnostics;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresPlanningRepository : IPlanningRepository
{
    private const int BulkWriteChunkSize = 500;
    private static readonly string[] StoreMutationTables = ["store_metadata", "store_profile_options", "product_nodes", "planning_cells"];
    private static readonly string[] HierarchyTables =
    [
        "hierarchy_categories",
        "hierarchy_subcategories",
        "hierarchy_departments_v2",
        "hierarchy_classes_v2",
        "hierarchy_subclasses_v2"
    ];
    private static readonly string[] ProductMutationTables =
    [
        "product_profiles",
        "product_profile_options",
        "product_hierarchy_catalog",
        "product_subclass_catalog",
        "product_nodes",
        "planning_cells",
        "hierarchy_categories",
        "hierarchy_subcategories",
        "hierarchy_departments_v2",
        "hierarchy_classes_v2",
        "hierarchy_subclasses_v2"
    ];
    private static readonly string[] InventoryMutationTables = ["inventory_profiles"];
    private static readonly string[] PricingMutationTables = ["pricing_policies"];
    private static readonly string[] SeasonalityMutationTables = ["seasonality_event_profiles"];
    private static readonly string[] VendorMutationTables = ["vendor_supply_profiles"];
    private static readonly string[] ProductNodeMutationTables = ["product_nodes", "planning_cells", "hierarchy_categories", "hierarchy_subcategories", "hierarchy_departments_v2", "hierarchy_classes_v2", "hierarchy_subclasses_v2"];
    private static readonly string[] TimePeriodMutationTables = ["time_periods", "planning_cells"];

    private readonly string _connectionString;
    private readonly ILogger<PostgresPlanningRepository> _logger;
    private readonly bool _applyMigrationsOnStartup;
    private readonly string _migrationsDirectory;
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly AsyncLocal<int> _atomicDepth = new();
    private readonly AsyncLocal<PostgresDirectAtomicContext?> _directAtomicContext = new();
    private readonly object _readCacheGate = new();
    private bool _databaseReady;
    private PlanningMetadataSnapshot? _metadataCache;
    private IReadOnlyList<StoreNodeMetadata>? _storeListCache;
    private IReadOnlyDictionary<long, long>? _storeRootProductNodeIdsCache;
    private IReadOnlyList<HierarchyDepartmentRecord>? _hierarchyMappingsCache;

    public PostgresPlanningRepository(
        string connectionString,
        ILogger<PostgresPlanningRepository> logger,
        bool applyMigrationsOnStartup,
        string migrationsDirectory)
    {
        _connectionString = connectionString;
        _logger = logger;
        _applyMigrationsOnStartup = applyMigrationsOnStartup;
        _migrationsDirectory = migrationsDirectory;
    }

    private PlanningMetadataSnapshot? GetCachedMetadataSnapshot()
    {
        lock (_readCacheGate)
        {
            return _metadataCache;
        }
    }

    private void SetCachedMetadataSnapshot(PlanningMetadataSnapshot snapshot)
    {
        lock (_readCacheGate)
        {
            _metadataCache = snapshot;
            _storeListCache = snapshot.Stores.Values
                .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
                .ToList();
            _storeRootProductNodeIdsCache = snapshot.ProductNodes.Values
                .Where(node => node.Level == 0)
                .GroupBy(node => node.StoreId)
                .ToDictionary(
                    group => group.Key,
                    group => group.OrderBy(node => node.ProductNodeId).First().ProductNodeId);
        }
    }

    private IReadOnlyList<StoreNodeMetadata>? GetCachedStoreList()
    {
        lock (_readCacheGate)
        {
            return _storeListCache;
        }
    }

    private IReadOnlyDictionary<long, long>? GetCachedStoreRoots()
    {
        lock (_readCacheGate)
        {
            return _storeRootProductNodeIdsCache;
        }
    }

    private IReadOnlyList<HierarchyDepartmentRecord>? GetCachedHierarchyMappings()
    {
        lock (_readCacheGate)
        {
            return _hierarchyMappingsCache;
        }
    }

    private void SetCachedHierarchyMappings(IReadOnlyList<HierarchyDepartmentRecord> mappings)
    {
        lock (_readCacheGate)
        {
            _hierarchyMappingsCache = mappings;
        }
    }

    private void InvalidateMetadataCaches()
    {
        lock (_readCacheGate)
        {
            _metadataCache = null;
            _storeListCache = null;
            _storeRootProductNodeIdsCache = null;
        }
    }

    private void InvalidateHierarchyCache()
    {
        lock (_readCacheGate)
        {
            _hierarchyMappingsCache = null;
        }
    }

    private void InvalidateReadCaches(params string[] tableNames)
    {
        var invalidateMetadata = false;
        var invalidateHierarchy = false;

        foreach (var tableName in tableNames)
        {
            if (string.Equals(tableName, "product_nodes", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "time_periods", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "store_metadata", StringComparison.OrdinalIgnoreCase))
            {
                invalidateMetadata = true;
            }

            if (string.Equals(tableName, "hierarchy_departments_v2", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "hierarchy_classes_v2", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "hierarchy_subclasses_v2", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "hierarchy_categories", StringComparison.OrdinalIgnoreCase)
                || string.Equals(tableName, "hierarchy_subcategories", StringComparison.OrdinalIgnoreCase))
            {
                invalidateHierarchy = true;
            }
        }

        if (invalidateMetadata)
        {
            InvalidateMetadataCaches();
        }

        if (invalidateHierarchy)
        {
            InvalidateHierarchyCache();
        }
    }

    private async Task WithMutationAndCacheInvalidationAsync(
        Func<CancellationToken, Task> action,
        IReadOnlyCollection<string> tableNames,
        CancellationToken cancellationToken)
    {
        await action(cancellationToken);
        InvalidateReadCaches(tableNames.ToArray());
    }

    private async Task<T> WithMutationAndCacheInvalidationAsync<T>(
        Func<CancellationToken, Task<T>> action,
        IReadOnlyCollection<string> tableNames,
        CancellationToken cancellationToken)
    {
        var result = await action(cancellationToken);
        InvalidateReadCaches(tableNames.ToArray());
        return result;
    }

    public Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        return ExecuteAtomicCoreAsync(action, cancellationToken);
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) =>
        GetMetadataDirectAsync(cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        GetCellsDirectAsync(coordinates, cancellationToken);

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) =>
        GetCellDirectAsync(coordinate, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        GetScenarioCellsDirectAsync(scenarioVersionId, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        GetDraftCellsDirectAsync(scenarioVersionId, PlanningUserIdentity.ParsePlanningUserToken(userId), coordinates, cancellationToken);

    public async Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var materialized = cells.Select(cell => cell.Clone()).ToList();
        await ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertPlanningCellsAsync(connection, transaction, materialized, ct),
            cancellationToken);
    }

    public async Task UpsertDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var userContext = PlanningUserIdentity.ParsePlanningUserToken(userId);
        var materialized = cells.Select(cell => cell.Clone()).ToList();
        var stopwatch = Stopwatch.StartNew();
        await ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => UpsertDraftPlanningCellsAsync(connection, transaction, scenarioVersionId, userContext, materialized, ct),
            cancellationToken);
        _logger.LogInformation(
            "Upserted {DraftCellCount} planning draft cells for scenario {ScenarioVersionId} user {UserId} in {ElapsedMs} ms.",
            materialized.Count,
            scenarioVersionId,
            userContext.PrimaryUserId,
            stopwatch.ElapsedMilliseconds);
    }

    public async Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        await ExecuteDirectMutationAsync(
            (connection, transaction, ct) => AppendAuditDirectAsync(connection, transaction, audit, ct),
            cancellationToken);
    }

    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) =>
        GetNextActionIdDirectAsync(cancellationToken);

    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) =>
        GetAuditDirectAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);

    public Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken) =>
        GetNextCommandBatchIdDirectAsync(cancellationToken);

    public async Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken)
    {
        await ExecuteDirectMutationAsync(
            (connection, transaction, ct) => AppendCommandBatchDirectAsync(connection, transaction, batch, ct),
            cancellationToken);
    }

    public Task<long> GetNextDraftCommandBatchIdAsync(CancellationToken cancellationToken) =>
        GetNextDraftCommandBatchIdDirectAsync(cancellationToken);

    public async Task AppendDraftCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        await ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => AppendDraftCommandBatchDirectAsync(connection, transaction, batch, ct),
            cancellationToken);
        _logger.LogInformation(
            "Appended planning draft command batch {CommandBatchId} ({CommandKind}) with {DeltaCount} deltas for scenario {ScenarioVersionId} user {UserId} in {ElapsedMs} ms.",
            batch.CommandBatchId,
            batch.CommandKind,
            batch.Deltas.Count,
            batch.ScenarioVersionId,
            batch.UserId,
            stopwatch.ElapsedMilliseconds);
    }

    public Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        GetUndoRedoAvailabilityDirectAsync(scenarioVersionId, userId, limit, cancellationToken);

    public Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UndoLatestCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, ct),
            cancellationToken);

    public Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => RedoLatestCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, ct),
            cancellationToken);

    public Task<PlanningUndoRedoAvailability> GetDraftUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        GetDraftUndoRedoAvailabilityDirectAsync(scenarioVersionId, PlanningUserIdentity.ParsePlanningUserToken(userId).PrimaryUserId, limit, cancellationToken);

    public Task<PlanningCommandBatch?> UndoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => UndoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, PlanningUserIdentity.ParsePlanningUserToken(userId).PrimaryUserId, limit, ct),
            cancellationToken);

    public Task<PlanningCommandBatch?> RedoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => RedoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, PlanningUserIdentity.ParsePlanningUserToken(userId).PrimaryUserId, limit, ct),
            cancellationToken);

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken) =>
        GetGridSliceDirectAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, cancellationToken);

    public Task<GridBranchResponse> GetGridBranchRowsAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken) =>
        GetGridBranchRowsDirectAsync(scenarioVersionId, parentProductNodeId, cancellationToken);

    public async Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        var node = await AddRowDirectAsync(request, cancellationToken);
        InvalidateReadCaches(ProductNodeMutationTables);
        return node;
    }

    public async Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken)
    {
        var deletedCount = await DeleteRowDirectAsync(scenarioVersionId, productNodeId, cancellationToken);
        InvalidateReadCaches(ProductNodeMutationTables);
        return deletedCount;
    }

    public async Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken)
    {
        var deletedCount = await DeleteYearDirectAsync(scenarioVersionId, yearTimePeriodId, cancellationToken);
        InvalidateReadCaches(TimePeriodMutationTables);
        return deletedCount;
    }

    public async Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken)
    {
        await EnsureYearDirectAsync(scenarioVersionId, fiscalYear, cancellationToken);
        InvalidateReadCaches(TimePeriodMutationTables);
    }

    public async Task CommitDraftAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken)
    {
        var userContext = PlanningUserIdentity.ParsePlanningUserToken(userId);
        var stopwatch = Stopwatch.StartNew();
        await ExecuteDirectMutationAsync(
            (connection, transaction, ct) => CommitDraftDirectAsync(connection, transaction, scenarioVersionId, userContext, ct),
            cancellationToken);
        InvalidateReadCaches("planning_cells");
        _logger.LogInformation(
            "Committed planning draft for scenario {ScenarioVersionId} user {UserId} in {ElapsedMs} ms.",
            scenarioVersionId,
            userContext.PrimaryUserId,
            stopwatch.ElapsedMilliseconds);
    }

    public Task RecordSaveCheckpointAsync(long scenarioVersionId, string userId, string mode, DateTimeOffset savedAt, CancellationToken cancellationToken) =>
        RecordSaveCheckpointDirectAsync(scenarioVersionId, userId, mode, savedAt, cancellationToken);

    public async Task SaveScenarioAsync(long scenarioVersionId, string userId, string mode, DateTimeOffset savedAt, PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        var userContext = PlanningUserIdentity.ParsePlanningUserToken(userId);
        var stopwatch = Stopwatch.StartNew();

        await ExecuteDirectMutationAsync(
            async (connection, transaction, ct) =>
            {
                await CommitDraftDirectAsync(connection, transaction, scenarioVersionId, userContext, ct);
                await RecordSaveCheckpointDirectAsync(connection, transaction, scenarioVersionId, userContext.PrimaryUserId, mode, savedAt, ct);
                await AppendAuditDirectAsync(connection, transaction, audit, ct);
            },
            cancellationToken);

        var remainingDraftRows = await ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand(
                    """
                    select count(*)
                    from planning_draft_cells
                    where scenario_version_id = @scenarioVersionId
                      and user_id = any(@candidateUserIds);
                    """,
                    connection,
                    transaction);
                command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                command.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
                return Convert.ToInt64(await command.ExecuteScalarAsync(ct) ?? 0L);
            },
            cancellationToken);

        if (remainingDraftRows != 0)
        {
            _logger.LogError(
                "Save verification failed for scenario {ScenarioVersionId} user {UserId}; {RemainingDraftRows} draft rows remained visible after commit.",
                scenarioVersionId,
                userContext.PrimaryUserId,
                remainingDraftRows);
            throw new InvalidOperationException($"Draft rows remained visible after save for scenario {scenarioVersionId}.");
        }

        InvalidateReadCaches("planning_cells");
        _logger.LogInformation(
            "Saved scenario {ScenarioVersionId} for user {UserId} in {ElapsedMs} ms.",
            scenarioVersionId,
            userContext.PrimaryUserId,
            stopwatch.ElapsedMilliseconds);
    }

    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) =>
        GetStoresDirectAsync(cancellationToken);

    public Task<IReadOnlyDictionary<long, long>> GetStoreRootProductNodeIdsAsync(CancellationToken cancellationToken) =>
        GetStoreRootProductNodeIdsDirectAsync(cancellationToken);

    public Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertStoreProfileDirectAsync(scenarioVersionId, storeProfile, ct),
            StoreMutationTables,
            cancellationToken);

    public Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => DeleteStoreProfileDirectAsync(scenarioVersionId, storeId, ct),
            StoreMutationTables,
            cancellationToken);

    public Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => InactivateStoreProfileDirectAsync(storeId, ct),
            ["store_metadata"],
            cancellationToken);

    public Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken) =>
        GetStoreProfileOptionsDirectAsync(cancellationToken);

    public Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertStoreProfileOptionDirectAsync(fieldName, value, isActive, ct),
            ["store_profile_options"],
            cancellationToken);

    public Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => DeleteStoreProfileOptionDirectAsync(fieldName, value, ct),
            ["store_profile_options"],
            cancellationToken);

    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) =>
        GetHierarchyMappingsDirectAsync(cancellationToken);

    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertHierarchyDepartmentDirectAsync(departmentLabel, ct),
            HierarchyTables,
            cancellationToken);

    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertHierarchyClassDirectAsync(departmentLabel, classLabel, ct),
            HierarchyTables,
            cancellationToken);

    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertHierarchySubclassDirectAsync(departmentLabel, classLabel, subclassLabel, ct),
            HierarchyTables,
            cancellationToken);

    public Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        GetProductProfilesDirectAsync(searchTerm, pageNumber, pageSize, cancellationToken);

    public Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertProductProfileDirectAsync(profile, ct),
            ProductMutationTables,
            cancellationToken);

    public Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => DeleteProductProfileDirectAsync(skuVariant, ct),
            ProductMutationTables,
            cancellationToken);

    public Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => InactivateProductProfileDirectAsync(skuVariant, ct),
            ProductMutationTables,
            cancellationToken);

    public Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken) =>
        GetProductProfileOptionsDirectAsync(cancellationToken);

    public Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertProductProfileOptionDirectAsync(fieldName, value, isActive, ct),
            ["product_profile_options"],
            cancellationToken);

    public Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => DeleteProductProfileOptionDirectAsync(fieldName, value, ct),
            ["product_profile_options"],
            cancellationToken);

    public Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken) =>
        GetProductHierarchyCatalogDirectAsync(cancellationToken);

    public Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken) =>
        GetProductSubclassCatalogDirectAsync(cancellationToken);

    public Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => UpsertProductHierarchyCatalogDirectAsync(record, ct),
            ProductMutationTables,
            cancellationToken);

    public Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => DeleteProductHierarchyCatalogDirectAsync(dptNo, clssNo, ct),
            ProductMutationTables,
            cancellationToken);

    public Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => ReplaceProductMasterDataDirectAsync(hierarchyRows, profiles, ct),
            ProductMutationTables,
            cancellationToken);

    public Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> GetInventoryProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        GetInventoryProfilesDirectAsync(searchTerm, pageNumber, pageSize, cancellationToken);

    public Task<InventoryProfileRecord> GetInventoryProfileByIdAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        GetInventoryProfileByIdDirectAsync(inventoryProfileId, cancellationToken);

    public Task<InventoryProfileRecord> UpsertInventoryProfileAsync(InventoryProfileRecord profile, CancellationToken cancellationToken) =>
        UpsertInventoryProfileDirectAsync(profile, cancellationToken);

    public Task DeleteInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        DeleteInventoryProfileDirectAsync(inventoryProfileId, cancellationToken);

    public Task InactivateInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        InactivateInventoryProfileDirectAsync(inventoryProfileId, cancellationToken);

    public Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> GetPricingPoliciesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        GetPricingPoliciesDirectAsync(searchTerm, pageNumber, pageSize, cancellationToken);

    public Task<PricingPolicyRecord> GetPricingPolicyByIdAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        GetPricingPolicyByIdDirectAsync(pricingPolicyId, cancellationToken);

    public Task<PricingPolicyRecord> UpsertPricingPolicyAsync(PricingPolicyRecord policy, CancellationToken cancellationToken) =>
        UpsertPricingPolicyDirectAsync(policy, cancellationToken);

    public Task DeletePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        DeletePricingPolicyDirectAsync(pricingPolicyId, cancellationToken);

    public Task InactivatePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        InactivatePricingPolicyDirectAsync(pricingPolicyId, cancellationToken);

    public Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> GetSeasonalityEventProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        GetSeasonalityEventProfilesDirectAsync(searchTerm, pageNumber, pageSize, cancellationToken);

    public Task<SeasonalityEventProfileRecord> GetSeasonalityEventProfileByIdAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        GetSeasonalityEventProfileByIdDirectAsync(seasonalityEventProfileId, cancellationToken);

    public Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileAsync(SeasonalityEventProfileRecord profile, CancellationToken cancellationToken) =>
        UpsertSeasonalityEventProfileDirectAsync(profile, cancellationToken);

    public Task DeleteSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        DeleteSeasonalityEventProfileDirectAsync(seasonalityEventProfileId, cancellationToken);

    public Task InactivateSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        InactivateSeasonalityEventProfileDirectAsync(seasonalityEventProfileId, cancellationToken);

    public Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> GetVendorSupplyProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        GetVendorSupplyProfilesDirectAsync(searchTerm, pageNumber, pageSize, cancellationToken);

    public Task<VendorSupplyProfileRecord> GetVendorSupplyProfileByIdAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        GetVendorSupplyProfileByIdDirectAsync(vendorSupplyProfileId, cancellationToken);

    public Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileAsync(VendorSupplyProfileRecord profile, CancellationToken cancellationToken) =>
        UpsertVendorSupplyProfileDirectAsync(profile, cancellationToken);

    public Task DeleteVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        DeleteVendorSupplyProfileDirectAsync(vendorSupplyProfileId, cancellationToken);

    public Task InactivateVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        InactivateVendorSupplyProfileDirectAsync(vendorSupplyProfileId, cancellationToken);

    public Task RebuildPlanningFromMasterDataAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => RebuildPlanningFromMasterDataDirectAsync(scenarioVersionId, fiscalYear, ct),
            ProductMutationTables.Concat(TimePeriodMutationTables).Concat(StoreMutationTables).Concat(HierarchyTables).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            cancellationToken);

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) =>
        FindProductNodeByPathDirectAsync(path, cancellationToken);

    public Task ResetAsync(CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => ResetDirectAsync(ct),
            ProductMutationTables.Concat(TimePeriodMutationTables).Concat(StoreMutationTables).Concat(HierarchyTables).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            cancellationToken);

    public async Task ApplyMigrationsAsync(string migrationsDirectory, CancellationToken cancellationToken)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        var migrator = new PostgresMigrationRunner(_connectionString, migrationsDirectory, loggerFactory.CreateLogger<PostgresMigrationRunner>());
        await migrator.ApplyMigrationsAsync(cancellationToken);
    }

    private async Task<T> ExecuteAtomicCoreAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        var isOutermost = _atomicDepth.Value == 0;
        PostgresDirectAtomicContext? outerContext = null;
        if (isOutermost)
        {
            _directAtomicContext.Value = null;
            outerContext = await GetOrCreateDirectAtomicContextAsync(cancellationToken);
        }

        _atomicDepth.Value += 1;
        var succeeded = false;

        try
        {
            var result = await action(cancellationToken);
            succeeded = true;
            return result;
        }
        finally
        {
            _atomicDepth.Value -= 1;
            if (isOutermost)
            {
                var directContext = outerContext ?? _directAtomicContext.Value;
                _directAtomicContext.Value = null;
                try
                {
                    if (directContext is not null)
                    {
                        if (succeeded)
                        {
                            if (directContext.HasMutations)
                            {
                                await UpdateDataVersionAsync(directContext.Connection, directContext.Transaction, CancellationToken.None);
                            }

                            await directContext.Transaction.CommitAsync(CancellationToken.None);
                        }
                        else
                        {
                            await directContext.Transaction.RollbackAsync(CancellationToken.None);
                        }
                    }
                }
                finally
                {
                    if (directContext is not null)
                    {
                        await directContext.DisposeAsync();
                    }
                }
            }
        }
    }

    private async Task<NpgsqlConnection> OpenPostgresConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private async Task<PostgresDirectAtomicContext> GetOrCreateDirectAtomicContextAsync(CancellationToken cancellationToken)
    {
        var current = _directAtomicContext.Value;
        if (current is not null)
        {
            return current;
        }

        await EnsureDatabaseReadyAsync(cancellationToken);
        var connection = await OpenPostgresConnectionAsync(cancellationToken);
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        current = new PostgresDirectAtomicContext(connection, transaction);
        _directAtomicContext.Value = current;
        return current;
    }

    private async Task<T> ExecuteDirectReadAsync<T>(Func<NpgsqlConnection, NpgsqlTransaction?, CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        var current = _directAtomicContext.Value;
        if (current is not null)
        {
            return await action(current.Connection, current.Transaction, cancellationToken);
        }

        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        return await action(connection, null, cancellationToken);
    }

    private async Task ExecuteDirectMutationAsync(Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        if (_atomicDepth.Value > 0)
        {
            var current = await GetOrCreateDirectAtomicContextAsync(cancellationToken);
            await action(current.Connection, current.Transaction, cancellationToken);
            current.HasMutations = true;
            return;
        }

        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await action(connection, transaction, cancellationToken);
        await UpdateDataVersionAsync(connection, transaction, CancellationToken.None);
        await transaction.CommitAsync(CancellationToken.None);
    }

    private async Task ExecuteDirectNonVersionedMutationAsync(Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        if (_atomicDepth.Value > 0)
        {
            var current = await GetOrCreateDirectAtomicContextAsync(cancellationToken);
            await action(current.Connection, current.Transaction, cancellationToken);
            return;
        }

        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await action(connection, transaction, cancellationToken);
        await transaction.CommitAsync(CancellationToken.None);
    }

    private async Task<T> ExecuteDirectNonVersionedMutationAsync<T>(Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        if (_atomicDepth.Value > 0)
        {
            var current = await GetOrCreateDirectAtomicContextAsync(cancellationToken);
            return await action(current.Connection, current.Transaction, cancellationToken);
        }

        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var result = await action(connection, transaction, cancellationToken);
        await transaction.CommitAsync(CancellationToken.None);
        return result;
    }

    private async Task<T> ExecuteDirectMutationAsync<T>(Func<NpgsqlConnection, NpgsqlTransaction, CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        if (_atomicDepth.Value > 0)
        {
            var current = await GetOrCreateDirectAtomicContextAsync(cancellationToken);
            var result = await action(current.Connection, current.Transaction, cancellationToken);
            current.HasMutations = true;
            return result;
        }

        await EnsureDatabaseReadyAsync(cancellationToken);
        await using var connection = await OpenPostgresConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var directResult = await action(connection, transaction, cancellationToken);
        await UpdateDataVersionAsync(connection, transaction, CancellationToken.None);
        await transaction.CommitAsync(CancellationToken.None);
        return directResult;
    }

    private async Task EnsureDatabaseReadyAsync(CancellationToken cancellationToken)
    {
        if (_databaseReady)
        {
            return;
        }

        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            if (_databaseReady)
            {
                return;
            }

            if (_applyMigrationsOnStartup)
            {
                _logger.LogInformation("Applying PostgreSQL migrations from {MigrationsDirectory}.", _migrationsDirectory);
                var migrator = new PostgresMigrationRunner(_connectionString, _migrationsDirectory, LoggerFactory.Create(builder => { }).CreateLogger<PostgresMigrationRunner>());
                await migrator.ApplyMigrationsAsync(cancellationToken);
                _logger.LogInformation("PostgreSQL migrations are up to date.");
            }

            _databaseReady = true;
            _logger.LogInformation("PostgreSQL repository is ready for use.");
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task<long> GetRemoteDataVersionAsync(CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(
            "select data_version from planning_data_state where state_key = 'default';",
            connection);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        if (value is null or DBNull)
        {
            return 0;
        }

        return Convert.ToInt64(value);
    }

    private static NpgsqlParameter CreateArrayParameter(string parameterName, NpgsqlDbType elementType, Array values) =>
        new(parameterName, NpgsqlDbType.Array | elementType)
        {
            Value = values
        };

    private static async Task UpsertPlanningCellsAsync(NpgsqlConnection postgres, NpgsqlTransaction transaction, IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var cellList = cells as IReadOnlyList<PlanningCell> ?? cells.ToList();
        if (cellList.Count == 0)
        {
            return;
        }

        var orderedCells = cellList
            .OrderBy(cell => cell.Coordinate.ScenarioVersionId)
            .ThenBy(cell => cell.Coordinate.MeasureId)
            .ThenBy(cell => cell.Coordinate.StoreId)
            .ThenBy(cell => cell.Coordinate.ProductNodeId)
            .ThenBy(cell => cell.Coordinate.TimePeriodId)
            .ToList();
        var stageTableName = $"planning_cells_stage_{Guid.NewGuid():N}";
        await using (var createStageCommand = new NpgsqlCommand(
            $"""
            create temp table {stageTableName} (
                scenario_version_id bigint not null,
                measure_id bigint not null,
                store_id bigint not null,
                product_node_id bigint not null,
                time_period_id bigint not null,
                input_value numeric null,
                override_value numeric null,
                is_system_generated_override integer not null,
                derived_value numeric not null,
                effective_value numeric not null,
                growth_factor numeric not null,
                is_locked integer not null,
                lock_reason text null,
                locked_by text null,
                row_version bigint not null,
                cell_kind text not null
            ) on commit drop;
            """,
            postgres,
            transaction))
        {
            createStageCommand.CommandTimeout = 300;
            await createStageCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var importer = await postgres.BeginBinaryImportAsync(
                         $"""
                          copy {stageTableName} (
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
                          from stdin (format binary)
                          """,
                         cancellationToken))
        {
            foreach (var cell in orderedCells)
            {
                await importer.StartRowAsync(cancellationToken);
                await importer.WriteAsync(cell.Coordinate.ScenarioVersionId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.MeasureId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.StoreId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.ProductNodeId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.TimePeriodId, NpgsqlDbType.Bigint, cancellationToken);
                if (cell.InputValue is { } inputValue)
                {
                    await importer.WriteAsync(inputValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (cell.OverrideValue is { } overrideValue)
                {
                    await importer.WriteAsync(overrideValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(cell.IsSystemGeneratedOverride ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync(cell.DerivedValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.EffectiveValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.GrowthFactor, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.IsLocked ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                if (cell.LockReason is { } lockReason)
                {
                    await importer.WriteAsync(lockReason, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (cell.LockedBy is { } lockedBy)
                {
                    await importer.WriteAsync(lockedBy, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(cell.RowVersion, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.CellKind, NpgsqlDbType.Text, cancellationToken);
            }

            await importer.CompleteAsync(cancellationToken);
        }

        await using (var updateCommand = new NpgsqlCommand(
            $"""
            update planning_cells as target
            set input_value = source.input_value,
                override_value = source.override_value,
                is_system_generated_override = source.is_system_generated_override,
                derived_value = source.derived_value,
                effective_value = source.effective_value,
                growth_factor = source.growth_factor,
                is_locked = source.is_locked,
                lock_reason = source.lock_reason,
                locked_by = source.locked_by,
                row_version = source.row_version,
                cell_kind = source.cell_kind
            from {stageTableName} as source
            where target.scenario_version_id = source.scenario_version_id
              and target.measure_id = source.measure_id
              and target.store_id = source.store_id
              and target.product_node_id = source.product_node_id
              and target.time_period_id = source.time_period_id;
            """,
            postgres,
            transaction))
        {
            updateCommand.CommandTimeout = 300;
            await updateCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var insertCommand = new NpgsqlCommand(
            $"""
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
            select
                source.scenario_version_id,
                source.measure_id,
                source.store_id,
                source.product_node_id,
                source.time_period_id,
                source.input_value,
                source.override_value,
                source.is_system_generated_override,
                source.derived_value,
                source.effective_value,
                source.growth_factor,
                source.is_locked,
                source.lock_reason,
                source.locked_by,
                source.row_version,
                source.cell_kind
            from {stageTableName} as source
            left join planning_cells as target
              on target.scenario_version_id = source.scenario_version_id
             and target.measure_id = source.measure_id
             and target.store_id = source.store_id
             and target.product_node_id = source.product_node_id
             and target.time_period_id = source.time_period_id
            where target.scenario_version_id is null;
            """,
            postgres,
            transaction))
        {
            insertCommand.CommandTimeout = 300;
            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static async Task UpdateDataVersionAsync(NpgsqlConnection postgres, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        const string sql = """
            insert into planning_data_state (state_key, data_version, updated_at)
            values ('default', 1, now())
            on conflict (state_key) do update set
                data_version = planning_data_state.data_version + 1,
                updated_at = now();
            """;
        await using var command = new NpgsqlCommand(sql, postgres, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private sealed class PostgresDirectAtomicContext(NpgsqlConnection connection, NpgsqlTransaction transaction) : IAsyncDisposable
    {
        public NpgsqlConnection Connection { get; } = connection;
        public NpgsqlTransaction Transaction { get; } = transaction;
        public bool HasMutations { get; set; }

        public async ValueTask DisposeAsync()
        {
            await Transaction.DisposeAsync();
            await Connection.DisposeAsync();
        }
    }
}
