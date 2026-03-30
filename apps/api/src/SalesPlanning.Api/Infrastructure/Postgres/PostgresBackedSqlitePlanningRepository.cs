using Microsoft.Data.Sqlite;
using Amazon;
using Amazon.S3;
using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using System.Globalization;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository : IPlanningRepository
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

    private readonly SqlitePlanningRepository _innerRepository;
    private readonly string _connectionString;
    private readonly string _localCachePath;
    private readonly string _localVersionPath;
    private readonly ILogger<PostgresBackedSqlitePlanningRepository> _logger;
    private readonly bool _applyMigrationsOnStartup;
    private readonly string _migrationsDirectory;
    private readonly string? _bootstrapS3Bucket;
    private readonly string? _bootstrapS3ObjectKey;
    private readonly string? _bootstrapS3Region;
    private readonly string? _bootstrapSeedKey;
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly AsyncLocal<int> _atomicDepth = new();
    private readonly AsyncLocal<PostgresSyncPlan?> _pendingSyncPlan = new();
    private readonly AsyncLocal<PostgresDirectAtomicContext?> _directAtomicContext = new();
    private readonly object _readCacheGate = new();
    private bool _hydrated;
    private bool _databaseReady;
    private long? _localDataVersion;
    private PlanningMetadataSnapshot? _metadataCache;
    private IReadOnlyList<StoreNodeMetadata>? _storeListCache;
    private IReadOnlyDictionary<long, long>? _storeRootProductNodeIdsCache;
    private IReadOnlyList<HierarchyDepartmentRecord>? _hierarchyMappingsCache;

    public PostgresBackedSqlitePlanningRepository(
        string connectionString,
        ILogger<PostgresBackedSqlitePlanningRepository> logger,
        string localCachePath,
        bool applyMigrationsOnStartup,
        string migrationsDirectory,
        string? bootstrapS3Bucket = null,
        string? bootstrapS3ObjectKey = null,
        string? bootstrapS3Region = null,
        string? bootstrapSeedKey = null)
    {
        _connectionString = connectionString;
        _logger = logger;
        _localCachePath = localCachePath;
        _localVersionPath = $"{localCachePath}.version";
        _applyMigrationsOnStartup = applyMigrationsOnStartup;
        _migrationsDirectory = migrationsDirectory;
        _bootstrapS3Bucket = bootstrapS3Bucket;
        _bootstrapS3ObjectKey = bootstrapS3ObjectKey;
        _bootstrapS3Region = bootstrapS3Region;
        _bootstrapSeedKey = bootstrapSeedKey;
        Directory.CreateDirectory(Path.GetDirectoryName(localCachePath) ?? ".");
        _innerRepository = new SqlitePlanningRepository(localCachePath);
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
        Action<PostgresSyncPlan> queueSync,
        IReadOnlyCollection<string> tableNames,
        CancellationToken cancellationToken)
    {
        await WithMutationAsync(action, queueSync, cancellationToken);
        InvalidateReadCaches(tableNames.ToArray());
    }

    private async Task<T> WithMutationAndCacheInvalidationAsync<T>(
        Func<CancellationToken, Task<T>> action,
        Action<PostgresSyncPlan> queueSync,
        IReadOnlyCollection<string> tableNames,
        CancellationToken cancellationToken)
    {
        var result = await WithMutationAsync(action, queueSync, cancellationToken);
        InvalidateReadCaches(tableNames.ToArray());
        return result;
    }

    public Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        return ExecuteAtomicCoreAsync(action, cancellationToken);
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) =>
        ShouldUseHydratedCacheRead()
            ? WithReadAsync(_innerRepository.GetMetadataAsync, cancellationToken)
            : GetMetadataDirectAsync(cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        ShouldUseHydratedCacheRead()
            ? WithReadAsync(ct => _innerRepository.GetCellsAsync(coordinates, ct), cancellationToken)
            : GetCellsDirectAsync(coordinates, cancellationToken);

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) =>
        ShouldUseHydratedCacheRead()
            ? WithReadAsync(ct => _innerRepository.GetCellAsync(coordinate, ct), cancellationToken)
            : GetCellDirectAsync(coordinate, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        ShouldUseHydratedCacheRead()
            ? WithReadAsync(ct => _innerRepository.GetScenarioCellsAsync(scenarioVersionId, ct), cancellationToken)
            : GetScenarioCellsDirectAsync(scenarioVersionId, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        GetDraftCellsDirectAsync(scenarioVersionId, userId, coordinates, cancellationToken);

    public async Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var materialized = cells.Select(cell => cell.Clone()).ToList();
        await ExecuteDirectMutationAsync(
            (connection, transaction, ct) => UpsertPlanningCellsAsync(connection, transaction, materialized, ct),
            cancellationToken);
    }

    public async Task UpsertDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var materialized = cells.Select(cell => cell.Clone()).ToList();
        await ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => UpsertDraftPlanningCellsAsync(connection, transaction, scenarioVersionId, userId, materialized, ct),
            cancellationToken);
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
        await ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => AppendDraftCommandBatchDirectAsync(connection, transaction, batch, ct),
            cancellationToken);
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
        GetDraftUndoRedoAvailabilityDirectAsync(scenarioVersionId, userId, limit, cancellationToken);

    public Task<PlanningCommandBatch?> UndoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => UndoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, ct),
            cancellationToken);

    public Task<PlanningCommandBatch?> RedoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        ExecuteDirectNonVersionedMutationAsync(
            (connection, transaction, ct) => RedoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, ct),
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

    public Task CommitDraftAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken) =>
        ExecuteDirectMutationAsync(
            (connection, transaction, ct) => CommitDraftDirectAsync(connection, transaction, scenarioVersionId, userId, ct),
            cancellationToken);

    public Task RecordSaveCheckpointAsync(long scenarioVersionId, string userId, string mode, DateTimeOffset savedAt, CancellationToken cancellationToken) =>
        RecordSaveCheckpointDirectAsync(scenarioVersionId, userId, mode, savedAt, cancellationToken);

    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) =>
        GetStoresDirectAsync(cancellationToken);

    public Task<IReadOnlyDictionary<long, long>> GetStoreRootProductNodeIdsAsync(CancellationToken cancellationToken) =>
        GetStoreRootProductNodeIdsDirectAsync(cancellationToken);

    public Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertStoreProfileAsync(scenarioVersionId, storeProfile, ct),
            plan => plan.QueueTableReplace(StoreMutationTables),
            StoreMutationTables,
            cancellationToken);

    public Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.DeleteStoreProfileAsync(scenarioVersionId, storeId, ct),
            plan => plan.QueueTableReplace(StoreMutationTables),
            StoreMutationTables,
            cancellationToken);

    public Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.InactivateStoreProfileAsync(storeId, ct),
            plan => plan.QueueTableReplace("store_metadata"),
            ["store_metadata"],
            cancellationToken);

    public Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoreProfileOptionsAsync, cancellationToken);

    public Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertStoreProfileOptionAsync(fieldName, value, isActive, ct),
            plan => plan.QueueTableReplace("store_profile_options"),
            ["store_profile_options"],
            cancellationToken);

    public Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.DeleteStoreProfileOptionAsync(fieldName, value, ct),
            plan => plan.QueueTableReplace("store_profile_options"),
            ["store_profile_options"],
            cancellationToken);

    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetHierarchyMappingsAsync, cancellationToken);

    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertHierarchyDepartmentAsync(departmentLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            HierarchyTables,
            cancellationToken);

    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertHierarchyClassAsync(departmentLabel, classLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            HierarchyTables,
            cancellationToken);

    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertHierarchySubclassAsync(departmentLabel, classLabel, subclassLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            HierarchyTables,
            cancellationToken);

    public Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertProductProfileAsync(profile, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            ProductMutationTables,
            cancellationToken);

    public Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.DeleteProductProfileAsync(skuVariant, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            ProductMutationTables,
            cancellationToken);

    public Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.InactivateProductProfileAsync(skuVariant, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            ProductMutationTables,
            cancellationToken);

    public Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductProfileOptionsAsync, cancellationToken);

    public Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertProductProfileOptionAsync(fieldName, value, isActive, ct),
            plan => plan.QueueTableReplace("product_profile_options"),
            ["product_profile_options"],
            cancellationToken);

    public Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.DeleteProductProfileOptionAsync(fieldName, value, ct),
            plan => plan.QueueTableReplace("product_profile_options"),
            ["product_profile_options"],
            cancellationToken);

    public Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductHierarchyCatalogAsync, cancellationToken);

    public Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductSubclassCatalogAsync, cancellationToken);

    public Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.UpsertProductHierarchyCatalogAsync(record, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            ProductMutationTables,
            cancellationToken);

    public Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.DeleteProductHierarchyCatalogAsync(dptNo, clssNo, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            ProductMutationTables,
            cancellationToken);

    public Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.ReplaceProductMasterDataAsync(hierarchyRows, profiles, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
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
            ct => _innerRepository.RebuildPlanningFromMasterDataAsync(scenarioVersionId, fiscalYear, ct),
            plan => plan.QueueFullSnapshot(),
            ProductMutationTables.Concat(TimePeriodMutationTables).Concat(StoreMutationTables).Concat(HierarchyTables).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            cancellationToken);

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) =>
        ShouldUseHydratedCacheRead()
            ? WithReadAsync(ct => _innerRepository.FindProductNodeByPathAsync(path, ct), cancellationToken)
            : FindProductNodeByPathDirectAsync(path, cancellationToken);

    public Task ResetAsync(CancellationToken cancellationToken) =>
        WithMutationAndCacheInvalidationAsync(
            ct => _innerRepository.ResetAsync(ct),
            plan => plan.QueueFullSnapshot(),
            ProductMutationTables.Concat(TimePeriodMutationTables).Concat(StoreMutationTables).Concat(HierarchyTables).Distinct(StringComparer.OrdinalIgnoreCase).ToArray(),
            cancellationToken);

    public async Task ApplyMigrationsAsync(string migrationsDirectory, CancellationToken cancellationToken)
    {
        var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        var migrator = new PostgresMigrationRunner(_connectionString, migrationsDirectory, loggerFactory.CreateLogger<PostgresMigrationRunner>());
        await migrator.ApplyMigrationsAsync(cancellationToken);
    }

    public async Task ImportSqliteSnapshotAsync(string sqliteDatabasePath, string seedKey, string sourceName, CancellationToken cancellationToken)
    {
        if (!File.Exists(sqliteDatabasePath))
        {
            throw new FileNotFoundException("SQLite migration source was not found.", sqliteDatabasePath);
        }

        _logger.LogInformation(
            "Starting SQLite to PostgreSQL snapshot import from {SourceName} using seed key {SeedKey}.",
            sourceName,
            seedKey);

        await using var sqlite = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = sqliteDatabasePath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString());
        await sqlite.OpenAsync(cancellationToken);

        await using var postgres = new NpgsqlConnection(_connectionString);
        await postgres.OpenAsync(cancellationToken);
        await using var transaction = await postgres.BeginTransactionAsync(cancellationToken);

        await using (var command = new NpgsqlCommand(
                         """
                         insert into seed_runs (seed_key, source_name, status, started_at)
                         values (@seedKey, @sourceName, 'running', now())
                         on conflict (seed_key) do nothing;
                         """,
                         postgres,
                         transaction))
        {
            command.Parameters.AddWithValue("@seedKey", seedKey);
            command.Parameters.AddWithValue("@sourceName", sourceName);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var tableName in PostgresTableDefinitions.FullSnapshotDeleteOrder)
        {
            await using var deleteCommand = new NpgsqlCommand($"delete from {tableName};", postgres, transaction);
            await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (var definition in PostgresTableDefinitions.All)
        {
            _logger.LogInformation("Copying table {TableName} from SQLite into PostgreSQL.", definition.Name);
            await CopyTableSqliteToPostgresAsync(sqlite, postgres, transaction, definition, cancellationToken);
            _logger.LogInformation("Copied table {TableName} into PostgreSQL.", definition.Name);
        }

        await UpdateDataVersionAsync(postgres, transaction, cancellationToken);

        await using (var completeCommand = new NpgsqlCommand(
                         """
                         update seed_runs
                         set status = 'completed',
                             completed_at = now(),
                             details_json = @detailsJson
                         where seed_key = @seedKey;
                         """,
                         postgres,
                         transaction))
        {
            completeCommand.Parameters.AddWithValue("@seedKey", seedKey);
            completeCommand.Parameters.AddWithValue("@detailsJson", $"{{\"source\":\"{EscapeJson(sourceName)}\"}}");
            await completeCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        _hydrated = false;
        _localDataVersion = null;
        _logger.LogInformation(
            "Completed SQLite to PostgreSQL snapshot import from {SourceName} using seed key {SeedKey}.",
            sourceName,
            seedKey);
    }

    private async Task<T> ExecuteAtomicCoreAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        var isOutermost = _atomicDepth.Value == 0;
        if (isOutermost)
        {
            _pendingSyncPlan.Value = new PostgresSyncPlan();
            _directAtomicContext.Value = null;
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
                var plan = _pendingSyncPlan.Value;
                var directContext = _directAtomicContext.Value;
                _pendingSyncPlan.Value = null;
                _directAtomicContext.Value = null;
                try
                {
                    if (directContext is not null)
                    {
                        if (succeeded)
                        {
                            if (directContext.HasMutations)
                            {
                                await UpdateDataVersionAsync(directContext.Connection, directContext.Transaction, cancellationToken);
                            }

                            await directContext.Transaction.CommitAsync(cancellationToken);

                            if (directContext.HasMutations)
                            {
                                _hydrated = false;
                                _localDataVersion = null;
                            }
                        }
                        else
                        {
                            await directContext.Transaction.RollbackAsync(cancellationToken);
                        }
                    }

                    if (succeeded && plan is { HasMutations: true })
                    {
                        await _syncGate.WaitAsync(cancellationToken);
                        try
                        {
                            await ExecuteSyncPlanAsync(plan, cancellationToken);
                        }
                        finally
                        {
                            _syncGate.Release();
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

    private async Task<T> WithReadAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        return await action(cancellationToken);
    }

    private bool ShouldUseHydratedCacheRead()
    {
        return _atomicDepth.Value > 0 && (_pendingSyncPlan.Value?.HasMutations ?? false);
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

    private async Task WithMutationAsync(Func<CancellationToken, Task> action, Action<PostgresSyncPlan> queueSync, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        await action(cancellationToken);
        await QueueOrExecuteSyncAsync(queueSync, cancellationToken);
    }

    private async Task<T> WithMutationAsync<T>(Func<CancellationToken, Task<T>> action, Action<PostgresSyncPlan> queueSync, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        var result = await action(cancellationToken);
        await QueueOrExecuteSyncAsync(queueSync, cancellationToken);
        return result;
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
        await UpdateDataVersionAsync(connection, transaction, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        _hydrated = false;
        _localDataVersion = null;
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
        await transaction.CommitAsync(cancellationToken);
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
        await transaction.CommitAsync(cancellationToken);
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
        await UpdateDataVersionAsync(connection, transaction, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        _hydrated = false;
        _localDataVersion = null;
        return directResult;
    }

    private async Task QueueOrExecuteSyncAsync(Action<PostgresSyncPlan> queueSync, CancellationToken cancellationToken)
    {
        if (_atomicDepth.Value > 0)
        {
            var plan = _pendingSyncPlan.Value ??= new PostgresSyncPlan();
            queueSync(plan);
            return;
        }

        var immediatePlan = new PostgresSyncPlan();
        queueSync(immediatePlan);
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            await ExecuteSyncPlanAsync(immediatePlan, cancellationToken);
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task EnsureHydratedAsync(CancellationToken cancellationToken)
    {
        await EnsureDatabaseReadyAsync(cancellationToken);
        var remoteVersion = await GetRemoteDataVersionAsync(cancellationToken);
        if (_hydrated && _localDataVersion == remoteVersion && File.Exists(_localCachePath))
        {
            return;
        }

        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            remoteVersion = await GetRemoteDataVersionAsync(cancellationToken);
            if (_hydrated && _localDataVersion == remoteVersion && File.Exists(_localCachePath))
            {
                return;
            }

            await RebuildLocalCacheFromPostgresAsync(remoteVersion, cancellationToken);
            _hydrated = true;
            _localDataVersion = remoteVersion;
        }
        finally
        {
            _syncGate.Release();
        }
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

            var remoteVersion = await GetRemoteDataVersionAsync(cancellationToken);
            if (remoteVersion == 0
                && !string.IsNullOrWhiteSpace(_bootstrapS3Bucket)
                && !string.IsNullOrWhiteSpace(_bootstrapS3ObjectKey)
                && !string.IsNullOrWhiteSpace(_bootstrapSeedKey))
            {
                _logger.LogInformation(
                    "PostgreSQL data version is 0. Bootstrapping from s3://{Bucket}/{Key} with seed key {SeedKey}.",
                    _bootstrapS3Bucket,
                    _bootstrapS3ObjectKey,
                    _bootstrapSeedKey);
                var tempSqlitePath = Path.Combine(Path.GetTempPath(), "sales-planning-postgres-bootstrap", "source.db");
                Directory.CreateDirectory(Path.GetDirectoryName(tempSqlitePath) ?? Path.GetTempPath());
                if (File.Exists(tempSqlitePath))
                {
                    File.Delete(tempSqlitePath);
                }

                using var s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(_bootstrapS3Region ?? "ap-southeast-5"));
                using var response = await s3Client.GetObjectAsync(_bootstrapS3Bucket, _bootstrapS3ObjectKey, cancellationToken);
                await using (var targetStream = File.Create(tempSqlitePath))
                {
                    await response.ResponseStream.CopyToAsync(targetStream, cancellationToken);
                }
                _logger.LogInformation("Downloaded SQLite bootstrap snapshot to {BootstrapPath}.", tempSqlitePath);

                await ImportSqliteSnapshotAsync(
                    tempSqlitePath,
                    _bootstrapSeedKey,
                    $"s3://{_bootstrapS3Bucket}/{_bootstrapS3ObjectKey}",
                    cancellationToken);
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

    private async Task RebuildLocalCacheFromPostgresAsync(long remoteVersion, CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(_localCachePath) ?? ".");
        var tempPath = $"{_localCachePath}.tmp";
        if (File.Exists(tempPath))
        {
            File.Delete(tempPath);
        }

        await using var sqlite = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = tempPath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString());
        await sqlite.OpenAsync(cancellationToken);
        await using (var schemaCommand = sqlite.CreateCommand())
        {
            schemaCommand.CommandText = PostgresTableDefinitions.SqliteCacheSchema;
            await schemaCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var pragmaCommand = sqlite.CreateCommand())
        {
            pragmaCommand.CommandText = """
                pragma journal_mode = memory;
                pragma synchronous = off;
                pragma temp_store = memory;
                """;
            await pragmaCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var postgres = new NpgsqlConnection(_connectionString);
        await postgres.OpenAsync(cancellationToken);
        foreach (var definition in PostgresTableDefinitions.All)
        {
            _logger.LogInformation("Hydrating local SQLite cache table {TableName} from PostgreSQL.", definition.Name);
            await CopyTablePostgresToSqliteAsync(postgres, sqlite, definition, cancellationToken);
            _logger.LogInformation("Hydrated local SQLite cache table {TableName}.", definition.Name);
        }

        sqlite.Close();
        File.Move(tempPath, _localCachePath, true);
        await File.WriteAllTextAsync(_localVersionPath, remoteVersion.ToString(), cancellationToken);
        _logger.LogInformation("Hydrated local SQLite cache from PostgreSQL at data version {DataVersion}", remoteVersion);
    }

    private async Task ExecuteSyncPlanAsync(PostgresSyncPlan plan, CancellationToken cancellationToken)
    {
        await using var postgres = new NpgsqlConnection(_connectionString);
        await postgres.OpenAsync(cancellationToken);
        await using var transaction = await postgres.BeginTransactionAsync(cancellationToken);

        if (plan.FullSnapshot)
        {
            await ReplaceTablesFromSqliteAsync(postgres, transaction, PostgresTableDefinitions.All.Select(definition => definition.Name), cancellationToken);
        }
        else
        {
            if (plan.TablesToReplace.Count > 0)
            {
                await ReplaceTablesFromSqliteAsync(postgres, transaction, plan.TablesToReplace, cancellationToken);
            }

            if (plan.CellUpserts.Count > 0 && !plan.TablesToReplace.Contains("planning_cells"))
            {
                await UpsertPlanningCellsAsync(postgres, transaction, plan.CellUpserts.Values, cancellationToken);
            }
        }

        await UpdateDataVersionAsync(postgres, transaction, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        _localDataVersion = (_localDataVersion ?? 0) + 1;
        await File.WriteAllTextAsync(_localVersionPath, _localDataVersion.Value.ToString(), cancellationToken);
        _logger.LogInformation("Synchronized PostgreSQL persistence at data version {DataVersion}", _localDataVersion);
    }

    private async Task ReplaceTablesFromSqliteAsync(NpgsqlConnection postgres, NpgsqlTransaction transaction, IEnumerable<string> tables, CancellationToken cancellationToken)
    {
        await using var sqlite = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = _localCachePath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadOnly
        }.ToString());
        await sqlite.OpenAsync(cancellationToken);

        var orderedTables = tables
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(tableName => Array.IndexOf(PostgresTableDefinitions.FullSnapshotDeleteOrder.ToArray(), tableName))
            .ToList();

        foreach (var tableName in orderedTables)
        {
            await using (var deleteCommand = new NpgsqlCommand($"delete from {tableName};", postgres, transaction))
            {
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            var definition = PostgresTableDefinitions.Get(tableName);
            await CopyTableSqliteToPostgresAsync(sqlite, postgres, transaction, definition, cancellationToken);
        }
    }

    private static async Task CopyTablePostgresToSqliteAsync(NpgsqlConnection postgres, SqliteConnection sqlite, PostgresTableDefinition definition, CancellationToken cancellationToken)
    {
        var selectSql = $"select {string.Join(", ", definition.Columns)} from {definition.Name};";
        await using var selectCommand = new NpgsqlCommand(selectSql, postgres);
        await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
        var insertSql = $"insert into {definition.Name} ({string.Join(", ", definition.Columns)}) values ({string.Join(", ", definition.Columns.Select((_, index) => $"@p{index}"))});";
        await using var transaction = (SqliteTransaction)await sqlite.BeginTransactionAsync(cancellationToken);
        await using var insertCommand = sqlite.CreateCommand();
        insertCommand.CommandText = insertSql;
        insertCommand.Transaction = transaction;
        var parameters = new SqliteParameter[definition.Columns.Length];
        for (var index = 0; index < definition.Columns.Length; index += 1)
        {
            var parameter = insertCommand.CreateParameter();
            parameter.ParameterName = $"@p{index}";
            insertCommand.Parameters.Add(parameter);
            parameters[index] = parameter;
        }

        await insertCommand.PrepareAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            for (var index = 0; index < definition.Columns.Length; index += 1)
            {
                parameters[index].Value = reader.IsDBNull(index) ? DBNull.Value : reader.GetValue(index);
            }

            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task CopyTableSqliteToPostgresAsync(SqliteConnection sqlite, NpgsqlConnection postgres, NpgsqlTransaction transaction, PostgresTableDefinition definition, CancellationToken cancellationToken)
    {
        var existingColumns = await GetSqliteColumnsAsync(sqlite, definition.Name, cancellationToken);
        var postgresColumnTypes = await GetPostgresColumnTypesAsync(postgres, transaction, definition.Name, cancellationToken);
        var selectedColumns = definition.Columns
            .Where(column => existingColumns.Contains(column))
            .ToArray();

        if (selectedColumns.Length == 0)
        {
            return;
        }

        try
        {
            var selectSql = $"select {string.Join(", ", selectedColumns)} from {definition.Name};";
            await using var selectCommand = sqlite.CreateCommand();
            selectCommand.CommandText = selectSql;
            await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken);
            var selectedIndexByColumn = selectedColumns
                .Select((column, index) => (column, index))
                .ToDictionary(entry => entry.column, entry => entry.index, StringComparer.OrdinalIgnoreCase);
            var copySql = $"copy {definition.Name} ({string.Join(", ", definition.Columns)}) from stdin (format binary)";
            await using var importer = await postgres.BeginBinaryImportAsync(copySql, cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                await importer.StartRowAsync(cancellationToken);
                for (var index = 0; index < definition.Columns.Length; index += 1)
                {
                    if (!selectedIndexByColumn.TryGetValue(definition.Columns[index], out var readerIndex) || reader.IsDBNull(readerIndex))
                    {
                        await importer.WriteNullAsync(cancellationToken);
                        continue;
                    }

                    var postgresType = postgresColumnTypes.GetValueOrDefault(definition.Columns[index], "text");
                    try
                    {
                        await WriteSqliteValueAsync(importer, reader.GetValue(readerIndex), postgresType, cancellationToken);
                    }
                    catch (Exception exception)
                    {
                        var rawValue = reader.GetValue(readerIndex);
                        throw new InvalidOperationException(
                            $"Failed to copy {definition.Name}.{definition.Columns[index]} as PostgreSQL type '{postgresType}'. SQLite value type was '{rawValue.GetType().FullName}' and value was '{Convert.ToString(rawValue, CultureInfo.InvariantCulture)}'.",
                            exception);
                    }
                }
            }

            await importer.CompleteAsync(cancellationToken);
        }
        catch (Exception exception) when (exception is not InvalidOperationException)
        {
            throw new InvalidOperationException($"Failed to bulk copy table '{definition.Name}' from SQLite into PostgreSQL.", exception);
        }
    }

    private static async Task<HashSet<string>> GetSqliteColumnsAsync(SqliteConnection sqlite, string tableName, CancellationToken cancellationToken)
    {
        await using var command = sqlite.CreateCommand();
        command.CommandText = $"pragma table_info({tableName});";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                columns.Add(reader.GetString(1));
            }
        }

        return columns;
    }

    private static async Task<Dictionary<string, string>> GetPostgresColumnTypesAsync(NpgsqlConnection postgres, NpgsqlTransaction transaction, string tableName, CancellationToken cancellationToken)
    {
        const string sql = """
            select column_name, data_type
            from information_schema.columns
            where table_schema = 'public' and table_name = @tableName;
            """;

        await using var command = new NpgsqlCommand(sql, postgres, transaction);
        command.Parameters.AddWithValue("@tableName", tableName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync(cancellationToken))
        {
            result[reader.GetString(0)] = reader.GetString(1);
        }

        return result;
    }

    private static Task WriteSqliteValueAsync(NpgsqlBinaryImporter importer, object value, string postgresType, CancellationToken cancellationToken)
    {
        return postgresType switch
        {
            "bigint" => importer.WriteAsync(Convert.ToInt64(value), cancellationToken),
            "integer" => importer.WriteAsync(Convert.ToInt32(value), cancellationToken),
            "smallint" => importer.WriteAsync(Convert.ToInt16(value), cancellationToken),
            "numeric" => importer.WriteAsync(Convert.ToDecimal(value), cancellationToken),
            "double precision" => importer.WriteAsync(Convert.ToDouble(value), cancellationToken),
            "real" => importer.WriteAsync(Convert.ToSingle(value), cancellationToken),
            "boolean" => importer.WriteAsync(ToBoolean(value), cancellationToken),
            "timestamp with time zone" => importer.WriteAsync(ToUtcDateTime(value), cancellationToken),
            "timestamp without time zone" => importer.WriteAsync(ToUnspecifiedDateTime(value), cancellationToken),
            "date" => importer.WriteAsync(ToDateOnly(value), cancellationToken),
            "uuid" => importer.WriteAsync(ToGuid(value), cancellationToken),
            _ => importer.WriteAsync(Convert.ToString(value, CultureInfo.InvariantCulture), cancellationToken)
        };
    }

    private static bool ToBoolean(object value)
    {
        return value switch
        {
            bool booleanValue => booleanValue,
            long longValue => longValue != 0,
            int intValue => intValue != 0,
            short shortValue => shortValue != 0,
            byte byteValue => byteValue != 0,
            string stringValue when bool.TryParse(stringValue, out var parsedBoolean) => parsedBoolean,
            string stringValue when long.TryParse(stringValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInteger) => parsedInteger != 0,
            _ => Convert.ToInt64(value, CultureInfo.InvariantCulture) != 0
        };
    }

    private static DateTime ToUtcDateTime(object value)
    {
        return value switch
        {
            DateTime dateTime => dateTime.Kind switch
            {
                DateTimeKind.Utc => dateTime,
                DateTimeKind.Local => dateTime.ToUniversalTime(),
                _ => DateTime.SpecifyKind(dateTime, DateTimeKind.Utc)
            },
            DateTimeOffset dateTimeOffset => dateTimeOffset.UtcDateTime,
            string stringValue => DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).UtcDateTime,
            _ => DateTime.SpecifyKind(Convert.ToDateTime(value, CultureInfo.InvariantCulture), DateTimeKind.Utc)
        };
    }

    private static DateTime ToUnspecifiedDateTime(object value)
    {
        return value switch
        {
            DateTime dateTime => dateTime.Kind == DateTimeKind.Unspecified
                ? dateTime
                : DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified),
            DateTimeOffset dateTimeOffset => DateTime.SpecifyKind(dateTimeOffset.DateTime, DateTimeKind.Unspecified),
            string stringValue => DateTime.SpecifyKind(
                DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).DateTime,
                DateTimeKind.Unspecified),
            _ => DateTime.SpecifyKind(Convert.ToDateTime(value, CultureInfo.InvariantCulture), DateTimeKind.Unspecified)
        };
    }

    private static DateOnly ToDateOnly(object value)
    {
        return value switch
        {
            DateOnly dateOnly => dateOnly,
            DateTime dateTime => DateOnly.FromDateTime(dateTime),
            DateTimeOffset dateTimeOffset => DateOnly.FromDateTime(dateTimeOffset.DateTime),
            string stringValue => DateOnly.Parse(stringValue, CultureInfo.InvariantCulture),
            _ => DateOnly.FromDateTime(Convert.ToDateTime(value, CultureInfo.InvariantCulture))
        };
    }

    private static Guid ToGuid(object value)
    {
        return value switch
        {
            Guid guid => guid,
            string stringValue => Guid.Parse(stringValue),
            _ => Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("Cannot convert null value to GUID."))
        };
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

    private static string EscapeJson(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal);

    private sealed class PostgresSyncPlan
    {
        public bool FullSnapshot { get; private set; }
        public HashSet<string> TablesToReplace { get; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, PlanningCell> CellUpserts { get; } = new(StringComparer.Ordinal);
        public bool HasMutations => FullSnapshot || TablesToReplace.Count > 0 || CellUpserts.Count > 0;

        public void QueueFullSnapshot()
        {
            FullSnapshot = true;
            TablesToReplace.Clear();
            CellUpserts.Clear();
        }

        public void QueueTableReplace(params IEnumerable<string>[] tableNames)
        {
            if (FullSnapshot)
            {
                return;
            }

            foreach (var tableCollection in tableNames)
            {
                foreach (var tableName in tableCollection)
                {
                    TablesToReplace.Add(tableName);
                    if (string.Equals(tableName, "planning_cells", StringComparison.OrdinalIgnoreCase))
                    {
                        CellUpserts.Clear();
                    }
                }
            }
        }

        public void QueueTableReplace(params string[] tableNames)
        {
            QueueTableReplace((IEnumerable<string>)tableNames);
        }

        public void QueueCellUpserts(IEnumerable<PlanningCell> cells)
        {
            if (FullSnapshot || TablesToReplace.Contains("planning_cells"))
            {
                return;
            }

            foreach (var cell in cells)
            {
                CellUpserts[cell.Coordinate.Key] = cell.Clone();
            }
        }
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
