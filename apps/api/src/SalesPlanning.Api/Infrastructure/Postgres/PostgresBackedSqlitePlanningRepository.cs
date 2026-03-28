using Microsoft.Data.Sqlite;
using Amazon;
using Amazon.S3;
using Npgsql;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository : IPlanningRepository
{
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
    private bool _hydrated;
    private bool _databaseReady;
    private long? _localDataVersion;

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

    public Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        return ExecuteAtomicCoreAsync(action, cancellationToken);
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetMetadataAsync, cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetCellsAsync(coordinates, ct), cancellationToken);

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetCellAsync(coordinate, ct), cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetScenarioCellsAsync(scenarioVersionId, ct), cancellationToken);

    public async Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        var materialized = cells.Select(cell => cell.Clone()).ToList();
        await WithMutationAsync(
            ct => _innerRepository.UpsertCellsAsync(materialized, ct),
            plan => plan.QueueCellUpserts(materialized),
            cancellationToken);
    }

    public async Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        await WithMutationAsync(
            ct => _innerRepository.AppendAuditAsync(audit, ct),
            plan => plan.QueueTableReplace("audits", "audit_deltas"),
            cancellationToken);
    }

    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetNextActionIdAsync, cancellationToken);

    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, ct), cancellationToken);

    public Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetNextCommandBatchIdAsync, cancellationToken);

    public async Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken)
    {
        await WithMutationAsync(
            ct => _innerRepository.AppendCommandBatchAsync(batch, ct),
            plan => plan.QueueTableReplace("planning_command_batches", "planning_command_cell_deltas"),
            cancellationToken);
    }

    public Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, limit, ct), cancellationToken);

    public Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UndoLatestCommandAsync(scenarioVersionId, userId, limit, ct),
            plan => plan.QueueTableReplace("planning_cells", "planning_command_batches", "planning_command_cell_deltas"),
            cancellationToken);

    public Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.RedoLatestCommandAsync(scenarioVersionId, userId, limit, ct),
            plan => plan.QueueTableReplace("planning_cells", "planning_command_batches", "planning_command_cell_deltas"),
            cancellationToken);

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetGridSliceAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, ct), cancellationToken);

    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.AddRowAsync(request, ct),
            plan => plan.QueueTableReplace(ProductNodeMutationTables),
            cancellationToken);

    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteRowAsync(scenarioVersionId, productNodeId, ct),
            plan => plan.QueueTableReplace(ProductNodeMutationTables),
            cancellationToken);

    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, ct),
            plan => plan.QueueTableReplace(TimePeriodMutationTables),
            cancellationToken);

    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.EnsureYearAsync(scenarioVersionId, fiscalYear, ct),
            plan => plan.QueueTableReplace(TimePeriodMutationTables),
            cancellationToken);

    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoresAsync, cancellationToken);

    public Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertStoreProfileAsync(scenarioVersionId, storeProfile, ct),
            plan => plan.QueueTableReplace(StoreMutationTables),
            cancellationToken);

    public Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteStoreProfileAsync(scenarioVersionId, storeId, ct),
            plan => plan.QueueTableReplace(StoreMutationTables),
            cancellationToken);

    public Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.InactivateStoreProfileAsync(storeId, ct),
            plan => plan.QueueTableReplace("store_metadata"),
            cancellationToken);

    public Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoreProfileOptionsAsync, cancellationToken);

    public Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertStoreProfileOptionAsync(fieldName, value, isActive, ct),
            plan => plan.QueueTableReplace("store_profile_options"),
            cancellationToken);

    public Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteStoreProfileOptionAsync(fieldName, value, ct),
            plan => plan.QueueTableReplace("store_profile_options"),
            cancellationToken);

    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetHierarchyMappingsAsync, cancellationToken);

    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertHierarchyDepartmentAsync(departmentLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            cancellationToken);

    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertHierarchyClassAsync(departmentLabel, classLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            cancellationToken);

    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertHierarchySubclassAsync(departmentLabel, classLabel, subclassLabel, ct),
            plan => plan.QueueTableReplace(HierarchyTables),
            cancellationToken);

    public Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertProductProfileAsync(profile, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            cancellationToken);

    public Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteProductProfileAsync(skuVariant, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            cancellationToken);

    public Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.InactivateProductProfileAsync(skuVariant, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            cancellationToken);

    public Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductProfileOptionsAsync, cancellationToken);

    public Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertProductProfileOptionAsync(fieldName, value, isActive, ct),
            plan => plan.QueueTableReplace("product_profile_options"),
            cancellationToken);

    public Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteProductProfileOptionAsync(fieldName, value, ct),
            plan => plan.QueueTableReplace("product_profile_options"),
            cancellationToken);

    public Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductHierarchyCatalogAsync, cancellationToken);

    public Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductSubclassCatalogAsync, cancellationToken);

    public Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.UpsertProductHierarchyCatalogAsync(record, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            cancellationToken);

    public Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.DeleteProductHierarchyCatalogAsync(dptNo, clssNo, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
            cancellationToken);

    public Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.ReplaceProductMasterDataAsync(hierarchyRows, profiles, ct),
            plan => plan.QueueTableReplace(ProductMutationTables),
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
        WithMutationAsync(
            ct => _innerRepository.RebuildPlanningFromMasterDataAsync(scenarioVersionId, fiscalYear, ct),
            plan => plan.QueueFullSnapshot(),
            cancellationToken);

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) =>
        WithReadAsync(ct => _innerRepository.FindProductNodeByPathAsync(path, ct), cancellationToken);

    public Task ResetAsync(CancellationToken cancellationToken) =>
        WithMutationAsync(
            ct => _innerRepository.ResetAsync(ct),
            plan => plan.QueueFullSnapshot(),
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
            await CopyTableSqliteToPostgresAsync(sqlite, postgres, transaction, definition, cancellationToken);
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
    }

    private async Task<T> ExecuteAtomicCoreAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        var isOutermost = _atomicDepth.Value == 0;
        if (isOutermost)
        {
            _pendingSyncPlan.Value = new PostgresSyncPlan();
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
                _pendingSyncPlan.Value = null;
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
        }
    }

    private async Task<T> WithReadAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        return await action(cancellationToken);
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
                var migrator = new PostgresMigrationRunner(_connectionString, _migrationsDirectory, LoggerFactory.Create(builder => { }).CreateLogger<PostgresMigrationRunner>());
                await migrator.ApplyMigrationsAsync(cancellationToken);
            }

            var remoteVersion = await GetRemoteDataVersionAsync(cancellationToken);
            if (remoteVersion == 0
                && !string.IsNullOrWhiteSpace(_bootstrapS3Bucket)
                && !string.IsNullOrWhiteSpace(_bootstrapS3ObjectKey)
                && !string.IsNullOrWhiteSpace(_bootstrapSeedKey))
            {
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

                await ImportSqliteSnapshotAsync(
                    tempSqlitePath,
                    _bootstrapSeedKey,
                    $"s3://{_bootstrapS3Bucket}/{_bootstrapS3ObjectKey}",
                    cancellationToken);
            }

            _databaseReady = true;
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

        await using var postgres = new NpgsqlConnection(_connectionString);
        await postgres.OpenAsync(cancellationToken);
        foreach (var definition in PostgresTableDefinitions.All)
        {
            await CopyTablePostgresToSqliteAsync(postgres, sqlite, definition, cancellationToken);
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
        while (await reader.ReadAsync(cancellationToken))
        {
            await using var insertCommand = sqlite.CreateCommand();
            insertCommand.CommandText = insertSql;
            for (var index = 0; index < definition.Columns.Length; index += 1)
            {
                insertCommand.Parameters.AddWithValue($"@p{index}", reader.IsDBNull(index) ? DBNull.Value : reader.GetValue(index));
            }

            await insertCommand.ExecuteNonQueryAsync(cancellationToken);
        }
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
                await WriteSqliteValueAsync(importer, reader.GetValue(readerIndex), postgresType, cancellationToken);
            }
        }

        await importer.CompleteAsync(cancellationToken);
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
            _ => importer.WriteAsync(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture), cancellationToken)
        };
    }

    private static async Task UpsertPlanningCellsAsync(NpgsqlConnection postgres, NpgsqlTransaction transaction, IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
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
                @inputValue,
                @overrideValue,
                @isSystemGeneratedOverride,
                @derivedValue,
                @effectiveValue,
                @growthFactor,
                @isLocked,
                @lockReason,
                @lockedBy,
                @rowVersion,
                @cellKind)
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

        foreach (var cell in cells)
        {
            await using var command = new NpgsqlCommand(sql, postgres, transaction);
            command.Parameters.AddWithValue("@scenarioVersionId", cell.Coordinate.ScenarioVersionId);
            command.Parameters.AddWithValue("@measureId", cell.Coordinate.MeasureId);
            command.Parameters.AddWithValue("@storeId", cell.Coordinate.StoreId);
            command.Parameters.AddWithValue("@productNodeId", cell.Coordinate.ProductNodeId);
            command.Parameters.AddWithValue("@timePeriodId", cell.Coordinate.TimePeriodId);
            command.Parameters.AddWithValue("@inputValue", (object?)cell.InputValue ?? DBNull.Value);
            command.Parameters.AddWithValue("@overrideValue", (object?)cell.OverrideValue ?? DBNull.Value);
            command.Parameters.AddWithValue("@isSystemGeneratedOverride", cell.IsSystemGeneratedOverride ? 1 : 0);
            command.Parameters.AddWithValue("@derivedValue", cell.DerivedValue);
            command.Parameters.AddWithValue("@effectiveValue", cell.EffectiveValue);
            command.Parameters.AddWithValue("@growthFactor", cell.GrowthFactor);
            command.Parameters.AddWithValue("@isLocked", cell.IsLocked ? 1 : 0);
            command.Parameters.AddWithValue("@lockReason", (object?)cell.LockReason ?? DBNull.Value);
            command.Parameters.AddWithValue("@lockedBy", (object?)cell.LockedBy ?? DBNull.Value);
            command.Parameters.AddWithValue("@rowVersion", cell.RowVersion);
            command.Parameters.AddWithValue("@cellKind", cell.CellKind);
            await command.ExecuteNonQueryAsync(cancellationToken);
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
}
