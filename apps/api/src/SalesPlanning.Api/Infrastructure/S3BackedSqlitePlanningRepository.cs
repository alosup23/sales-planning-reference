using Amazon.S3;
using Amazon.S3.Model;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using System.Threading;

namespace SalesPlanning.Api.Infrastructure;

public sealed class S3BackedSqlitePlanningRepository : IPlanningRepository
{
    private readonly SqlitePlanningRepository _innerRepository;
    private readonly IAmazonS3 _s3Client;
    private readonly ILogger<S3BackedSqlitePlanningRepository> _logger;
    private readonly string _bucketName;
    private readonly string _objectKey;
    private readonly string _localDatabasePath;
    private readonly SemaphoreSlim _syncGate = new(1, 1);
    private readonly AsyncLocal<int> _atomicDepth = new();
    private readonly AsyncLocal<bool> _pendingUpload = new();
    private bool _hydrated;

    public S3BackedSqlitePlanningRepository(
        IAmazonS3 s3Client,
        ILogger<S3BackedSqlitePlanningRepository> logger,
        string bucketName,
        string objectKey,
        string localDatabasePath)
    {
        _s3Client = s3Client;
        _logger = logger;
        _bucketName = bucketName;
        _objectKey = objectKey;
        _localDatabasePath = localDatabasePath;
        _innerRepository = new SqlitePlanningRepository(localDatabasePath);
    }

    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetMetadataAsync, cancellationToken);

    public async Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
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
            if (_atomicDepth.Value == 0)
            {
                var shouldUpload = succeeded && _pendingUpload.Value;
                _pendingUpload.Value = false;

                if (shouldUpload)
                {
                    await _syncGate.WaitAsync(cancellationToken);
                    try
                    {
                        await UploadLocalDatabaseAsync(cancellationToken);
                    }
                    finally
                    {
                        _syncGate.Release();
                    }
                }
            }
        }
    }

    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetCellsAsync(coordinates, ct), cancellationToken);

    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetCellAsync(coordinate, ct), cancellationToken);

    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetScenarioCellsAsync(scenarioVersionId, ct), cancellationToken);

    public Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertCellsAsync(cells, ct), cancellationToken);

    public Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.AppendAuditAsync(audit, ct), cancellationToken);

    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetNextActionIdAsync, cancellationToken);

    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, ct), cancellationToken);

    public Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetNextCommandBatchIdAsync, cancellationToken);

    public Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.AppendCommandBatchAsync(batch, ct), cancellationToken);

    public Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, limit, ct), cancellationToken);

    public Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UndoLatestCommandAsync(scenarioVersionId, userId, limit, ct), cancellationToken);

    public Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.RedoLatestCommandAsync(scenarioVersionId, userId, limit, ct), cancellationToken);

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetGridSliceAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, ct), cancellationToken);

    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.AddRowAsync(request, ct), cancellationToken);

    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteRowAsync(scenarioVersionId, productNodeId, ct), cancellationToken);

    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, ct), cancellationToken);

    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.EnsureYearAsync(scenarioVersionId, fiscalYear, ct), cancellationToken);

    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoresAsync, cancellationToken);

    public Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertStoreProfileAsync(scenarioVersionId, storeProfile, ct), cancellationToken);

    public Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteStoreProfileAsync(scenarioVersionId, storeId, ct), cancellationToken);

    public Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivateStoreProfileAsync(storeId, ct), cancellationToken);

    public Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetStoreProfileOptionsAsync, cancellationToken);

    public Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertStoreProfileOptionAsync(fieldName, value, isActive, ct), cancellationToken);

    public Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteStoreProfileOptionAsync(fieldName, value, ct), cancellationToken);

    public Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertProductProfileAsync(profile, ct), cancellationToken);

    public Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteProductProfileAsync(skuVariant, ct), cancellationToken);

    public Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivateProductProfileAsync(skuVariant, ct), cancellationToken);

    public Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductProfileOptionsAsync, cancellationToken);

    public Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertProductProfileOptionAsync(fieldName, value, isActive, ct), cancellationToken);

    public Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteProductProfileOptionAsync(fieldName, value, ct), cancellationToken);

    public Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductHierarchyCatalogAsync, cancellationToken);

    public Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetProductSubclassCatalogAsync, cancellationToken);

    public Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertProductHierarchyCatalogAsync(record, ct), cancellationToken);

    public Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteProductHierarchyCatalogAsync(dptNo, clssNo, ct), cancellationToken);

    public Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.ReplaceProductMasterDataAsync(hierarchyRows, profiles, ct), cancellationToken);

    public Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> GetInventoryProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetInventoryProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<InventoryProfileRecord> GetInventoryProfileByIdAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetInventoryProfileByIdAsync(inventoryProfileId, ct), cancellationToken);

    public Task<InventoryProfileRecord> UpsertInventoryProfileAsync(InventoryProfileRecord profile, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertInventoryProfileAsync(profile, ct), cancellationToken);

    public Task DeleteInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteInventoryProfileAsync(inventoryProfileId, ct), cancellationToken);

    public Task InactivateInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivateInventoryProfileAsync(inventoryProfileId, ct), cancellationToken);

    public Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> GetPricingPoliciesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetPricingPoliciesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<PricingPolicyRecord> GetPricingPolicyByIdAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetPricingPolicyByIdAsync(pricingPolicyId, ct), cancellationToken);

    public Task<PricingPolicyRecord> UpsertPricingPolicyAsync(PricingPolicyRecord policy, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertPricingPolicyAsync(policy, ct), cancellationToken);

    public Task DeletePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeletePricingPolicyAsync(pricingPolicyId, ct), cancellationToken);

    public Task InactivatePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivatePricingPolicyAsync(pricingPolicyId, ct), cancellationToken);

    public Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> GetSeasonalityEventProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetSeasonalityEventProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<SeasonalityEventProfileRecord> GetSeasonalityEventProfileByIdAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetSeasonalityEventProfileByIdAsync(seasonalityEventProfileId, ct), cancellationToken);

    public Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileAsync(SeasonalityEventProfileRecord profile, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertSeasonalityEventProfileAsync(profile, ct), cancellationToken);

    public Task DeleteSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteSeasonalityEventProfileAsync(seasonalityEventProfileId, ct), cancellationToken);

    public Task InactivateSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivateSeasonalityEventProfileAsync(seasonalityEventProfileId, ct), cancellationToken);

    public Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> GetVendorSupplyProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetVendorSupplyProfilesAsync(searchTerm, pageNumber, pageSize, ct), cancellationToken);

    public Task<VendorSupplyProfileRecord> GetVendorSupplyProfileByIdAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.GetVendorSupplyProfileByIdAsync(vendorSupplyProfileId, ct), cancellationToken);

    public Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileAsync(VendorSupplyProfileRecord profile, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertVendorSupplyProfileAsync(profile, ct), cancellationToken);

    public Task DeleteVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.DeleteVendorSupplyProfileAsync(vendorSupplyProfileId, ct), cancellationToken);

    public Task InactivateVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.InactivateVendorSupplyProfileAsync(vendorSupplyProfileId, ct), cancellationToken);

    public Task RebuildPlanningFromMasterDataAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.RebuildPlanningFromMasterDataAsync(scenarioVersionId, fiscalYear, ct), cancellationToken);

    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) =>
        WithReadAsync(_innerRepository.GetHierarchyMappingsAsync, cancellationToken);

    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchyDepartmentAsync(departmentLabel, ct), cancellationToken);

    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchyClassAsync(departmentLabel, classLabel, ct), cancellationToken);

    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) =>
        WithMutationAsync((ct) => _innerRepository.UpsertHierarchySubclassAsync(departmentLabel, classLabel, subclassLabel, ct), cancellationToken);

    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) =>
        WithReadAsync((ct) => _innerRepository.FindProductNodeByPathAsync(path, ct), cancellationToken);

    public Task ResetAsync(CancellationToken cancellationToken) =>
        WithMutationAsync(_innerRepository.ResetAsync, cancellationToken);

    private async Task<T> WithReadAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        return await action(cancellationToken);
    }

    private async Task<T> WithMutationAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            var result = await action(cancellationToken);
            if (_atomicDepth.Value > 0)
            {
                _pendingUpload.Value = true;
            }
            else
            {
                await UploadLocalDatabaseAsync(cancellationToken);
            }
            return result;
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task WithMutationAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        await EnsureHydratedAsync(cancellationToken);
        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            await action(cancellationToken);
            if (_atomicDepth.Value > 0)
            {
                _pendingUpload.Value = true;
            }
            else
            {
                await UploadLocalDatabaseAsync(cancellationToken);
            }
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task EnsureHydratedAsync(CancellationToken cancellationToken)
    {
        if (_hydrated)
        {
            return;
        }

        await _syncGate.WaitAsync(cancellationToken);
        try
        {
            if (_hydrated)
            {
                return;
            }

            Directory.CreateDirectory(Path.GetDirectoryName(_localDatabasePath) ?? Path.GetTempPath());
            if (await ObjectExistsAsync(cancellationToken))
            {
                using var response = await _s3Client.GetObjectAsync(_bucketName, _objectKey, cancellationToken);
                await response.WriteResponseStreamToFileAsync(_localDatabasePath, false, cancellationToken);
                _logger.LogInformation("Hydrated planning database from s3://{Bucket}/{Key}", _bucketName, _objectKey);
            }
            else
            {
                _logger.LogInformation(
                    "No planning database object found at s3://{Bucket}/{Key}. Using local seeded database and uploading on first mutation.",
                    _bucketName,
                    _objectKey);
            }

            _hydrated = true;
        }
        finally
        {
            _syncGate.Release();
        }
    }

    private async Task UploadLocalDatabaseAsync(CancellationToken cancellationToken)
    {
        if (!File.Exists(_localDatabasePath))
        {
            throw new FileNotFoundException("Local planning database was not found for upload.", _localDatabasePath);
        }

        await using var stream = File.Open(_localDatabasePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        await _s3Client.PutObjectAsync(new PutObjectRequest
        {
            BucketName = _bucketName,
            Key = _objectKey,
            InputStream = stream
        }, cancellationToken);

        _logger.LogInformation("Uploaded planning database to s3://{Bucket}/{Key}", _bucketName, _objectKey);
    }

    private async Task<bool> ObjectExistsAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _s3Client.GetObjectMetadataAsync(_bucketName, _objectKey, cancellationToken);
            return true;
        }
        catch (AmazonS3Exception exception) when (exception.StatusCode == System.Net.HttpStatusCode.NotFound || exception.ErrorCode is "NoSuchKey" or "NotFound")
        {
            return false;
        }
    }
}
