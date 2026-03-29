using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed class InMemoryPlanningRepository : IPlanningRepository, IDisposable
{
    private readonly string _databasePath;
    private readonly SqlitePlanningRepository _inner;

    public InMemoryPlanningRepository()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"sales-planning-tests-{Guid.NewGuid():N}.db");
        _inner = new SqlitePlanningRepository(_databasePath);
    }

    public Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken) => _inner.ExecuteAtomicAsync(action, cancellationToken);
    public Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken) => _inner.GetMetadataAsync(cancellationToken);
    public Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken) => _inner.GetCellsAsync(coordinates, cancellationToken);
    public Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken) => _inner.GetCellAsync(coordinate, cancellationToken);
    public Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken) => _inner.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
    public Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken) => _inner.UpsertCellsAsync(cells, cancellationToken);
    public Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken) => _inner.AppendAuditAsync(audit, cancellationToken);
    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) => _inner.GetNextActionIdAsync(cancellationToken);
    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) => _inner.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
    public Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken) => _inner.GetNextCommandBatchIdAsync(cancellationToken);
    public Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken) => _inner.AppendCommandBatchAsync(batch, cancellationToken);
    public Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.UndoLatestCommandAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.RedoLatestCommandAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken) => _inner.GetGridSliceAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, cancellationToken);
    public Task<GridBranchResponse> GetGridBranchRowsAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken) => _inner.GetGridBranchRowsAsync(scenarioVersionId, parentProductNodeId, cancellationToken);
    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) => _inner.AddRowAsync(request, cancellationToken);
    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) => _inner.DeleteRowAsync(scenarioVersionId, productNodeId, cancellationToken);
    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) => _inner.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, cancellationToken);
    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) => _inner.EnsureYearAsync(scenarioVersionId, fiscalYear, cancellationToken);
    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) => _inner.GetStoresAsync(cancellationToken);
    public Task<IReadOnlyDictionary<long, long>> GetStoreRootProductNodeIdsAsync(CancellationToken cancellationToken) => _inner.GetStoreRootProductNodeIdsAsync(cancellationToken);
    public Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken) => _inner.UpsertStoreProfileAsync(scenarioVersionId, storeProfile, cancellationToken);
    public Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken) => _inner.DeleteStoreProfileAsync(scenarioVersionId, storeId, cancellationToken);
    public Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken) => _inner.InactivateStoreProfileAsync(storeId, cancellationToken);
    public Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken) => _inner.GetStoreProfileOptionsAsync(cancellationToken);
    public Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) => _inner.UpsertStoreProfileOptionAsync(fieldName, value, isActive, cancellationToken);
    public Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) => _inner.DeleteStoreProfileOptionAsync(fieldName, value, cancellationToken);
    public Task<(IReadOnlyList<ProductProfileMetadata> Profiles, int TotalCount)> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) => _inner.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    public Task<ProductProfileMetadata> UpsertProductProfileAsync(ProductProfileMetadata profile, CancellationToken cancellationToken) => _inner.UpsertProductProfileAsync(profile, cancellationToken);
    public Task DeleteProductProfileAsync(string skuVariant, CancellationToken cancellationToken) => _inner.DeleteProductProfileAsync(skuVariant, cancellationToken);
    public Task InactivateProductProfileAsync(string skuVariant, CancellationToken cancellationToken) => _inner.InactivateProductProfileAsync(skuVariant, cancellationToken);
    public Task<IReadOnlyList<ProductProfileOptionValue>> GetProductProfileOptionsAsync(CancellationToken cancellationToken) => _inner.GetProductProfileOptionsAsync(cancellationToken);
    public Task UpsertProductProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken) => _inner.UpsertProductProfileOptionAsync(fieldName, value, isActive, cancellationToken);
    public Task DeleteProductProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken) => _inner.DeleteProductProfileOptionAsync(fieldName, value, cancellationToken);
    public Task<IReadOnlyList<ProductHierarchyCatalogRecord>> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken) => _inner.GetProductHierarchyCatalogAsync(cancellationToken);
    public Task<IReadOnlyList<ProductSubclassCatalogRecord>> GetProductSubclassCatalogAsync(CancellationToken cancellationToken) => _inner.GetProductSubclassCatalogAsync(cancellationToken);
    public Task UpsertProductHierarchyCatalogAsync(ProductHierarchyCatalogRecord record, CancellationToken cancellationToken) => _inner.UpsertProductHierarchyCatalogAsync(record, cancellationToken);
    public Task DeleteProductHierarchyCatalogAsync(string dptNo, string clssNo, CancellationToken cancellationToken) => _inner.DeleteProductHierarchyCatalogAsync(dptNo, clssNo, cancellationToken);
    public Task ReplaceProductMasterDataAsync(IReadOnlyList<ProductHierarchyCatalogRecord> hierarchyRows, IReadOnlyList<ProductProfileMetadata> profiles, CancellationToken cancellationToken) => _inner.ReplaceProductMasterDataAsync(hierarchyRows, profiles, cancellationToken);
    public Task<(IReadOnlyList<InventoryProfileRecord> Profiles, int TotalCount)> GetInventoryProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) => _inner.GetInventoryProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    public Task<InventoryProfileRecord> GetInventoryProfileByIdAsync(long inventoryProfileId, CancellationToken cancellationToken) => _inner.GetInventoryProfileByIdAsync(inventoryProfileId, cancellationToken);
    public Task<InventoryProfileRecord> UpsertInventoryProfileAsync(InventoryProfileRecord profile, CancellationToken cancellationToken) => _inner.UpsertInventoryProfileAsync(profile, cancellationToken);
    public Task DeleteInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) => _inner.DeleteInventoryProfileAsync(inventoryProfileId, cancellationToken);
    public Task InactivateInventoryProfileAsync(long inventoryProfileId, CancellationToken cancellationToken) => _inner.InactivateInventoryProfileAsync(inventoryProfileId, cancellationToken);
    public Task<(IReadOnlyList<PricingPolicyRecord> Policies, int TotalCount)> GetPricingPoliciesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) => _inner.GetPricingPoliciesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    public Task<PricingPolicyRecord> GetPricingPolicyByIdAsync(long pricingPolicyId, CancellationToken cancellationToken) => _inner.GetPricingPolicyByIdAsync(pricingPolicyId, cancellationToken);
    public Task<PricingPolicyRecord> UpsertPricingPolicyAsync(PricingPolicyRecord policy, CancellationToken cancellationToken) => _inner.UpsertPricingPolicyAsync(policy, cancellationToken);
    public Task DeletePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) => _inner.DeletePricingPolicyAsync(pricingPolicyId, cancellationToken);
    public Task InactivatePricingPolicyAsync(long pricingPolicyId, CancellationToken cancellationToken) => _inner.InactivatePricingPolicyAsync(pricingPolicyId, cancellationToken);
    public Task<(IReadOnlyList<SeasonalityEventProfileRecord> Profiles, int TotalCount)> GetSeasonalityEventProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) => _inner.GetSeasonalityEventProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    public Task<SeasonalityEventProfileRecord> GetSeasonalityEventProfileByIdAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) => _inner.GetSeasonalityEventProfileByIdAsync(seasonalityEventProfileId, cancellationToken);
    public Task<SeasonalityEventProfileRecord> UpsertSeasonalityEventProfileAsync(SeasonalityEventProfileRecord profile, CancellationToken cancellationToken) => _inner.UpsertSeasonalityEventProfileAsync(profile, cancellationToken);
    public Task DeleteSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) => _inner.DeleteSeasonalityEventProfileAsync(seasonalityEventProfileId, cancellationToken);
    public Task InactivateSeasonalityEventProfileAsync(long seasonalityEventProfileId, CancellationToken cancellationToken) => _inner.InactivateSeasonalityEventProfileAsync(seasonalityEventProfileId, cancellationToken);
    public Task<(IReadOnlyList<VendorSupplyProfileRecord> Profiles, int TotalCount)> GetVendorSupplyProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken) => _inner.GetVendorSupplyProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    public Task<VendorSupplyProfileRecord> GetVendorSupplyProfileByIdAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) => _inner.GetVendorSupplyProfileByIdAsync(vendorSupplyProfileId, cancellationToken);
    public Task<VendorSupplyProfileRecord> UpsertVendorSupplyProfileAsync(VendorSupplyProfileRecord profile, CancellationToken cancellationToken) => _inner.UpsertVendorSupplyProfileAsync(profile, cancellationToken);
    public Task DeleteVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) => _inner.DeleteVendorSupplyProfileAsync(vendorSupplyProfileId, cancellationToken);
    public Task InactivateVendorSupplyProfileAsync(long vendorSupplyProfileId, CancellationToken cancellationToken) => _inner.InactivateVendorSupplyProfileAsync(vendorSupplyProfileId, cancellationToken);
    public Task RebuildPlanningFromMasterDataAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) => _inner.RebuildPlanningFromMasterDataAsync(scenarioVersionId, fiscalYear, cancellationToken);
    public Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken) => _inner.GetHierarchyMappingsAsync(cancellationToken);
    public Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken) => _inner.UpsertHierarchyDepartmentAsync(departmentLabel, cancellationToken);
    public Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken) => _inner.UpsertHierarchyClassAsync(departmentLabel, classLabel, cancellationToken);
    public Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken) => _inner.UpsertHierarchySubclassAsync(departmentLabel, classLabel, subclassLabel, cancellationToken);
    public Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken) => _inner.FindProductNodeByPathAsync(path, cancellationToken);
    public Task ResetAsync(CancellationToken cancellationToken) => _inner.ResetAsync(cancellationToken);

    public void Dispose()
    {
        if (File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }
}
