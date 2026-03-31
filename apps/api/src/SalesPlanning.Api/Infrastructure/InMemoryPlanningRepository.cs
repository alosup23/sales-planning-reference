using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed class InMemoryPlanningRepository : IPlanningRepository, IDisposable
{
    private readonly string _databasePath;
    private readonly SqlitePlanningRepository _inner;
    private readonly object _draftGate = new();
    private readonly Dictionary<(long ScenarioVersionId, string UserId, string CoordinateKey), PlanningCell> _draftCells = new();
    private readonly List<PlanningCommandBatch> _draftCommandBatches = [];
    private long _nextDraftCommandBatchId = 1;

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
    public Task<IReadOnlyList<PlanningCell>> GetDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            var cells = coordinates
                .DistinctBy(coordinate => coordinate.Key)
                .Select(coordinate => _draftCells.GetValueOrDefault((scenarioVersionId, userId, coordinate.Key)))
                .Where(cell => cell is not null)
                .Select(cell => cell!.Clone())
                .ToList();
            return Task.FromResult<IReadOnlyList<PlanningCell>>(cells);
        }
    }
    public Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken) => _inner.UpsertCellsAsync(cells, cancellationToken);
    public Task UpsertDraftCellsAsync(long scenarioVersionId, string userId, IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            foreach (var cell in cells)
            {
                _draftCells[(scenarioVersionId, userId, cell.Coordinate.Key)] = cell.Clone();
            }
        }

        return Task.CompletedTask;
    }
    public Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken) => _inner.AppendAuditAsync(audit, cancellationToken);
    public Task<long> GetNextActionIdAsync(CancellationToken cancellationToken) => _inner.GetNextActionIdAsync(cancellationToken);
    public Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken) => _inner.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
    public Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken) => _inner.GetNextCommandBatchIdAsync(cancellationToken);
    public Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken) => _inner.AppendCommandBatchAsync(batch, cancellationToken);
    public Task<long> GetNextDraftCommandBatchIdAsync(CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            return Task.FromResult(_nextDraftCommandBatchId++);
        }
    }

    public Task AppendDraftCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            for (var index = 0; index < _draftCommandBatches.Count; index += 1)
            {
                var existing = _draftCommandBatches[index];
                if (existing.ScenarioVersionId == batch.ScenarioVersionId
                    && string.Equals(existing.UserId, batch.UserId, StringComparison.Ordinal)
                    && existing.IsUndone
                    && existing.SupersededByBatchId is null)
                {
                    _draftCommandBatches[index] = existing with { SupersededByBatchId = batch.CommandBatchId };
                }
            }

            _draftCommandBatches.Add(CloneBatch(batch));
        }

        return Task.CompletedTask;
    }
    public Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.UndoLatestCommandAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken) => _inner.RedoLatestCommandAsync(scenarioVersionId, userId, limit, cancellationToken);
    public Task<PlanningUndoRedoAvailability> GetDraftUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            var retained = GetRetainedDraftHistory(scenarioVersionId, userId, limit);
            var undoDepth = retained.Count(batch => !batch.IsUndone);
            var redoDepth = retained.Count(batch => batch.IsUndone);
            return Task.FromResult(new PlanningUndoRedoAvailability(undoDepth > 0, redoDepth > 0, undoDepth, redoDepth, limit));
        }
    }

    public Task<PlanningCommandBatch?> UndoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            var candidate = GetRetainedDraftHistory(scenarioVersionId, userId, limit)
                .Where(batch => !batch.IsUndone)
                .OrderByDescending(batch => batch.CommandBatchId)
                .FirstOrDefault();
            if (candidate is null)
            {
                return Task.FromResult<PlanningCommandBatch?>(null);
            }

            foreach (var delta in candidate.Deltas)
            {
                _draftCells[(scenarioVersionId, userId, delta.Coordinate.Key)] = delta.OldState.ToPlanningCell(delta.Coordinate);
            }

            var undone = candidate with { IsUndone = true, UndoneAt = DateTimeOffset.UtcNow };
            ReplaceDraftBatch(undone);
            return Task.FromResult<PlanningCommandBatch?>(CloneBatch(undone));
        }
    }

    public Task<PlanningCommandBatch?> RedoLatestDraftCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        lock (_draftGate)
        {
            var candidate = GetRetainedDraftHistory(scenarioVersionId, userId, limit)
                .Where(batch => batch.IsUndone)
                .OrderByDescending(batch => batch.UndoneAt ?? batch.CreatedAt)
                .ThenByDescending(batch => batch.CommandBatchId)
                .FirstOrDefault();
            if (candidate is null)
            {
                return Task.FromResult<PlanningCommandBatch?>(null);
            }

            foreach (var delta in candidate.Deltas)
            {
                _draftCells[(scenarioVersionId, userId, delta.Coordinate.Key)] = delta.NewState.ToPlanningCell(delta.Coordinate);
            }

            var redone = candidate with { IsUndone = false, UndoneAt = null };
            ReplaceDraftBatch(redone);
            return Task.FromResult<PlanningCommandBatch?>(CloneBatch(redone));
        }
    }
    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken) => _inner.GetGridSliceAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, cancellationToken);
    public Task<GridBranchResponse> GetGridBranchRowsAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken) => _inner.GetGridBranchRowsAsync(scenarioVersionId, parentProductNodeId, cancellationToken);
    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) => _inner.AddRowAsync(request, cancellationToken);
    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) => _inner.DeleteRowAsync(scenarioVersionId, productNodeId, cancellationToken);
    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) => _inner.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, cancellationToken);
    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) => _inner.EnsureYearAsync(scenarioVersionId, fiscalYear, cancellationToken);
    public async Task CommitDraftAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken)
    {
        List<PlanningCell> cellsToCommit;
        lock (_draftGate)
        {
            cellsToCommit = _draftCells
                .Where(entry => entry.Key.ScenarioVersionId == scenarioVersionId && string.Equals(entry.Key.UserId, userId, StringComparison.Ordinal))
                .Select(entry => entry.Value.Clone())
                .ToList();

            foreach (var key in _draftCells.Keys
                         .Where(key => key.ScenarioVersionId == scenarioVersionId && string.Equals(key.UserId, userId, StringComparison.Ordinal))
                         .ToList())
            {
                _draftCells.Remove(key);
            }

            _draftCommandBatches.RemoveAll(batch =>
                batch.ScenarioVersionId == scenarioVersionId
                && string.Equals(batch.UserId, userId, StringComparison.Ordinal));
        }

        if (cellsToCommit.Count > 0)
        {
            await _inner.UpsertCellsAsync(cellsToCommit, cancellationToken);
        }
    }
    public Task RecordSaveCheckpointAsync(long scenarioVersionId, string userId, string mode, DateTimeOffset savedAt, CancellationToken cancellationToken) => Task.CompletedTask;
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

    private List<PlanningCommandBatch> GetRetainedDraftHistory(long scenarioVersionId, string userId, int limit) =>
        _draftCommandBatches
            .Where(batch => batch.ScenarioVersionId == scenarioVersionId
                && string.Equals(batch.UserId, userId, StringComparison.Ordinal)
                && batch.SupersededByBatchId is null)
            .OrderByDescending(batch => batch.CommandBatchId)
            .Take(limit)
            .Select(CloneBatch)
            .ToList();

    private void ReplaceDraftBatch(PlanningCommandBatch replacement)
    {
        var index = _draftCommandBatches.FindIndex(batch => batch.CommandBatchId == replacement.CommandBatchId);
        if (index >= 0)
        {
            _draftCommandBatches[index] = CloneBatch(replacement);
        }
    }

    private static PlanningCommandBatch CloneBatch(PlanningCommandBatch batch) =>
        batch with
        {
            Deltas = batch.Deltas
                .Select(delta => new PlanningCommandCellDelta(delta.Coordinate, delta.OldState, delta.NewState, delta.ChangeKind))
                .ToList()
        };
}
