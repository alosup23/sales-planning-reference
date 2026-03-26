using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public interface IPlanningRepository
{
    Task<T> ExecuteAtomicAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken);
    Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken);
    Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, CancellationToken cancellationToken);
    Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken);
    Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken);
    Task<long> GetNextActionIdAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken);
    Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, CancellationToken cancellationToken);
    Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken);
    Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken);
    Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken);
    Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken);
    Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken);
    Task<StoreNodeMetadata> UpsertStoreProfileAsync(long scenarioVersionId, StoreNodeMetadata storeProfile, CancellationToken cancellationToken);
    Task DeleteStoreProfileAsync(long scenarioVersionId, long storeId, CancellationToken cancellationToken);
    Task InactivateStoreProfileAsync(long storeId, CancellationToken cancellationToken);
    Task<IReadOnlyList<StoreProfileOptionValue>> GetStoreProfileOptionsAsync(CancellationToken cancellationToken);
    Task UpsertStoreProfileOptionAsync(string fieldName, string value, bool isActive, CancellationToken cancellationToken);
    Task DeleteStoreProfileOptionAsync(string fieldName, string value, CancellationToken cancellationToken);
    Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken);
    Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken);
    Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken);
    Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken);
    Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken);
    Task ResetAsync(CancellationToken cancellationToken);
}
