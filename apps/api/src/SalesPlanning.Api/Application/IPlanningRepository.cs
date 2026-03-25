using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public interface IPlanningRepository
{
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
    Task<IReadOnlyList<HierarchyDepartmentRecord>> GetHierarchyMappingsAsync(CancellationToken cancellationToken);
    Task UpsertHierarchyDepartmentAsync(string departmentLabel, CancellationToken cancellationToken);
    Task UpsertHierarchyClassAsync(string departmentLabel, string classLabel, CancellationToken cancellationToken);
    Task UpsertHierarchySubclassAsync(string departmentLabel, string classLabel, string subclassLabel, CancellationToken cancellationToken);
    Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken);
    Task ResetAsync(CancellationToken cancellationToken);
}
