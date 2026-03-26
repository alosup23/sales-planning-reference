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
    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, CancellationToken cancellationToken) => _inner.GetGridSliceAsync(scenarioVersionId, cancellationToken);
    public Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken) => _inner.AddRowAsync(request, cancellationToken);
    public Task<int> DeleteRowAsync(long scenarioVersionId, long productNodeId, CancellationToken cancellationToken) => _inner.DeleteRowAsync(scenarioVersionId, productNodeId, cancellationToken);
    public Task<int> DeleteYearAsync(long scenarioVersionId, long yearTimePeriodId, CancellationToken cancellationToken) => _inner.DeleteYearAsync(scenarioVersionId, yearTimePeriodId, cancellationToken);
    public Task EnsureYearAsync(long scenarioVersionId, int fiscalYear, CancellationToken cancellationToken) => _inner.EnsureYearAsync(scenarioVersionId, fiscalYear, cancellationToken);
    public Task<IReadOnlyList<StoreNodeMetadata>> GetStoresAsync(CancellationToken cancellationToken) => _inner.GetStoresAsync(cancellationToken);
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
