using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public interface IPlanningRepository
{
    Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken);
    Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken);
    Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken);
    Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken);
    Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken);
    Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken);
    Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken);
    Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken);
    Task ResetAsync(CancellationToken cancellationToken);
}
