using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public interface IPlanningService
{
    Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken);
    Task<EditCellsResponse> ApplyEditsAsync(EditCellsRequest request, string userId, CancellationToken cancellationToken);
    Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken);
    Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken);
    Task<IReadOnlyList<AuditTrailItemDto>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken);
    Task ResetAsync(CancellationToken cancellationToken);
}
