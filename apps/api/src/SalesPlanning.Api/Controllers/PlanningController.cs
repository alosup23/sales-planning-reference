using Microsoft.AspNetCore.Mvc;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Controllers;

[ApiController]
[Route("api/v1")]
public sealed class PlanningController : ControllerBase
{
    private readonly IPlanningService _planningService;

    public PlanningController(IPlanningService planningService)
    {
        _planningService = planningService;
    }

    [HttpGet("grid-slices")]
    public Task<GridSliceResponse> GetGridSlice([FromQuery] long scenarioVersionId = 1, [FromQuery] long measureId = 1, CancellationToken cancellationToken = default)
    {
        return _planningService.GetGridSliceAsync(scenarioVersionId, measureId, cancellationToken);
    }

    [HttpPost("cell-edits")]
    public Task<EditCellsResponse> EditCells([FromBody] EditCellsRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplyEditsAsync(request, User.Identity?.Name ?? "demo.user", cancellationToken);
    }

    [HttpPost("actions/splash")]
    public Task<SplashResponse> Splash([FromBody] SplashRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplySplashAsync(request, User.Identity?.Name ?? "demo.user", cancellationToken);
    }

    [HttpPost("locks")]
    public Task<LockCellsResponse> LockCells([FromBody] LockCellsRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplyLockAsync(request, User.Identity?.Name ?? "demo.manager", cancellationToken);
    }

    [HttpGet("audit")]
    public Task<IReadOnlyList<AuditTrailItemDto>> GetAudit(
        [FromQuery] long scenarioVersionId,
        [FromQuery] long measureId,
        [FromQuery] long storeId,
        [FromQuery] long productNodeId,
        CancellationToken cancellationToken)
    {
        return _planningService.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
    }

    [HttpPost("test/reset")]
    public Task Reset(CancellationToken cancellationToken)
    {
        return _planningService.ResetAsync(cancellationToken);
    }
}
