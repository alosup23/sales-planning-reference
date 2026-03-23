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

    [HttpPost("rows")]
    public Task<AddRowResponse> AddRow([FromBody] AddRowRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddRowAsync(request, cancellationToken);
    }

    [HttpGet("hierarchy-mappings")]
    public Task<HierarchyMappingResponse> GetHierarchyMappings(CancellationToken cancellationToken)
    {
        return _planningService.GetHierarchyMappingsAsync(cancellationToken);
    }

    [HttpPost("hierarchy-mappings/categories")]
    public Task<HierarchyMappingResponse> AddHierarchyCategory([FromBody] AddHierarchyCategoryRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddHierarchyCategoryAsync(request, cancellationToken);
    }

    [HttpPost("hierarchy-mappings/subcategories")]
    public Task<HierarchyMappingResponse> AddHierarchySubcategory([FromBody] AddHierarchySubcategoryRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddHierarchySubcategoryAsync(request, cancellationToken);
    }

    [HttpPost("imports/workbook")]
    [RequestSizeLimit(10_000_000)]
    public async Task<ImportWorkbookResponse> ImportWorkbook(
        [FromForm] long scenarioVersionId,
        [FromForm] long measureId,
        [FromForm] IFormFile file,
        CancellationToken cancellationToken)
    {
        await using var stream = file.OpenReadStream();
        return await _planningService.ImportWorkbookAsync(
            scenarioVersionId,
            measureId,
            stream,
            file.FileName,
            User.Identity?.Name ?? "demo.user",
            cancellationToken);
    }

    [HttpPost("test/reset")]
    public Task Reset(CancellationToken cancellationToken)
    {
        return _planningService.ResetAsync(cancellationToken);
    }
}
