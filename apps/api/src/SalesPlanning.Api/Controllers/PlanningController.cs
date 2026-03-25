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
    public Task<GridSliceResponse> GetGridSlice([FromQuery] long scenarioVersionId = 1, CancellationToken cancellationToken = default)
    {
        return _planningService.GetGridSliceAsync(scenarioVersionId, cancellationToken);
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

    [HttpPost("rows/delete")]
    public Task<DeleteEntityResponse> DeleteRow([FromBody] DeleteRowRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteRowAsync(request, cancellationToken);
    }

    [HttpPost("years/delete")]
    public Task<DeleteEntityResponse> DeleteYear([FromBody] DeleteYearRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteYearAsync(request, cancellationToken);
    }

    [HttpPost("years/generate-next")]
    public Task<GenerateNextYearResponse> GenerateNextYear([FromBody] GenerateNextYearRequest request, CancellationToken cancellationToken)
    {
        return _planningService.GenerateNextYearAsync(request, User.Identity?.Name ?? "demo.user", cancellationToken);
    }

    [HttpGet("hierarchy-mappings")]
    public Task<HierarchyMappingResponse> GetHierarchyMappings(CancellationToken cancellationToken)
    {
        return _planningService.GetHierarchyMappingsAsync(cancellationToken);
    }

    [HttpPost("hierarchy-mappings/departments")]
    public Task<HierarchyMappingResponse> AddHierarchyDepartment([FromBody] AddHierarchyDepartmentRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddHierarchyDepartmentAsync(request, cancellationToken);
    }

    [HttpPost("hierarchy-mappings/classes")]
    public Task<HierarchyMappingResponse> AddHierarchyClass([FromBody] AddHierarchyClassRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddHierarchyClassAsync(request, cancellationToken);
    }

    [HttpPost("hierarchy-mappings/subclasses")]
    public Task<HierarchyMappingResponse> AddHierarchySubclass([FromBody] AddHierarchySubclassRequest request, CancellationToken cancellationToken)
    {
        return _planningService.AddHierarchySubclassAsync(request, cancellationToken);
    }

    [HttpGet("insights")]
    public Task<PlanningInsightResponse> GetPlanningInsights(
        [FromQuery] long scenarioVersionId,
        [FromQuery] long storeId,
        [FromQuery] long productNodeId,
        [FromQuery] long yearTimePeriodId,
        CancellationToken cancellationToken)
    {
        return _planningService.GetPlanningInsightsAsync(scenarioVersionId, storeId, productNodeId, yearTimePeriodId, cancellationToken);
    }

    [HttpPost("growth-factors/apply")]
    public Task<ApplyGrowthFactorResponse> ApplyGrowthFactor([FromBody] ApplyGrowthFactorRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplyGrowthFactorAsync(request, User.Identity?.Name ?? "demo.user", cancellationToken);
    }

    [HttpPost("save")]
    public Task<SaveScenarioResponse> SaveScenario([FromBody] SaveScenarioRequest request, CancellationToken cancellationToken)
    {
        return _planningService.SaveScenarioAsync(request, User.Identity?.Name ?? "demo.user", cancellationToken);
    }

    [HttpPost("imports/workbook")]
    [RequestSizeLimit(10_000_000)]
    public async Task<ImportWorkbookResponse> ImportWorkbook(
        [FromForm] long scenarioVersionId,
        [FromForm] IFormFile file,
        CancellationToken cancellationToken)
    {
        await using var stream = file.OpenReadStream();
        return await _planningService.ImportWorkbookAsync(
            scenarioVersionId,
            stream,
            file.FileName,
            User.Identity?.Name ?? "demo.user",
            cancellationToken);
    }

    [HttpGet("exports/workbook")]
    public async Task<IActionResult> ExportWorkbook([FromQuery] long scenarioVersionId = 1, CancellationToken cancellationToken = default)
    {
        var result = await _planningService.ExportWorkbookAsync(scenarioVersionId, cancellationToken);
        return File(result.Content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", result.FileName);
    }

    [HttpPost("test/reset")]
    public Task Reset(CancellationToken cancellationToken)
    {
        return _planningService.ResetAsync(cancellationToken);
    }
}
