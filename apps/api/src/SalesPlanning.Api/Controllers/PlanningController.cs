using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Controllers;

[ApiController]
[Route("api/v1")]
public sealed class PlanningController : ControllerBase
{
    private readonly IPlanningService _planningService;
    private readonly bool _enableTestReset;
    private readonly bool _authEnabled;

    public PlanningController(IPlanningService planningService, IConfiguration configuration)
    {
        _planningService = planningService;
        _enableTestReset = string.Equals(configuration["EnableTestReset"], "true", StringComparison.OrdinalIgnoreCase);
        _authEnabled = !string.Equals(configuration["PlanningSecurityMode"], "disabled", StringComparison.OrdinalIgnoreCase);
    }

    [HttpGet("grid-slices")]
    public Task<GridSliceResponse> GetGridSlice([FromQuery] long scenarioVersionId = 1, [FromQuery] long? selectedStoreId = null, CancellationToken cancellationToken = default)
    {
        return _planningService.GetGridSliceAsync(scenarioVersionId, selectedStoreId, cancellationToken);
    }

    [HttpPost("cell-edits")]
    public Task<EditCellsResponse> EditCells([FromBody] EditCellsRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplyEditsAsync(request, GetRequiredUserId(), cancellationToken);
    }

    [HttpPost("actions/splash")]
    public Task<SplashResponse> Splash([FromBody] SplashRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplySplashAsync(request, GetRequiredUserId(), cancellationToken);
    }

    [HttpPost("locks")]
    public Task<LockCellsResponse> LockCells([FromBody] LockCellsRequest request, CancellationToken cancellationToken)
    {
        return _planningService.ApplyLockAsync(request, GetRequiredUserId(), cancellationToken);
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
        return _planningService.GenerateNextYearAsync(request, GetRequiredUserId(), cancellationToken);
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
        return _planningService.ApplyGrowthFactorAsync(request, GetRequiredUserId(), cancellationToken);
    }

    [HttpPost("save")]
    public Task<SaveScenarioResponse> SaveScenario([FromBody] SaveScenarioRequest request, CancellationToken cancellationToken)
    {
        return _planningService.SaveScenarioAsync(request, GetRequiredUserId(), cancellationToken);
    }

    [HttpGet("store-profiles")]
    public Task<StoreProfileResponse> GetStoreProfiles(CancellationToken cancellationToken)
    {
        return _planningService.GetStoreProfilesAsync(cancellationToken);
    }

    [HttpPost("store-profiles")]
    public Task<StoreProfileDto> UpsertStoreProfile([FromBody] UpsertStoreProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.UpsertStoreProfileAsync(request, cancellationToken);
    }

    [HttpPost("store-profiles/delete")]
    public Task DeleteStoreProfile([FromBody] DeleteStoreProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteStoreProfileAsync(request, cancellationToken);
    }

    [HttpPost("store-profiles/inactivate")]
    public Task<StoreProfileDto> InactivateStoreProfile([FromBody] InactivateStoreProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.InactivateStoreProfileAsync(request, cancellationToken);
    }

    [HttpGet("store-profile-options")]
    public Task<StoreProfileOptionsResponse> GetStoreProfileOptions(CancellationToken cancellationToken)
    {
        return _planningService.GetStoreProfileOptionsAsync(cancellationToken);
    }

    [HttpPost("store-profile-options")]
    public Task<StoreProfileOptionsResponse> UpsertStoreProfileOption([FromBody] UpsertStoreProfileOptionRequest request, CancellationToken cancellationToken)
    {
        return _planningService.UpsertStoreProfileOptionAsync(request, cancellationToken);
    }

    [HttpPost("store-profile-options/delete")]
    public Task<StoreProfileOptionsResponse> DeleteStoreProfileOption([FromBody] DeleteStoreProfileOptionRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteStoreProfileOptionAsync(request, cancellationToken);
    }

    [HttpPost("imports/store-profiles")]
    [RequestSizeLimit(10_000_000)]
    public async Task<StoreProfileImportResponse> ImportStoreProfiles(
        [FromForm] IFormFile file,
        CancellationToken cancellationToken)
    {
        await using var stream = file.OpenReadStream();
        return await _planningService.ImportStoreProfilesAsync(stream, file.FileName, cancellationToken);
    }

    [HttpGet("exports/store-profiles")]
    public async Task<IActionResult> ExportStoreProfiles(CancellationToken cancellationToken = default)
    {
        var result = await _planningService.ExportStoreProfilesAsync(cancellationToken);
        return File(result.Content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", result.FileName);
    }

    [HttpGet("product-profiles")]
    public Task<ProductProfileResponse> GetProductProfiles(
        [FromQuery] string? searchTerm,
        [FromQuery] int pageNumber = 1,
        [FromQuery] int pageSize = 200,
        CancellationToken cancellationToken = default)
    {
        return _planningService.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
    }

    [HttpPost("product-profiles")]
    public Task<ProductProfileDto> UpsertProductProfile([FromBody] UpsertProductProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.UpsertProductProfileAsync(request, cancellationToken);
    }

    [HttpPost("product-profiles/delete")]
    public Task DeleteProductProfile([FromBody] DeleteProductProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteProductProfileAsync(request, cancellationToken);
    }

    [HttpPost("product-profiles/inactivate")]
    public Task<ProductProfileDto> InactivateProductProfile([FromBody] InactivateProductProfileRequest request, CancellationToken cancellationToken)
    {
        return _planningService.InactivateProductProfileAsync(request, cancellationToken);
    }

    [HttpGet("product-profile-options")]
    public Task<ProductProfileOptionsResponse> GetProductProfileOptions(CancellationToken cancellationToken)
    {
        return _planningService.GetProductProfileOptionsAsync(cancellationToken);
    }

    [HttpPost("product-profile-options")]
    public Task<ProductProfileOptionsResponse> UpsertProductProfileOption([FromBody] UpsertProductProfileOptionRequest request, CancellationToken cancellationToken)
    {
        return _planningService.UpsertProductProfileOptionAsync(request, cancellationToken);
    }

    [HttpPost("product-profile-options/delete")]
    public Task<ProductProfileOptionsResponse> DeleteProductProfileOption([FromBody] DeleteProductProfileOptionRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteProductProfileOptionAsync(request, cancellationToken);
    }

    [HttpGet("product-hierarchy")]
    public Task<ProductHierarchyResponse> GetProductHierarchy(CancellationToken cancellationToken)
    {
        return _planningService.GetProductHierarchyCatalogAsync(cancellationToken);
    }

    [HttpPost("product-hierarchy")]
    public Task<ProductHierarchyResponse> UpsertProductHierarchy([FromBody] UpsertProductHierarchyRequest request, CancellationToken cancellationToken)
    {
        return _planningService.UpsertProductHierarchyCatalogAsync(request, cancellationToken);
    }

    [HttpPost("product-hierarchy/delete")]
    public Task<ProductHierarchyResponse> DeleteProductHierarchy([FromBody] DeleteProductHierarchyRequest request, CancellationToken cancellationToken)
    {
        return _planningService.DeleteProductHierarchyCatalogAsync(request, cancellationToken);
    }

    [HttpPost("imports/product-profiles")]
    [RequestSizeLimit(30_000_000)]
    public async Task<ProductProfileImportResponse> ImportProductProfiles(
        [FromForm] IFormFile file,
        CancellationToken cancellationToken)
    {
        await using var stream = file.OpenReadStream();
        return await _planningService.ImportProductProfilesAsync(stream, file.FileName, cancellationToken);
    }

    [HttpGet("exports/product-profiles")]
    public async Task<IActionResult> ExportProductProfiles(CancellationToken cancellationToken = default)
    {
        var result = await _planningService.ExportProductProfilesAsync(cancellationToken);
        return File(result.Content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", result.FileName);
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
            GetRequiredUserId(),
            cancellationToken);
    }

    [HttpGet("exports/workbook")]
    public async Task<IActionResult> ExportWorkbook([FromQuery] long scenarioVersionId = 1, CancellationToken cancellationToken = default)
    {
        var result = await _planningService.ExportWorkbookAsync(scenarioVersionId, cancellationToken);
        return File(result.Content, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", result.FileName);
    }

    [HttpPost("test/reset")]
    [AllowAnonymous]
    public Task<IResult> Reset(CancellationToken cancellationToken)
    {
        if (!_enableTestReset)
        {
            return Task.FromResult(Results.NotFound());
        }

        return ResetInternalAsync(cancellationToken);
    }

    private async Task<IResult> ResetInternalAsync(CancellationToken cancellationToken)
    {
        await _planningService.ResetAsync(cancellationToken);
        return Results.Ok();
    }

    private string GetRequiredUserId()
    {
        if (!_authEnabled)
        {
            return "local.test.user";
        }

        return User.Identity?.Name
            ?? User.FindFirst("preferred_username")?.Value
            ?? User.FindFirst("name")?.Value
            ?? User.FindFirst("oid")?.Value
            ?? throw new UnauthorizedAccessException("Authenticated user identity was not available.");
    }
}
