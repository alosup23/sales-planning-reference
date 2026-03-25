using SalesPlanning.Api.Contracts;

namespace SalesPlanning.Api.Application;

public interface IPlanningService
{
    Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, CancellationToken cancellationToken);
    Task<EditCellsResponse> ApplyEditsAsync(EditCellsRequest request, string userId, CancellationToken cancellationToken);
    Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken);
    Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken);
    Task<IReadOnlyList<AuditTrailItemDto>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken);
    Task<AddRowResponse> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken);
    Task<DeleteEntityResponse> DeleteRowAsync(DeleteRowRequest request, CancellationToken cancellationToken);
    Task<DeleteEntityResponse> DeleteYearAsync(DeleteYearRequest request, CancellationToken cancellationToken);
    Task<GenerateNextYearResponse> GenerateNextYearAsync(GenerateNextYearRequest request, string userId, CancellationToken cancellationToken);
    Task<HierarchyMappingResponse> GetHierarchyMappingsAsync(CancellationToken cancellationToken);
    Task<HierarchyMappingResponse> AddHierarchyDepartmentAsync(AddHierarchyDepartmentRequest request, CancellationToken cancellationToken);
    Task<HierarchyMappingResponse> AddHierarchyClassAsync(AddHierarchyClassRequest request, CancellationToken cancellationToken);
    Task<HierarchyMappingResponse> AddHierarchySubclassAsync(AddHierarchySubclassRequest request, CancellationToken cancellationToken);
    Task<PlanningInsightResponse> GetPlanningInsightsAsync(long scenarioVersionId, long storeId, long productNodeId, long yearTimePeriodId, CancellationToken cancellationToken);
    Task<ApplyGrowthFactorResponse> ApplyGrowthFactorAsync(ApplyGrowthFactorRequest request, string userId, CancellationToken cancellationToken);
    Task<SaveScenarioResponse> SaveScenarioAsync(SaveScenarioRequest request, string userId, CancellationToken cancellationToken);
    Task<ImportWorkbookResponse> ImportWorkbookAsync(long scenarioVersionId, Stream workbookStream, string fileName, string userId, CancellationToken cancellationToken);
    Task<(byte[] Content, string FileName)> ExportWorkbookAsync(long scenarioVersionId, CancellationToken cancellationToken);
    Task ResetAsync(CancellationToken cancellationToken);
}
