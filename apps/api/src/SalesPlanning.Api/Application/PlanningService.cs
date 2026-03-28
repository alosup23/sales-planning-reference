using System.Globalization;
using System.Text.Json;
using ClosedXML.Excel;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService : IPlanningService
{
    private static readonly string[] MonthLabels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    private static readonly string[] ImportHeaders =
    [
        "Store", "Department", "Class", "Subclass", "Year", "Month",
        "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"
    ];
    private static readonly string[] StoreProfileImportHeaders =
    [
        "CompCode", "BranchName", "State", "Branch Type", "Latitude", "Longitude", "Region", "Opening Date",
        "SSSG", "Sales Type", "Status", "Storey", "Building Status", "GTA", "NTA", "RSOM", "DM", "Rental",
        "Lifecycle State", "Ramp Profile", "Active", "Store Cluster Role", "Store Capacity SqFt", "Store Format Tier",
        "Catchment Type", "Demographic Segment", "Climate Zone", "Fulfilment Enabled", "Online Fulfilment Node",
        "Store Opening Season", "Store Closure Date", "Refurbishment Date", "Store Priority"
    ];
    private static readonly string[] ProductProfileImportHeaders =
    [
        "SKU Variant", "Description", "Description2", "Price", "Cost", "DptNo", "ClssNo", "BrandNo",
        "Department", "Class", "Brand", "Rev. Dept", "Rev. Class", "Subclass", "Prod Group", "Prod Type",
        "Active Flag", "Order Flag", "Brand Type", "Launch Month", "Gender", "Size", "Collection", "Promo", "Ramadhan Promo",
        "Supplier", "Lifecycle Stage", "Age Stage", "Gender Target", "Material", "Pack Size", "Size Range", "Colour Family",
        "KVI Flag", "Markdown Eligible", "Markdown Floor Price", "Minimum Margin Pct", "Price Ladder Group",
        "Good Better Best Tier", "Season Code", "Event Code", "Launch Date", "End Of Life Date", "Substitute Group",
        "Companion Group", "Replenishment Type", "Lead Time Days", "MOQ", "Case Pack", "Starting Inventory",
        "Projected Stock On Hand", "Sell Through Target Pct", "Weeks Of Cover Target"
    ];
    private static readonly string[] ProductHierarchyImportHeaders =
    [
        "DptNo", "ClssNo", "Dept", "Class", "Prod Group"
    ];
    private const string RemarkHeader = "Remark";
    private const string ExpectedValueHeader = "Expected Value";
    private const int RequiredProductProfileImportHeaderCount = 25;
    private readonly IPlanningRepository _repository;
    private readonly ISplashAllocator _splashAllocator;

    public PlanningService(IPlanningRepository repository, ISplashAllocator splashAllocator)
    {
        _repository = repository;
        _splashAllocator = splashAllocator;
    }

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long? selectedStoreId, string? selectedDepartmentLabel, IReadOnlyCollection<long>? expandedProductNodeIds, bool expandAllBranches, CancellationToken cancellationToken)
    {
        return _repository.GetGridSliceAsync(scenarioVersionId, selectedStoreId, selectedDepartmentLabel, expandedProductNodeIds, expandAllBranches, cancellationToken);
    }

    public Task<GridBranchResponse> GetGridBranchRowsAsync(long scenarioVersionId, long parentProductNodeId, CancellationToken cancellationToken)
    {
        return _repository.GetGridBranchRowsAsync(scenarioVersionId, parentProductNodeId, cancellationToken);
    }

    public Task<EditCellsResponse> ApplyEditsAsync(EditCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var metadata = await _repository.GetMetadataAsync(ct);
            var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, ct);
            var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

            foreach (var edit in request.Cells)
            {
                var coordinate = new PlanningCellCoordinate(request.ScenarioVersionId, request.MeasureId, edit.StoreId, edit.ProductNodeId, edit.TimePeriodId);
                ValidateDirectEdit(coordinate, edit.RowVersion, workingCells, metadata);

                if (string.Equals(edit.EditMode, "input", StringComparison.OrdinalIgnoreCase))
                {
                    ApplyLeafMeasureEdit(coordinate, edit.NewValue, workingCells, metadata);
                }
                else
                {
                    ApplyAggregateAllocation(
                        request.ScenarioVersionId,
                        request.MeasureId,
                        edit.TimePeriodId,
                        [new SplashScopeRootDto(edit.StoreId, edit.ProductNodeId)],
                        edit.NewValue,
                        "existing_plan",
                        null,
                        workingCells,
                        metadata);
                }
            }

            RecalculateAll(workingCells, metadata, request.ScenarioVersionId);
            var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "manual-edit", ct);
            var actionId = await AppendAuditAsync("manual_edit", "manual", userId, request.Comment, deltas, ct);
            await AppendCommandBatchAsync(
                request.ScenarioVersionId,
                userId,
                "manual_edit",
                new
                {
                    request.MeasureId,
                    request.Comment,
                    request.Cells
                },
                deltas,
                ct);
            var availability = await _repository.GetUndoRedoAvailabilityAsync(request.ScenarioVersionId, userId, UndoRedoLimit, ct);
            return new EditCellsResponse(actionId, deltas.Count, "applied", BuildGridPatch(request.ScenarioVersionId, deltas), ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var metadata = await _repository.GetMetadataAsync(ct);
            var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, ct);
            var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
            var scopeRoots = (request.ScopeRoots is { Count: > 0 } ? request.ScopeRoots : [new SplashScopeRootDto(request.SourceCell.StoreId, request.SourceCell.ProductNodeId)])
                .Distinct()
                .ToList();

            if (scopeRoots.Count == 1)
            {
                var directCoordinate = new PlanningCellCoordinate(
                    request.ScenarioVersionId,
                    request.MeasureId,
                    scopeRoots[0].StoreId,
                    scopeRoots[0].ProductNodeId,
                    request.SourceCell.TimePeriodId);
                if (IsLockedBySelfOrAncestor(directCoordinate, workingCells.Values, metadata))
                {
                    throw new InvalidOperationException($"Cell {directCoordinate.Key} is locked and cannot be changed.");
                }
            }

            ApplyAggregateAllocation(
                request.ScenarioVersionId,
                request.MeasureId,
                request.SourceCell.TimePeriodId,
                scopeRoots,
                request.TotalValue,
                request.Method,
                request.ManualWeights,
                workingCells,
                metadata);

            RecalculateAll(workingCells, metadata, request.ScenarioVersionId);
            var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "splash", ct);
            var actionId = await AppendAuditAsync("splash", request.Method, userId, request.Comment, deltas, ct);
            await AppendCommandBatchAsync(
                request.ScenarioVersionId,
                userId,
                "splash",
                new
                {
                    request.MeasureId,
                    request.SourceCell,
                    request.TotalValue,
                    request.Method,
                    request.ScopeRoots,
                    request.ManualWeights,
                    request.Comment
                },
                deltas,
                ct);
            var availability = await _repository.GetUndoRedoAvailabilityAsync(request.ScenarioVersionId, userId, UndoRedoLimit, ct);
            return new SplashResponse(
                actionId,
                "applied",
                deltas.Count,
                deltas.Count(delta => delta.OldState.IsLocked),
                BuildGridPatch(request.ScenarioVersionId, deltas),
                ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var scenarioCells = (await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, ct))
                .Where(cell => cell.Coordinate.MeasureId == request.MeasureId)
                .ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
            var originalCells = new List<PlanningCell>();
            var targetedCells = new List<PlanningCell>();

            foreach (var coordinate in request.Coordinates)
            {
                var key = new PlanningCellCoordinate(
                    request.ScenarioVersionId,
                    request.MeasureId,
                    coordinate.StoreId,
                    coordinate.ProductNodeId,
                    coordinate.TimePeriodId).Key;

                if (!scenarioCells.TryGetValue(key, out var cell))
                {
                    throw new InvalidOperationException($"Cell {key} was not found.");
                }

                originalCells.Add(cell.Clone());
                cell.IsLocked = request.Locked;
                cell.LockReason = request.Reason;
                cell.LockedBy = request.Locked ? userId : null;
                targetedCells.Add(cell);
            }

            var deltas = await PersistScenarioChangesAsync(originalCells, targetedCells, request.Locked ? "lock" : "unlock", ct);
            await AppendAuditAsync(request.Locked ? "lock" : "unlock", "lock", userId, request.Reason, deltas, ct);
            await AppendCommandBatchAsync(
                request.ScenarioVersionId,
                userId,
                request.Locked ? "lock" : "unlock",
                new
                {
                    request.MeasureId,
                    request.Reason,
                    request.Locked,
                    request.Coordinates
                },
                deltas,
                ct);
            var availability = await _repository.GetUndoRedoAvailabilityAsync(request.ScenarioVersionId, userId, UndoRedoLimit, ct);
            return new LockCellsResponse(request.Coordinates.Count, request.Locked, ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public Task<IReadOnlyList<AuditTrailItemDto>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        return GetAuditInternalAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
    }

    public async Task<AddRowResponse> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        var node = await _repository.AddRowAsync(request, cancellationToken);
        return new AddRowResponse(node.StoreId, node.ProductNodeId, node.Label, node.Level, node.Path, node.IsLeaf);
    }

    public async Task<DeleteEntityResponse> DeleteRowAsync(DeleteRowRequest request, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        if (!metadata.ProductNodes.TryGetValue(request.ProductNodeId, out var node))
        {
            throw new InvalidOperationException($"Row {request.ProductNodeId} was not found.");
        }

        var deletedNodeCount = metadata.ProductNodes.Values.Count(candidate => IsDescendantProduct(candidate.ProductNodeId, request.ProductNodeId, metadata));
        var deletedCellCount = await _repository.DeleteRowAsync(request.ScenarioVersionId, request.ProductNodeId, cancellationToken);
        return new DeleteEntityResponse(deletedNodeCount, deletedCellCount, "deleted");
    }

    public async Task<DeleteEntityResponse> DeleteYearAsync(DeleteYearRequest request, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        if (!metadata.TimePeriods.TryGetValue(request.YearTimePeriodId, out var timePeriod) || !string.Equals(timePeriod.Grain, "year", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Year {request.YearTimePeriodId} was not found.");
        }

        var deletedNodeCount = metadata.TimePeriods.Values.Count(period => IsDescendantTime(period.TimePeriodId, request.YearTimePeriodId, metadata));
        var deletedCellCount = await _repository.DeleteYearAsync(request.ScenarioVersionId, request.YearTimePeriodId, cancellationToken);
        return new DeleteEntityResponse(deletedNodeCount, deletedCellCount, "deleted");
    }

    public Task<GenerateNextYearResponse> GenerateNextYearAsync(GenerateNextYearRequest request, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var metadata = await _repository.GetMetadataAsync(ct);
            if (!metadata.TimePeriods.TryGetValue(request.SourceYearTimePeriodId, out var sourceYear) || !string.Equals(sourceYear.Grain, "year", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException($"Year {request.SourceYearTimePeriodId} was not found.");
            }

            var sourceFiscalYear = (int)(request.SourceYearTimePeriodId / 100);
            var targetFiscalYear = sourceFiscalYear + 1;
            var targetYearTimePeriodId = targetFiscalYear * 100L;

            await _repository.EnsureYearAsync(request.ScenarioVersionId, targetFiscalYear, ct);
            metadata = await _repository.GetMetadataAsync(ct);

            var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, ct);
            var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
            var copiedCount = 0;

            var sourceMonthIds = GetLeafTimeIds(request.SourceYearTimePeriodId, metadata);
            var sourceMonthIdSet = sourceMonthIds.ToHashSet();
            var sourceMonthByTimeId = sourceMonthIds.ToDictionary(timeId => timeId, timeId => metadata.TimePeriods[timeId].Label);
            var targetMonthIds = GetLeafTimeIds(targetYearTimePeriodId, metadata)
                .ToDictionary(timeId => metadata.TimePeriods[timeId].Label);

            foreach (var sourceCell in workingCells.Values.Where(cell =>
                         cell.Coordinate.ScenarioVersionId == request.ScenarioVersionId &&
                         sourceMonthIdSet.Contains(cell.Coordinate.TimePeriodId) &&
                         metadata.ProductNodes[cell.Coordinate.ProductNodeId].IsLeaf &&
                         cell.InputValue is not null &&
                         PlanningMeasures.GetDefinition(cell.Coordinate.MeasureId).EditableAtLeaf &&
                         cell.Coordinate.MeasureId != PlanningMeasures.GrossProfitPercent))
            {
                var targetLabel = sourceMonthByTimeId[sourceCell.Coordinate.TimePeriodId];
                if (!targetMonthIds.TryGetValue(targetLabel, out var targetTimePeriodId))
                {
                    continue;
                }

                var targetKey = new PlanningCellCoordinate(
                    request.ScenarioVersionId,
                    sourceCell.Coordinate.MeasureId,
                    sourceCell.Coordinate.StoreId,
                    sourceCell.Coordinate.ProductNodeId,
                    targetTimePeriodId).Key;

                if (!workingCells.TryGetValue(targetKey, out var targetCell))
                {
                    continue;
                }

                targetCell.InputValue = sourceCell.InputValue;
                targetCell.OverrideValue = null;
                targetCell.IsSystemGeneratedOverride = false;
                targetCell.DerivedValue = sourceCell.InputValue ?? 0m;
                targetCell.EffectiveValue = sourceCell.InputValue ?? 0m;
                targetCell.CellKind = "input";
                targetCell.GrowthFactor = 1.0m;
                copiedCount += 1;
            }

            RecalculateAll(workingCells, metadata, request.ScenarioVersionId);
            var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "generate-next-year", ct);
            await AppendAuditAsync("generate_next_year", "copy-inputs", userId, $"Generated FY{targetFiscalYear % 100:00} from FY{sourceFiscalYear % 100:00}", deltas, ct);
            await AppendCommandBatchAsync(
                request.ScenarioVersionId,
                userId,
                "generate_next_year",
                new
                {
                    request.SourceYearTimePeriodId,
                    GeneratedYearTimePeriodId = targetYearTimePeriodId,
                    sourceFiscalYear,
                    targetFiscalYear
                },
                deltas,
                ct);
            return new GenerateNextYearResponse(request.SourceYearTimePeriodId, targetYearTimePeriodId, "applied", copiedCount);
        }, cancellationToken);
    }

    public async Task<HierarchyMappingResponse> GetHierarchyMappingsAsync(CancellationToken cancellationToken)
    {
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<HierarchyMappingResponse> AddHierarchyDepartmentAsync(AddHierarchyDepartmentRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertHierarchyDepartmentAsync(request.DepartmentLabel, cancellationToken);
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<HierarchyMappingResponse> AddHierarchyClassAsync(AddHierarchyClassRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertHierarchyClassAsync(request.DepartmentLabel, request.ClassLabel, cancellationToken);
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<HierarchyMappingResponse> AddHierarchySubclassAsync(AddHierarchySubclassRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertHierarchySubclassAsync(request.DepartmentLabel, request.ClassLabel, request.SubclassLabel, cancellationToken);
        return await BuildHierarchyMappingResponseAsync(cancellationToken);
    }

    public async Task<PlanningInsightResponse> GetPlanningInsightsAsync(long scenarioVersionId, long storeId, long productNodeId, long yearTimePeriodId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var cells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
        var targetNode = metadata.ProductNodes[productNodeId];
        var monthPeriods = metadata.TimePeriods.Values
            .Where(period => period.ParentTimePeriodId == yearTimePeriodId)
            .OrderBy(period => period.SortOrder)
            .ToList();
        var scopedLeafNodes = metadata.ProductNodes.Values
            .Where(node => node.StoreId == storeId && node.IsLeaf && IsDescendantProduct(node.ProductNodeId, productNodeId, metadata))
            .ToList();

        var monthlyRevenue = monthPeriods.Select(period => scopedLeafNodes.Sum(node =>
            cells.FirstOrDefault(cell => cell.Coordinate.Key == new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, node.StoreId, node.ProductNodeId, period.TimePeriodId).Key)?.EffectiveValue ?? 0m)).ToList();
        var monthlyQuantity = monthPeriods.Select(period => scopedLeafNodes.Sum(node =>
            cells.FirstOrDefault(cell => cell.Coordinate.Key == new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, node.StoreId, node.ProductNodeId, period.TimePeriodId).Key)?.EffectiveValue ?? 0m)).ToList();
        var yearlyRevenue = monthlyRevenue.Sum();
        var yearlyQuantity = monthlyQuantity.Sum();
        var asp = yearlyQuantity > 0m ? PlanningMath.NormalizeAsp(yearlyRevenue / yearlyQuantity) : 1.00m;
        var unitCost = scopedLeafNodes.Count == 0
            ? 0m
            : scopedLeafNodes.Average(node => cells.FirstOrDefault(cell => cell.Coordinate.Key == new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, node.StoreId, node.ProductNodeId, yearTimePeriodId).Key)?.EffectiveValue ?? 0m);
        var currentGrossProfitPercent = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);
        var seasonalityStrength = monthlyRevenue.Count == 0 || monthlyRevenue.Average() <= 0m
            ? 0m
            : Math.Round(monthlyRevenue.Max() / Math.Max(monthlyRevenue.Average(), 1m), 2, MidpointRounding.AwayFromZero);
        var recommendedForecastModel =
            monthlyRevenue.Count(value => value == 0m) > 4 ? "Croston / intermittent-demand" :
            seasonalityStrength >= 1.2m ? "Seasonal Naive + causal overlay" :
            "ETS / level-trend-seasonal";
        var recommendedPriceFloor = PlanningMath.NormalizeAsp(Math.Max(asp * 0.95m, unitCost * 1.05m));
        var recommendedPriceTarget = PlanningMath.NormalizeAsp(Math.Max(asp * 1.03m, unitCost * 1.12m));
        var recommendedPriceCeiling = PlanningMath.NormalizeAsp(Math.Max(asp * 1.08m, unitCost * 1.18m));
        var grossProfitOpportunity = PlanningMath.NormalizeGrossProfit((recommendedPriceTarget - asp) * yearlyQuantity);
        var quantityOpportunity = PlanningMath.NormalizeQuantity(yearlyQuantity * 0.04m);

        return new PlanningInsightResponse(
            "demo-heuristic-openai-ready",
            string.Join(" > ", targetNode.Path),
            recommendedForecastModel,
            PlanningMath.NormalizeGrossProfitPercent((decimal)seasonalityStrength * 10m),
            recommendedPriceFloor,
            recommendedPriceTarget,
            recommendedPriceCeiling,
            grossProfitOpportunity,
            quantityOpportunity,
            [
                $"Current GP% is {currentGrossProfitPercent:0.0}% with ASP {asp:0.00} and Unit Cost {unitCost:0.00}.",
                $"Recommended forecast family: {recommendedForecastModel}.",
                $"Price-band analysis suggests a target ASP of {recommendedPriceTarget:0.00} with estimated GP uplift of {grossProfitOpportunity:0}.",
                $"Seasonality strength for the selected year is {seasonalityStrength:0.00} and the branch stays coherent under the existing roll-up and splash rules."
            ]);
    }

    public Task<ApplyGrowthFactorResponse> ApplyGrowthFactorAsync(ApplyGrowthFactorRequest request, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var metadata = await _repository.GetMetadataAsync(ct);
            var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, ct);
            var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
            var scopeRoots = (request.ScopeRoots is { Count: > 0 } ? request.ScopeRoots : [new SplashScopeRootDto(request.SourceCell.StoreId, request.SourceCell.ProductNodeId)])
                .Distinct()
                .ToList();

            var growthFactor = PlanningMath.NormalizeGrowthFactor(request.GrowthFactor);
            var newValue = request.CurrentValue * growthFactor;
            var sourceCoordinate = new PlanningCellCoordinate(
                request.ScenarioVersionId,
                request.MeasureId,
                request.SourceCell.StoreId,
                request.SourceCell.ProductNodeId,
                request.SourceCell.TimePeriodId);

            var isLeafWrite = IsLeafWriteCoordinate(sourceCoordinate, metadata);
            if (isLeafWrite)
            {
                ValidateDirectEdit(sourceCoordinate, null, workingCells, metadata);
                ApplyLeafMeasureEdit(sourceCoordinate, newValue, workingCells, metadata);
            }
            else
            {
                ApplyAggregateAllocation(
                    request.ScenarioVersionId,
                    request.MeasureId,
                    request.SourceCell.TimePeriodId,
                    scopeRoots,
                    newValue,
                    request.SourceCell.TimePeriodId % 100 == 0 ? "seasonality_profile" : "existing_plan",
                    request.SourceCell.TimePeriodId % 100 == 0 ? BuildDefaultSeasonalityWeights(request.SourceCell.TimePeriodId, metadata) : null,
                    workingCells,
                    metadata);
            }

            ResetGrowthFactors(workingCells.Values, request.MeasureId);

            RecalculateAll(workingCells, metadata, request.ScenarioVersionId);
            var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "growth-factor", ct);
            var actionId = await AppendAuditAsync("growth_factor", "growth-factor", userId, request.Comment, deltas, ct);
            await AppendCommandBatchAsync(
                request.ScenarioVersionId,
                userId,
                "growth_factor",
                new
                {
                    request.MeasureId,
                    request.SourceCell,
                    request.CurrentValue,
                    request.GrowthFactor,
                    request.ScopeRoots,
                    request.Comment
                },
                deltas,
                ct);
            var availability = await _repository.GetUndoRedoAvailabilityAsync(request.ScenarioVersionId, userId, UndoRedoLimit, ct);
            return new ApplyGrowthFactorResponse(
                actionId,
                "applied",
                growthFactor,
                deltas.Count,
                BuildGridPatch(request.ScenarioVersionId, deltas),
                ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public async Task<SaveScenarioResponse> SaveScenarioAsync(SaveScenarioRequest request, string userId, CancellationToken cancellationToken)
    {
        await AppendAuditAsync("save", request.Mode, userId, $"Scenario {request.ScenarioVersionId} save checkpoint", [], cancellationToken);
        return new SaveScenarioResponse("saved", request.Mode, DateTimeOffset.UtcNow);
    }

    public async Task<UndoRedoAvailabilityDto> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken)
    {
        var availability = await _repository.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, UndoRedoLimit, cancellationToken);
        return ToUndoRedoAvailabilityDto(availability);
    }

    public Task<UndoPlanningActionResponse> UndoAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var batch = await _repository.UndoLatestCommandAsync(scenarioVersionId, userId, UndoRedoLimit, ct);
            if (batch is not null)
            {
                await AppendAuditAsync("undo", "undo", userId, $"Undo {batch.CommandKind}", InvertCommandDeltas(batch.Deltas), ct);
            }

            var availability = await _repository.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, UndoRedoLimit, ct);
            return new UndoPlanningActionResponse(batch is null ? "no-op" : "applied", ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public Task<RedoPlanningActionResponse> RedoAsync(long scenarioVersionId, string userId, CancellationToken cancellationToken)
    {
        return _repository.ExecuteAtomicAsync(async ct =>
        {
            var batch = await _repository.RedoLatestCommandAsync(scenarioVersionId, userId, UndoRedoLimit, ct);
            if (batch is not null)
            {
                await AppendAuditAsync("redo", "redo", userId, $"Redo {batch.CommandKind}", batch.Deltas, ct);
            }

            var availability = await _repository.GetUndoRedoAvailabilityAsync(scenarioVersionId, userId, UndoRedoLimit, ct);
            return new RedoPlanningActionResponse(batch is null ? "no-op" : "applied", ToUndoRedoAvailabilityDto(availability));
        }, cancellationToken);
    }

    public async Task<PlanningStoreScopeResponse> GetPlanningStoreScopesAsync(CancellationToken cancellationToken)
    {
        var stores = await _repository.GetStoresAsync(cancellationToken);
        return new PlanningStoreScopeResponse(
            stores
                .OrderByDescending(store => store.IsActive)
                .ThenBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
                .Select(store => new PlanningStoreScopeDto(
                    store.StoreId,
                    store.StoreLabel,
                    store.StoreCode,
                    store.ClusterLabel,
                    store.RegionLabel,
                    store.IsActive))
                .ToList());
    }

    public async Task<StoreProfileResponse> GetStoreProfilesAsync(CancellationToken cancellationToken)
    {
        var stores = await _repository.GetStoresAsync(cancellationToken);
        return new StoreProfileResponse(
            stores
                .OrderBy(store => store.StoreLabel, StringComparer.OrdinalIgnoreCase)
                .Select(ToStoreProfileDto)
                .ToList());
    }

    public async Task<StoreProfileDto> UpsertStoreProfileAsync(UpsertStoreProfileRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertStoreProfileAsync(
            request.ScenarioVersionId,
            NormalizeStoreProfile(request),
            cancellationToken);

        return ToStoreProfileDto(upserted);
    }

    public Task DeleteStoreProfileAsync(DeleteStoreProfileRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeleteStoreProfileAsync(request.ScenarioVersionId, request.StoreId, cancellationToken);
    }

    public async Task<StoreProfileDto> InactivateStoreProfileAsync(InactivateStoreProfileRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivateStoreProfileAsync(request.StoreId, cancellationToken);
        var stores = await _repository.GetStoresAsync(cancellationToken);
        var store = stores.Single(metadata => metadata.StoreId == request.StoreId);
        return ToStoreProfileDto(store);
    }

    public async Task<StoreProfileOptionsResponse> GetStoreProfileOptionsAsync(CancellationToken cancellationToken)
    {
        return new StoreProfileOptionsResponse((await _repository.GetStoreProfileOptionsAsync(cancellationToken))
            .OrderBy(option => option.FieldName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(option => option.Value, StringComparer.OrdinalIgnoreCase)
            .Select(option => new StoreProfileOptionDto(option.FieldName, option.Value, option.IsActive))
            .ToList());
    }

    public async Task<StoreProfileOptionsResponse> UpsertStoreProfileOptionAsync(UpsertStoreProfileOptionRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertStoreProfileOptionAsync(request.FieldName, request.Value, request.IsActive, cancellationToken);
        return await GetStoreProfileOptionsAsync(cancellationToken);
    }

    public async Task<StoreProfileOptionsResponse> DeleteStoreProfileOptionAsync(DeleteStoreProfileOptionRequest request, CancellationToken cancellationToken)
    {
        await _repository.DeleteStoreProfileOptionAsync(request.FieldName, request.Value, cancellationToken);
        return await GetStoreProfileOptionsAsync(cancellationToken);
    }

    public async Task<StoreProfileImportResponse> ImportStoreProfilesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Only .xlsx workbook uploads are supported for Store Profile maintenance.");
            }

            using var workbook = new XLWorkbook(workbookStream);
            if (!workbook.Worksheets.Any())
            {
                throw new InvalidOperationException("The uploaded store profile workbook does not contain any worksheets.");
            }

            var sheet = workbook.Worksheets.First();
            var headerMap = GetHeaderMap(sheet);
            ValidateStoreProfileHeaders(headerMap);

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet.Name);
            WriteStoreProfileImportHeader(exceptionSheet, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var storesAdded = 0;
            var storesUpdated = 0;
            var existingStores = (await _repository.GetStoresAsync(ct)).ToDictionary(
                store => (store.StoreCode ?? store.StoreLabel).Trim().ToUpperInvariant(),
                store => store);

            foreach (var row in sheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadStoreProfileImportRow(row, headerMap);
                if (!TryNormalizeStoreProfileImportRow(importRow, out var normalized, out var error))
                {
                    WriteStoreProfileExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                var normalizedStoreCode = (normalized.StoreCode ?? normalized.StoreLabel).Trim().ToUpperInvariant();
                var existing = existingStores.GetValueOrDefault(normalizedStoreCode);
                var upserted = await _repository.UpsertStoreProfileAsync(
                    1,
                    existing is null ? normalized : normalized with { StoreId = existing.StoreId },
                    ct);
                existingStores[normalizedStoreCode] = upserted;
                if (existing is null)
                {
                    storesAdded += 1;
                }
                else
                {
                    storesUpdated += 1;
                }
            }

            string? exceptionWorkbookBase64 = null;
            string? exceptionFileName = null;
            if (exceptionRowIndex > 2)
            {
                using var exceptionStream = new MemoryStream();
                exceptionWorkbook.SaveAs(exceptionStream);
                exceptionWorkbookBase64 = Convert.ToBase64String(exceptionStream.ToArray());
                exceptionFileName = $"{Path.GetFileNameWithoutExtension(fileName)}-exceptions.xlsx";
            }

            return new StoreProfileImportResponse(rowsProcessed, storesAdded, storesUpdated, "applied", exceptionFileName, exceptionWorkbookBase64);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportStoreProfilesAsync(CancellationToken cancellationToken)
    {
        var stores = await _repository.GetStoresAsync(cancellationToken);
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Store Profile");
        WriteStoreProfileImportHeader(sheet);

        var rowIndex = 2;
        foreach (var store in stores.OrderBy(item => item.StoreLabel, StringComparer.OrdinalIgnoreCase))
        {
            WriteStoreProfileRow(sheet, rowIndex++, ToStoreProfileDto(store));
        }

        sheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "store-profile-export.xlsx");
    }

    public async Task<ProductProfileResponse> GetProductProfilesAsync(string? searchTerm, int pageNumber, int pageSize, CancellationToken cancellationToken)
    {
        var (profiles, totalCount) = await _repository.GetProductProfilesAsync(searchTerm, pageNumber, pageSize, cancellationToken);
        return new ProductProfileResponse(
            profiles.Select(ToProductProfileDto).ToList(),
            totalCount,
            Math.Max(1, pageNumber),
            Math.Clamp(pageSize, 25, 500),
            string.IsNullOrWhiteSpace(searchTerm) ? null : searchTerm.Trim());
    }

    public async Task<ProductProfileDto> UpsertProductProfileAsync(UpsertProductProfileRequest request, CancellationToken cancellationToken)
    {
        var upserted = await _repository.UpsertProductProfileAsync(NormalizeProductProfile(request), cancellationToken);
        return ToProductProfileDto(upserted);
    }

    public Task DeleteProductProfileAsync(DeleteProductProfileRequest request, CancellationToken cancellationToken)
    {
        return _repository.DeleteProductProfileAsync(request.SkuVariant, cancellationToken);
    }

    public async Task<ProductProfileDto> InactivateProductProfileAsync(InactivateProductProfileRequest request, CancellationToken cancellationToken)
    {
        await _repository.InactivateProductProfileAsync(request.SkuVariant, cancellationToken);
        var existing = await LoadAllProductProfilesAsync(cancellationToken);
        var profile = existing.Single(item => string.Equals(item.SkuVariant, request.SkuVariant, StringComparison.OrdinalIgnoreCase));
        return ToProductProfileDto(profile);
    }

    public async Task<ProductProfileOptionsResponse> GetProductProfileOptionsAsync(CancellationToken cancellationToken)
    {
        return new ProductProfileOptionsResponse((await _repository.GetProductProfileOptionsAsync(cancellationToken))
            .OrderBy(option => option.FieldName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(option => option.Value, StringComparer.OrdinalIgnoreCase)
            .Select(option => new ProductProfileOptionDto(option.FieldName, option.Value, option.IsActive))
            .ToList());
    }

    public async Task<ProductProfileOptionsResponse> UpsertProductProfileOptionAsync(UpsertProductProfileOptionRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertProductProfileOptionAsync(request.FieldName, request.Value, request.IsActive, cancellationToken);
        return await GetProductProfileOptionsAsync(cancellationToken);
    }

    public async Task<ProductProfileOptionsResponse> DeleteProductProfileOptionAsync(DeleteProductProfileOptionRequest request, CancellationToken cancellationToken)
    {
        await _repository.DeleteProductProfileOptionAsync(request.FieldName, request.Value, cancellationToken);
        return await GetProductProfileOptionsAsync(cancellationToken);
    }

    public async Task<ProductHierarchyResponse> GetProductHierarchyCatalogAsync(CancellationToken cancellationToken)
    {
        var hierarchy = await _repository.GetProductHierarchyCatalogAsync(cancellationToken);
        var subclasses = await _repository.GetProductSubclassCatalogAsync(cancellationToken);
        return new ProductHierarchyResponse(
            hierarchy.OrderBy(item => item.Department, StringComparer.OrdinalIgnoreCase)
                .ThenBy(item => item.Class, StringComparer.OrdinalIgnoreCase)
                .Select(item => new ProductHierarchyCatalogDto(item.DptNo, item.ClssNo, item.Department, item.Class, item.ProdGroup, item.IsActive))
                .ToList(),
            subclasses.OrderBy(item => item.Department, StringComparer.OrdinalIgnoreCase)
                .ThenBy(item => item.Class, StringComparer.OrdinalIgnoreCase)
                .ThenBy(item => item.Subclass, StringComparer.OrdinalIgnoreCase)
                .Select(item => new ProductHierarchySubclassDto(item.Department, item.Class, item.Subclass, item.IsActive))
                .ToList());
    }

    public async Task<ProductHierarchyResponse> UpsertProductHierarchyCatalogAsync(UpsertProductHierarchyRequest request, CancellationToken cancellationToken)
    {
        await _repository.UpsertProductHierarchyCatalogAsync(
            NormalizeProductHierarchy(new ProductHierarchyCatalogRecord(request.DptNo, request.ClssNo, request.Department, request.Class, request.ProdGroup, request.IsActive)),
            cancellationToken);
        return await GetProductHierarchyCatalogAsync(cancellationToken);
    }

    public async Task<ProductHierarchyResponse> DeleteProductHierarchyCatalogAsync(DeleteProductHierarchyRequest request, CancellationToken cancellationToken)
    {
        await _repository.DeleteProductHierarchyCatalogAsync(request.DptNo, request.ClssNo, cancellationToken);
        return await GetProductHierarchyCatalogAsync(cancellationToken);
    }

    public async Task<ProductProfileImportResponse> ImportProductProfilesAsync(Stream workbookStream, string fileName, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Only .xlsx workbook uploads are supported for Product Profile maintenance.");
            }

            using var workbook = new XLWorkbook(workbookStream);
            var sheet1 = workbook.Worksheets.FirstOrDefault(worksheet => string.Equals(worksheet.Name, "Sheet1", StringComparison.OrdinalIgnoreCase))
                ?? workbook.Worksheet(1);
            var sheet2 = workbook.Worksheets.FirstOrDefault(worksheet => string.Equals(worksheet.Name, "Sheet2", StringComparison.OrdinalIgnoreCase))
                ?? (workbook.Worksheets.Count >= 2 ? workbook.Worksheet(2) : null);

            var sheet1HeaderMap = GetHeaderMap(sheet1);
            ValidateProductProfileHeaders(sheet1HeaderMap);
            if (sheet2 is null)
            {
                throw new InvalidOperationException("The uploaded product profile workbook must include Sheet2 with the Department / Class hierarchy.");
            }

            var sheet2HeaderMap = GetHeaderMap(sheet2);
            ValidateProductHierarchyHeaders(sheet2HeaderMap);

            var existingProfiles = (await LoadAllProductProfilesAsync(ct))
                .ToDictionary(profile => profile.SkuVariant, StringComparer.OrdinalIgnoreCase);

            var hierarchyRows = new Dictionary<string, ProductHierarchyCatalogRecord>(StringComparer.OrdinalIgnoreCase);
            foreach (var row in sheet2.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                var hierarchyRow = ReadProductHierarchyImportRow(row, sheet2HeaderMap);
                if (!TryNormalizeProductHierarchyImportRow(hierarchyRow, out var normalizedHierarchy, out _))
                {
                    continue;
                }

                hierarchyRows[$"{normalizedHierarchy.DptNo}|{normalizedHierarchy.ClssNo}"] = normalizedHierarchy;
            }

            using var exceptionWorkbook = new XLWorkbook();
            var exceptionSheet = exceptionWorkbook.AddWorksheet(sheet1.Name);
            WriteProductProfileImportHeader(exceptionSheet, includeRemark: true);
            var exceptionRowIndex = 2;
            var rowsProcessed = 0;
            var productsAdded = 0;
            var productsUpdated = 0;
            var profiles = new List<ProductProfileMetadata>();

            foreach (var row in sheet1.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadProductProfileImportRow(row, sheet1HeaderMap);
                if (!TryNormalizeProductProfileImportRow(importRow, out var normalizedProfile, out var error))
                {
                    WriteProductProfileExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, error);
                    continue;
                }

                if (!hierarchyRows.ContainsKey($"{normalizedProfile.DptNo}|{normalizedProfile.ClssNo}"))
                {
                    hierarchyRows[$"{normalizedProfile.DptNo}|{normalizedProfile.ClssNo}"] = new ProductHierarchyCatalogRecord(
                        normalizedProfile.DptNo,
                        normalizedProfile.ClssNo,
                        normalizedProfile.Department,
                        normalizedProfile.Class,
                        normalizedProfile.ProdGroup ?? "UNASSIGNED",
                        true);
                }

                if (existingProfiles.ContainsKey(normalizedProfile.SkuVariant))
                {
                    productsUpdated += 1;
                }
                else
                {
                    productsAdded += 1;
                }

                profiles.Add(normalizedProfile);
            }

            await _repository.ReplaceProductMasterDataAsync(
                hierarchyRows.Values.OrderBy(item => item.Department, StringComparer.OrdinalIgnoreCase).ThenBy(item => item.Class, StringComparer.OrdinalIgnoreCase).ToList(),
                profiles,
                ct);

            string? exceptionWorkbookBase64 = null;
            string? exceptionFileName = null;
            if (exceptionRowIndex > 2)
            {
                using var exceptionStream = new MemoryStream();
                exceptionWorkbook.SaveAs(exceptionStream);
                exceptionWorkbookBase64 = Convert.ToBase64String(exceptionStream.ToArray());
                exceptionFileName = $"{Path.GetFileNameWithoutExtension(fileName)}-exceptions.xlsx";
            }

            return new ProductProfileImportResponse(
                rowsProcessed,
                productsAdded,
                productsUpdated,
                hierarchyRows.Count,
                "applied",
                exceptionFileName,
                exceptionWorkbookBase64);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportProductProfilesAsync(CancellationToken cancellationToken)
    {
        var profiles = await LoadAllProductProfilesAsync(cancellationToken);
        var hierarchyRows = await _repository.GetProductHierarchyCatalogAsync(cancellationToken);
        using var workbook = new XLWorkbook();

        var profileSheet = workbook.AddWorksheet("Sheet1");
        WriteProductProfileImportHeader(profileSheet);
        var rowIndex = 2;
        foreach (var profile in profiles.OrderBy(item => item.Department, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Class, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Subclass, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.Description, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(item => item.SkuVariant, StringComparer.OrdinalIgnoreCase))
        {
            WriteProductProfileRow(profileSheet, rowIndex++, ToProductProfileDto(profile));
        }

        var hierarchySheet = workbook.AddWorksheet("Sheet2");
        WriteProductHierarchyImportHeader(hierarchySheet);
        rowIndex = 2;
        foreach (var hierarchyRow in hierarchyRows.OrderBy(item => item.Department, StringComparer.OrdinalIgnoreCase).ThenBy(item => item.Class, StringComparer.OrdinalIgnoreCase))
        {
            WriteProductHierarchyRow(hierarchySheet, rowIndex++, hierarchyRow);
        }

        profileSheet.Columns().AdjustToContents();
        hierarchySheet.Columns().AdjustToContents();
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), "product-profile-export.xlsx");
    }

    public async Task<ImportWorkbookResponse> ImportWorkbookAsync(long scenarioVersionId, Stream workbookStream, string fileName, string userId, CancellationToken cancellationToken)
    {
        return await _repository.ExecuteAtomicAsync(async ct =>
        {
            if (!fileName.EndsWith(".xlsx", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Only .xlsx workbook uploads are supported in this reference implementation.");
            }

            using var workbook = new XLWorkbook(workbookStream);
            if (!workbook.Worksheets.Any())
            {
                throw new InvalidOperationException("The uploaded workbook does not contain any worksheets.");
            }

            var metadata = await _repository.GetMetadataAsync(ct);
            var originalCells = await _repository.GetScenarioCellsAsync(scenarioVersionId, ct);
            var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
            var existingYears = metadata.TimePeriods.Values
                .Where(period => string.Equals(period.Grain, "year", StringComparison.OrdinalIgnoreCase))
                .Select(period => (int)(period.TimePeriodId / 100))
                .ToHashSet();
            var rowsProcessed = 0;
            var rowsCreated = 0;
            var touchedCoordinates = new HashSet<string>(StringComparer.Ordinal);
            var exceptionWorkbook = new XLWorkbook();
            var exceptionCountsBySheet = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            async Task RefreshScenarioStateAsync()
            {
                metadata = await _repository.GetMetadataAsync(ct);
                var refreshedCells = await _repository.GetScenarioCellsAsync(scenarioVersionId, ct);
                foreach (var cell in refreshedCells)
                {
                    if (!workingCells.ContainsKey(cell.Coordinate.Key))
                    {
                        workingCells[cell.Coordinate.Key] = cell.Clone();
                    }
                }
            }

            foreach (var worksheet in workbook.Worksheets)
            {
                var headerMap = GetHeaderMap(worksheet);
                ValidateImportHeaders(headerMap);
                var exceptionSheet = exceptionWorkbook.AddWorksheet(worksheet.Name);
                WriteImportHeader(exceptionSheet, includeRemark: true);
                var exceptionRowIndex = 2;

                foreach (var row in worksheet.RowsUsed().Skip(1))
                {
                    if (row.CellsUsed().All(cell => cell.IsEmpty()))
                    {
                        continue;
                    }

                    rowsProcessed += 1;
                    var importRow = ReadImportRow(worksheet.Name, row, headerMap);

                    if (!TryNormalizeImportRow(importRow, out var normalized, out var exceptionMessage, out var expectedValue))
                    {
                        WriteImportExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, exceptionMessage, expectedValue);
                        exceptionCountsBySheet[worksheet.Name] = exceptionRowIndex - 2;
                        continue;
                    }

                    if (!existingYears.Contains(normalized.Year))
                    {
                        await _repository.EnsureYearAsync(scenarioVersionId, normalized.Year, ct);
                        existingYears.Add(normalized.Year);
                        await RefreshScenarioStateAsync();
                    }

                    var storeNode = await EnsureNodeAsync(scenarioVersionId, "store", null, normalized.Store, ct);
                    var departmentNode = await EnsureNodeAsync(scenarioVersionId, "department", storeNode.Node.ProductNodeId, normalized.Department, ct);
                    var classNode = await EnsureNodeAsync(scenarioVersionId, "class", departmentNode.Node.ProductNodeId, normalized.Class, ct);
                    var subclassNode = await EnsureNodeAsync(scenarioVersionId, "subclass", classNode.Node.ProductNodeId, normalized.Subclass, ct);
                    var createdCount = storeNode.CreatedCount + departmentNode.CreatedCount + classNode.CreatedCount + subclassNode.CreatedCount;
                    rowsCreated += createdCount;

                    if (createdCount > 0)
                    {
                        await RefreshScenarioStateAsync();
                    }

                    var timePeriodId = BuildMonthTimePeriodId(normalized.Year, normalized.MonthIndex);
                    ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, subclassNode.Node.StoreId, subclassNode.Node.ProductNodeId, timePeriodId), normalized.SoldQty, workingCells, metadata);
                    ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, subclassNode.Node.StoreId, subclassNode.Node.ProductNodeId, timePeriodId), normalized.Asp, workingCells, metadata);
                    ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, subclassNode.Node.StoreId, subclassNode.Node.ProductNodeId, timePeriodId), normalized.UnitCost, workingCells, metadata);

                    foreach (var measureId in PlanningMeasures.SupportedMeasureIds)
                    {
                        touchedCoordinates.Add(new PlanningCellCoordinate(scenarioVersionId, measureId, subclassNode.Node.StoreId, subclassNode.Node.ProductNodeId, timePeriodId).Key);
                    }
                }
            }

            RecalculateAll(workingCells, metadata, scenarioVersionId);
            var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "import", ct);
            await AppendAuditAsync("import", "workbook", userId, $"Imported workbook {fileName}", deltas, ct);
            await AppendCommandBatchAsync(
                scenarioVersionId,
                userId,
                "import",
                new
                {
                    FileName = fileName,
                    RowsProcessed = rowsProcessed,
                    RowsCreated = rowsCreated,
                    TouchedCellCount = touchedCoordinates.Count
                },
                deltas,
                ct);

            string? exceptionWorkbookBase64 = null;
            string? exceptionFileName = null;
            if (exceptionCountsBySheet.Values.Sum() > 0)
            {
                RemoveEmptyWorksheets(exceptionWorkbook);
                using var exceptionStream = new MemoryStream();
                exceptionWorkbook.SaveAs(exceptionStream);
                exceptionWorkbookBase64 = Convert.ToBase64String(exceptionStream.ToArray());
                exceptionFileName = $"{Path.GetFileNameWithoutExtension(fileName)}-exceptions.xlsx";
            }

            return new ImportWorkbookResponse(rowsProcessed, touchedCoordinates.Count, rowsCreated, "applied", exceptionFileName, exceptionWorkbookBase64);
        }, cancellationToken);
    }

    public async Task<(byte[] Content, string FileName)> ExportWorkbookAsync(long scenarioVersionId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var cells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
        var cellLookup = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        using var workbook = new XLWorkbook();

        foreach (var storeNode in metadata.ProductNodes.Values.Where(node => node.Level == 0).OrderBy(node => node.Label, StringComparer.OrdinalIgnoreCase))
        {
            var sheet = workbook.AddWorksheet(storeNode.Label);
            WriteImportHeader(sheet);
            var rowIndex = 2;
            var subclassNodes = metadata.ProductNodes.Values
                .Where(node => node.StoreId == storeNode.StoreId && node.IsLeaf)
                .OrderBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
                .ToList();
            var monthPeriods = metadata.TimePeriods.Values
                .Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
                .OrderBy(period => period.SortOrder)
                .ToList();

            foreach (var subclassNode in subclassNodes)
            {
                var classNode = metadata.ProductNodes[subclassNode.ParentProductNodeId!.Value];
                var departmentNode = metadata.ProductNodes[classNode.ParentProductNodeId!.Value];
                foreach (var monthPeriod in monthPeriods)
                {
                    var quantity = PlanningMath.NormalizeQuantity(cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, subclassNode.StoreId, subclassNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue);
                    var asp = PlanningMath.NormalizeAsp(cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, subclassNode.StoreId, subclassNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue);
                    var unitCost = PlanningMath.NormalizeUnitCost(cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, subclassNode.StoreId, subclassNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue);
                    var revenue = PlanningMath.CalculateRevenue(quantity, asp);
                    var totalCosts = PlanningMath.CalculateTotalCosts(quantity, unitCost);
                    var grossProfit = PlanningMath.CalculateGrossProfit(quantity, asp, unitCost);
                    var grossProfitPercent = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);

                    sheet.Cell(rowIndex, 1).Value = storeNode.Label;
                    sheet.Cell(rowIndex, 2).Value = departmentNode.Label;
                    sheet.Cell(rowIndex, 3).Value = classNode.Label;
                    sheet.Cell(rowIndex, 4).Value = subclassNode.Label;
                    sheet.Cell(rowIndex, 5).Value = monthPeriod.TimePeriodId / 100;
                    sheet.Cell(rowIndex, 6).Value = monthPeriod.Label;
                    sheet.Cell(rowIndex, 7).Value = revenue;
                    sheet.Cell(rowIndex, 8).Value = quantity;
                    sheet.Cell(rowIndex, 9).Value = asp;
                    sheet.Cell(rowIndex, 10).Value = unitCost;
                    sheet.Cell(rowIndex, 11).Value = totalCosts;
                    sheet.Cell(rowIndex, 12).Value = grossProfit;
                    sheet.Cell(rowIndex, 13).Value = grossProfitPercent;
                    rowIndex += 1;
                }
            }

            sheet.Columns().AdjustToContents();
        }

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return (stream.ToArray(), $"sales-planning-export-{scenarioVersionId}.xlsx");
    }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        return _repository.ResetAsync(cancellationToken);
    }

    private static void ValidateDirectEdit(
        PlanningCellCoordinate coordinate,
        long? rowVersion,
        IReadOnlyDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata)
    {
        if (!workingCells.TryGetValue(coordinate.Key, out var sourceCell))
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} was not found.");
        }

        if (rowVersion is not null && sourceCell.RowVersion != rowVersion.Value)
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} has changed since it was last read.");
        }

        if (IsLockedBySelfOrAncestor(coordinate, workingCells.Values, metadata))
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} is locked and cannot be changed.");
        }

        var definition = PlanningMeasures.GetDefinition(coordinate.MeasureId);
        var isLeafWrite = IsLeafWriteCoordinate(coordinate, metadata);
        if (isLeafWrite && !definition.EditableAtLeaf)
        {
            throw new InvalidOperationException($"{definition.Label} cannot be edited at leaf level.");
        }

        if (!isLeafWrite && !definition.EditableAtAggregate)
        {
            throw new InvalidOperationException($"{definition.Label} cannot be edited at aggregate level.");
        }
    }

    private static void ApplyLeafMeasureEdit(
        PlanningCellCoordinate coordinate,
        decimal newValue,
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata)
    {
        if (!IsLeafWriteCoordinate(coordinate, metadata))
        {
            throw new InvalidOperationException($"Cell {coordinate.Key} is an aggregate intersection and requires top-down override or splash.");
        }

        var revenueCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.SalesRevenue, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var quantityCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.SoldQuantity, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var aspCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.AverageSellingPrice, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var unitCostCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.UnitCost, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var totalCostsCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.TotalCosts, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var grossProfitCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.GrossProfit, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];
        var grossProfitPercentCell = workingCells[new PlanningCellCoordinate(coordinate.ScenarioVersionId, PlanningMeasures.GrossProfitPercent, coordinate.StoreId, coordinate.ProductNodeId, coordinate.TimePeriodId).Key];

        var quantity = PlanningMath.NormalizeQuantity(quantityCell.InputValue ?? quantityCell.EffectiveValue);
        var asp = PlanningMath.NormalizeAsp(aspCell.InputValue ?? aspCell.EffectiveValue);
        var unitCost = PlanningMath.NormalizeUnitCost(unitCostCell.InputValue ?? unitCostCell.EffectiveValue);

        switch (coordinate.MeasureId)
        {
            case PlanningMeasures.SalesRevenue:
                asp = PlanningMath.NormalizeAsp(aspCell.InputValue ?? aspCell.EffectiveValue);
                quantity = PlanningMath.DeriveQuantityFromRevenue(newValue, asp);
                break;
            case PlanningMeasures.SoldQuantity:
                quantity = PlanningMath.NormalizeQuantity(newValue);
                asp = PlanningMath.NormalizeAsp(aspCell.InputValue ?? aspCell.EffectiveValue);
                break;
            case PlanningMeasures.AverageSellingPrice:
                quantity = PlanningMath.NormalizeQuantity(quantityCell.InputValue ?? quantityCell.EffectiveValue);
                asp = PlanningMath.NormalizeAsp(newValue);
                break;
            case PlanningMeasures.UnitCost:
                unitCost = PlanningMath.NormalizeUnitCost(newValue);
                break;
            case PlanningMeasures.GrossProfitPercent:
                asp = PlanningMath.DeriveAspFromGrossProfitPercent(unitCost, newValue);
                break;
            default:
                throw new InvalidOperationException($"Measure {coordinate.MeasureId} is not supported.");
        }

        var revenue = PlanningMath.CalculateRevenue(quantity, asp);
        var totalCosts = PlanningMath.CalculateTotalCosts(quantity, unitCost);
        var grossProfit = PlanningMath.CalculateGrossProfit(quantity, asp, unitCost);
        var grossProfitPercentValue = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);
        SetLeafValue(quantityCell, quantity);
        SetLeafValue(aspCell, asp);
        SetLeafValue(unitCostCell, unitCost);
        SetLeafValue(revenueCell, revenue);
        SetCalculatedLeafValue(totalCostsCell, totalCosts);
        SetCalculatedLeafValue(grossProfitCell, grossProfit);
        SetCalculatedLeafValue(grossProfitPercentCell, grossProfitPercentValue);
    }

    private void ApplyAggregateAllocation(
        long scenarioVersionId,
        long measureId,
        long sourceTimePeriodId,
        IReadOnlyList<SplashScopeRootDto> scopeRoots,
        decimal totalValue,
        string method,
        Dictionary<long, decimal>? manualWeights,
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata)
    {
        var definition = PlanningMeasures.GetDefinition(measureId);
        if (!definition.EditableAtAggregate)
        {
            throw new InvalidOperationException($"{definition.Label} cannot be edited at aggregate level.");
        }

        var targetTimeIds = GetLeafTimeIds(sourceTimePeriodId, metadata);
        var targetCells = scopeRoots
            .SelectMany(root => GetLeafProductIds(root.ProductNodeId, metadata)
                .SelectMany(productId => targetTimeIds.Select(timeId => workingCells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    root.StoreId,
                    productId,
                    timeId).Key])))
            .DistinctBy(cell => cell.Coordinate.Key)
            .ToList();

        if (targetCells.Count == 0)
        {
            throw new InvalidOperationException("No leaf targets were found for this splash.");
        }

        var weights = BuildWeights(sourceTimePeriodId, measureId, targetCells, method, manualWeights, metadata);
        var splashTargets = targetCells
            .Select(cell =>
            {
                var clone = cell.Clone();
                clone.IsLocked = IsLockedBySelfOrAncestor(clone.Coordinate, workingCells.Values, metadata);
                return new SplashTarget(clone, weights[clone.Coordinate.Key]);
            })
            .ToList();

        var roundingScale = measureId is PlanningMeasures.AverageSellingPrice or PlanningMeasures.UnitCost ? 2 :
            measureId == PlanningMeasures.GrossProfitPercent ? 1 : 0;
        var allocations = _splashAllocator.Allocate(totalValue, splashTargets, roundingScale);
        foreach (var allocation in allocations)
        {
            ApplyLeafMeasureEdit(allocation.Cell.Coordinate, allocation.NewValue, workingCells, metadata);
        }
    }

    private Dictionary<string, decimal> BuildWeights(
        long sourceTimePeriodId,
        long measureId,
        IReadOnlyList<PlanningCell> targetCells,
        string method,
        Dictionary<long, decimal>? manualWeights,
        PlanningMetadataSnapshot metadata)
    {
        var normalizedMethod = method.Trim().ToLowerInvariant();
        if (normalizedMethod == "equal_distribution")
        {
            return targetCells.ToDictionary(cell => cell.Coordinate.Key, _ => 1m);
        }

        if (normalizedMethod is "seasonality_profile" or "manual_weights")
        {
            if (manualWeights is null || manualWeights.Count == 0)
            {
                throw new InvalidOperationException("Manual or seasonality weights are required for this splash.");
            }

            var timeWeights = GetLeafTimeIds(sourceTimePeriodId, metadata)
                .ToDictionary(timeId => timeId, timeId =>
                {
                    if (!manualWeights.TryGetValue(timeId, out var weight))
                    {
                        throw new InvalidOperationException($"A weight was not provided for time period {timeId}.");
                    }

                    if (weight < 0)
                    {
                        throw new InvalidOperationException("Negative splash weights are not supported.");
                    }

                    return weight;
                });

            var productWeights = BuildProductWeights(measureId, targetCells);
            return targetCells.ToDictionary(
                cell => cell.Coordinate.Key,
                cell => timeWeights[cell.Coordinate.TimePeriodId] * productWeights[cell.Coordinate.ProductNodeId]);
        }

        var existingPlanWeights = targetCells.ToDictionary(
            cell => cell.Coordinate.Key,
            cell => Math.Max(cell.EffectiveValue, 0m));

        return existingPlanWeights.Values.Sum() > 0
            ? existingPlanWeights
            : targetCells.ToDictionary(cell => cell.Coordinate.Key, _ => 1m);
    }

    private static Dictionary<long, decimal> BuildProductWeights(long measureId, IReadOnlyList<PlanningCell> targetCells)
    {
        var productWeights = targetCells
            .GroupBy(cell => cell.Coordinate.ProductNodeId)
            .ToDictionary(group => group.Key, group => Math.Max(group.Sum(cell => cell.EffectiveValue), 0m));

        if (productWeights.Values.Sum() > 0)
        {
            return productWeights;
        }

        return productWeights.Keys.ToDictionary(key => key, _ => measureId == PlanningMeasures.AverageSellingPrice ? 1m : 1m);
    }

    private static Dictionary<long, decimal> BuildDefaultSeasonalityWeights(long yearTimePeriodId, PlanningMetadataSnapshot metadata)
    {
        var monthWeights = new decimal[] { 8m, 12m, 7m, 7m, 8m, 8m, 9m, 9m, 8m, 7m, 8m, 9m };
        var monthPeriods = metadata.TimePeriods.Values
            .Where(period => period.ParentTimePeriodId == yearTimePeriodId)
            .OrderBy(period => period.SortOrder)
            .ToList();

        return monthPeriods
            .Select((period, index) => new { period.TimePeriodId, Weight = index < monthWeights.Length ? monthWeights[index] : 1m })
            .ToDictionary(item => item.TimePeriodId, item => item.Weight);
    }

    private static void RecalculateAll(
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId)
    {
        var leafMonths = metadata.ProductNodes.Values
            .Where(node => node.IsLeaf)
            .SelectMany(node => metadata.TimePeriods.Values
                .Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
                .Select(period => (node.StoreId, node.ProductNodeId, period.TimePeriodId)))
            .ToList();

        foreach (var key in leafMonths)
        {
            var quantityCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var aspCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var revenueCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var unitCostCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var totalCostsCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var grossProfitCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfit, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];
            var grossProfitPercentCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfitPercent, key.StoreId, key.ProductNodeId, key.TimePeriodId).Key];

            var quantity = PlanningMath.NormalizeQuantity(quantityCell.InputValue ?? quantityCell.EffectiveValue);
            var asp = PlanningMath.NormalizeAsp(aspCell.InputValue ?? aspCell.EffectiveValue);
            var unitCost = PlanningMath.NormalizeUnitCost(unitCostCell.InputValue ?? unitCostCell.EffectiveValue);
            var revenue = PlanningMath.CalculateRevenue(quantity, asp);
            var totalCosts = PlanningMath.CalculateTotalCosts(quantity, unitCost);
            var grossProfit = PlanningMath.CalculateGrossProfit(quantity, asp, unitCost);
            var grossProfitPercent = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);

            SetLeafValue(quantityCell, quantity);
            SetLeafValue(aspCell, asp);
            SetLeafValue(unitCostCell, unitCost);
            SetLeafValue(revenueCell, revenue);
            SetCalculatedLeafValue(totalCostsCell, totalCosts);
            SetCalculatedLeafValue(grossProfitCell, grossProfit);
            SetCalculatedLeafValue(grossProfitPercentCell, grossProfitPercent);
        }

        RecalculateMeasureTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.SalesRevenue);
        RecalculateMeasureTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.SoldQuantity);
        RecalculateMeasureTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.TotalCosts);
        RecalculateMeasureTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.GrossProfit);
        RecalculateDerivedRateTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.AverageSellingPrice);
        RecalculateDerivedRateTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.UnitCost);
        RecalculateDerivedRateTotals(workingCells, metadata, scenarioVersionId, PlanningMeasures.GrossProfitPercent);
    }

    private static void ResetGrowthFactors(IEnumerable<PlanningCell> cells, long measureId)
    {
        foreach (var cell in cells.Where(cell => cell.Coordinate.MeasureId == measureId))
        {
            cell.GrowthFactor = 1.0m;
        }
    }

    private static void RecalculateMeasureTotals(
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        long measureId)
    {
        var timeDepth = metadata.TimePeriods.Values.ToDictionary(period => period.TimePeriodId, period => GetTimeDepth(period.TimePeriodId, metadata));
        var aggregateTimes = metadata.TimePeriods.Values
            .Where(period => metadata.TimePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId))
            .OrderByDescending(period => timeDepth[period.TimePeriodId])
            .ThenBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in metadata.ProductNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var aggregateTime in aggregateTimes)
            {
                var childTimeIds = metadata.TimePeriods.Values
                    .Where(period => period.ParentTimePeriodId == aggregateTime.TimePeriodId)
                    .Select(period => period.TimePeriodId)
                    .ToList();
                var cell = workingCells[new PlanningCellCoordinate(scenarioVersionId, measureId, leafNode.StoreId, leafNode.ProductNodeId, aggregateTime.TimePeriodId).Key];
                var value = childTimeIds.Sum(childTimeId => workingCells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    leafNode.StoreId,
                    leafNode.ProductNodeId,
                    childTimeId).Key].EffectiveValue);
                SetAggregateValue(cell, value);
            }
        }

        foreach (var productNode in metadata.ProductNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            var childProductIds = metadata.ProductNodes.Values
                .Where(node => node.ParentProductNodeId == productNode.ProductNodeId)
                .Select(node => node.ProductNodeId)
                .ToList();

            foreach (var timePeriod in metadata.TimePeriods.Values)
            {
                var cell = workingCells[new PlanningCellCoordinate(scenarioVersionId, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key];
                var value = childProductIds.Sum(childProductId => workingCells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    productNode.StoreId,
                    childProductId,
                    timePeriod.TimePeriodId).Key].EffectiveValue);
                SetAggregateValue(cell, value);
            }
        }
    }

    private static void RecalculateDerivedRateTotals(
        IDictionary<string, PlanningCell> workingCells,
        PlanningMetadataSnapshot metadata,
        long scenarioVersionId,
        long measureId)
    {
        foreach (var productNode in metadata.ProductNodes.Values)
        {
            foreach (var timePeriod in metadata.TimePeriods.Values)
            {
                var rateCell = workingCells[new PlanningCellCoordinate(scenarioVersionId, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key];
                var quantity = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;
                var revenue = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;
                var totalCosts = workingCells[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId).Key].EffectiveValue;

                var value = measureId switch
                {
                    PlanningMeasures.AverageSellingPrice => quantity > 0m ? PlanningMath.NormalizeAsp(revenue / quantity) : 1.00m,
                    PlanningMeasures.UnitCost => quantity > 0m ? PlanningMath.NormalizeUnitCost(totalCosts / quantity) : 0m,
                    PlanningMeasures.GrossProfitPercent => PlanningMath.CalculateGrossProfitPercent(
                        quantity > 0m ? revenue / quantity : 1.00m,
                        quantity > 0m ? totalCosts / quantity : 0m),
                    _ => 0m
                };

                if (productNode.IsLeaf && string.Equals(timePeriod.Grain, "month", StringComparison.OrdinalIgnoreCase))
                {
                    if (measureId is PlanningMeasures.AverageSellingPrice or PlanningMeasures.UnitCost)
                    {
                        SetLeafValue(rateCell, value);
                    }
                    else
                    {
                        SetCalculatedLeafValue(rateCell, value);
                    }
                }
                else
                {
                    SetAggregateValue(rateCell, value);
                }
            }
        }
    }

    private static void SetLeafValue(PlanningCell cell, decimal value)
    {
        cell.InputValue = value;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.CellKind = "input";
    }

    private static void SetCalculatedLeafValue(PlanningCell cell, decimal value)
    {
        cell.InputValue = null;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.CellKind = "calculated";
    }

    private static void SetAggregateValue(PlanningCell cell, decimal value)
    {
        cell.InputValue = null;
        cell.OverrideValue = null;
        cell.IsSystemGeneratedOverride = false;
        cell.DerivedValue = value;
        cell.EffectiveValue = value;
        cell.CellKind = "calculated";
    }

    private async Task<IReadOnlyList<AuditTrailItemDto>> GetAuditInternalAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        var audits = await _repository.GetAuditAsync(scenarioVersionId, measureId, storeId, productNodeId, cancellationToken);
        return audits.Select(audit => new AuditTrailItemDto(
                audit.ActionId,
                audit.ActionType,
                audit.Method,
                audit.UserId,
                audit.Comment,
                audit.CreatedAt,
                audit.Deltas.Select(delta => new AuditCellDeltaDto(
                    delta.Coordinate.StoreId,
                    delta.Coordinate.ProductNodeId,
                    delta.Coordinate.TimePeriodId,
                    delta.OldValue,
                    delta.NewValue,
                    delta.ChangeKind)).ToList()))
            .ToList();
    }

    private async Task<IReadOnlyList<PlanningCommandCellDelta>> PersistScenarioChangesAsync(
        IReadOnlyList<PlanningCell> originalCells,
        IEnumerable<PlanningCell> workingCells,
        string changeKind,
        CancellationToken cancellationToken)
    {
        var originalByKey = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var changedCells = new List<PlanningCell>();
        var deltas = new List<PlanningCommandCellDelta>();

        foreach (var cell in workingCells)
        {
            if (!originalByKey.TryGetValue(cell.Coordinate.Key, out var original))
            {
                changedCells.Add(cell);
                continue;
            }

            if (!HasMaterialChange(original, cell))
            {
                continue;
            }

            var updated = cell.Clone();
            updated.RowVersion = original.RowVersion + 1;
            changedCells.Add(updated);
            deltas.Add(new PlanningCommandCellDelta(
                updated.Coordinate,
                PlanningCellState.FromCell(original),
                PlanningCellState.FromCell(updated),
                changeKind));
        }

        await _repository.UpsertCellsAsync(changedCells, cancellationToken);
        return deltas;
    }

    private async Task<long> AppendAuditAsync(
        string actionType,
        string method,
        string userId,
        string? comment,
        IReadOnlyList<PlanningCommandCellDelta> deltas,
        CancellationToken cancellationToken)
    {
        var audit = new PlanningActionAudit(
            await _repository.GetNextActionIdAsync(cancellationToken),
            actionType,
            method,
            userId,
            comment,
            DateTimeOffset.UtcNow,
            BuildAuditDeltas(deltas));

        await _repository.AppendAuditAsync(audit, cancellationToken);
        return audit.ActionId;
    }

    private async Task<(ProductNode Node, int CreatedCount)> EnsureNodeAsync(
        long scenarioVersionId,
        string level,
        long? parentProductNodeId,
        string label,
        CancellationToken cancellationToken)
    {
        ProductNode? existingNode;
        if (string.Equals(level, "store", StringComparison.OrdinalIgnoreCase))
        {
            existingNode = await _repository.FindProductNodeByPathAsync([label], cancellationToken);
        }
        else
        {
            var metadata = await _repository.GetMetadataAsync(cancellationToken);
            var parent = metadata.ProductNodes[parentProductNodeId!.Value];
            existingNode = await _repository.FindProductNodeByPathAsync(parent.Path.Append(label).ToArray(), cancellationToken);
        }

        if (existingNode is not null)
        {
            return (existingNode, 0);
        }

        var createdNode = await _repository.AddRowAsync(new AddRowRequest(scenarioVersionId, level, parentProductNodeId, label, null), cancellationToken);
        return (createdNode, 1);
    }

    private async Task<HierarchyMappingResponse> BuildHierarchyMappingResponseAsync(CancellationToken cancellationToken)
    {
        var mappings = await _repository.GetHierarchyMappingsAsync(cancellationToken);
        return new HierarchyMappingResponse(
            mappings
                .OrderBy(entry => entry.DepartmentLabel, StringComparer.OrdinalIgnoreCase)
                .Select(entry => new HierarchyDepartmentDto(
                    entry.DepartmentLabel,
                    entry.LifecycleState,
                    entry.RampProfileCode,
                    entry.EffectiveFromTimePeriodId,
                    entry.EffectiveToTimePeriodId,
                    entry.Classes
                        .OrderBy(value => value.ClassLabel, StringComparer.OrdinalIgnoreCase)
                        .Select(classEntry => new HierarchyClassDto(
                            classEntry.ClassLabel,
                            classEntry.LifecycleState,
                            classEntry.RampProfileCode,
                            classEntry.EffectiveFromTimePeriodId,
                            classEntry.EffectiveToTimePeriodId,
                            classEntry.Subclasses
                                .OrderBy(value => value.SubclassLabel, StringComparer.OrdinalIgnoreCase)
                                .Select(subclassEntry => new HierarchySubclassDto(
                                    subclassEntry.SubclassLabel,
                                    subclassEntry.LifecycleState,
                                    subclassEntry.RampProfileCode,
                                    subclassEntry.EffectiveFromTimePeriodId,
                                    subclassEntry.EffectiveToTimePeriodId))
                                .ToList()))
                        .ToList()))
                .ToList());
    }

    private static bool HasMaterialChange(PlanningCell left, PlanningCell right)
    {
        return left.InputValue != right.InputValue ||
               left.OverrideValue != right.OverrideValue ||
               left.IsSystemGeneratedOverride != right.IsSystemGeneratedOverride ||
               left.DerivedValue != right.DerivedValue ||
               left.EffectiveValue != right.EffectiveValue ||
               left.GrowthFactor != right.GrowthFactor ||
               left.IsLocked != right.IsLocked ||
               left.LockReason != right.LockReason ||
               left.LockedBy != right.LockedBy ||
               left.CellKind != right.CellKind;
    }

    private static bool IsLockedBySelfOrAncestor(
        PlanningCellCoordinate coordinate,
        IEnumerable<PlanningCell> scenarioCells,
        PlanningMetadataSnapshot metadata)
    {
        return scenarioCells.Any(cell =>
            cell.Coordinate.MeasureId == coordinate.MeasureId &&
            cell.IsLocked &&
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsDescendantProduct(coordinate.ProductNodeId, cell.Coordinate.ProductNodeId, metadata) &&
            IsDescendantTime(coordinate.TimePeriodId, cell.Coordinate.TimePeriodId, metadata));
    }

    private static bool IsLeafWriteCoordinate(PlanningCellCoordinate coordinate, PlanningMetadataSnapshot metadata)
    {
        return metadata.ProductNodes[coordinate.ProductNodeId].IsLeaf &&
               !metadata.TimePeriods.Values.Any(period => period.ParentTimePeriodId == coordinate.TimePeriodId);
    }

    private static IReadOnlyList<long> GetLeafProductIds(long productNodeId, PlanningMetadataSnapshot metadata)
    {
        var descendants = metadata.ProductNodes.Values
            .Where(node => IsDescendantProduct(node.ProductNodeId, productNodeId, metadata))
            .Where(node => node.IsLeaf)
            .Select(node => node.ProductNodeId)
            .ToList();

        return descendants.Count == 0 ? [productNodeId] : descendants;
    }

    private static IReadOnlyList<long> GetLeafTimeIds(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        var children = metadata.TimePeriods.Values
            .Where(node => node.ParentTimePeriodId == timePeriodId)
            .Select(node => node.TimePeriodId)
            .ToList();

        if (children.Count == 0)
        {
            return [timePeriodId];
        }

        return children
            .SelectMany(childId => GetLeafTimeIds(childId, metadata))
            .Distinct()
            .OrderBy(id => metadata.TimePeriods[id].SortOrder)
            .ToList();
    }

    private static bool IsDescendantProduct(long candidateId, long ancestorId, PlanningMetadataSnapshot metadata)
    {
        var current = candidateId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = metadata.ProductNodes[current];
            if (node.ParentProductNodeId is null)
            {
                return false;
            }

            current = node.ParentProductNodeId.Value;
        }
    }

    private static bool IsDescendantTime(long candidateId, long ancestorId, PlanningMetadataSnapshot metadata)
    {
        var current = candidateId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = metadata.TimePeriods[current];
            if (node.ParentTimePeriodId is null)
            {
                return false;
            }

            current = node.ParentTimePeriodId.Value;
        }
    }

    private static int GetTimeDepth(long timePeriodId, PlanningMetadataSnapshot metadata)
    {
        var depth = 0;
        var current = metadata.TimePeriods[timePeriodId];
        while (current.ParentTimePeriodId is not null)
        {
            depth += 1;
            current = metadata.TimePeriods[current.ParentTimePeriodId.Value];
        }

        return depth;
    }

    private static void ValidateImportHeaders(IReadOnlyDictionary<string, int> headerMap)
    {
        foreach (var requiredHeader in ImportHeaders)
        {
            if (!headerMap.ContainsKey(requiredHeader))
            {
                throw new InvalidOperationException($"The workbook is missing the required '{requiredHeader}' column.");
            }
        }
    }

    private static Dictionary<string, int> GetHeaderMap(IXLWorksheet worksheet)
    {
        return worksheet.Row(1)
            .CellsUsed()
            .ToDictionary(
                cell => cell.GetString().Trim(),
                cell => cell.Address.ColumnNumber,
                StringComparer.OrdinalIgnoreCase);
    }

    private static ImportedPlanRow ReadImportRow(string sheetName, IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        return new ImportedPlanRow(
            sheetName.Trim(),
            row.Cell(headerMap["Store"]).GetString().Trim(),
            row.Cell(headerMap["Department"]).GetString().Trim(),
            row.Cell(headerMap["Class"]).GetString().Trim(),
            row.Cell(headerMap["Subclass"]).GetString().Trim(),
            row.Cell(headerMap["Year"]).GetString().Trim(),
            row.Cell(headerMap["Month"]).GetString().Trim(),
            row.Cell(headerMap["Sales Revenue"]).GetString().Trim(),
            row.Cell(headerMap["Sold Qty"]).GetString().Trim(),
            row.Cell(headerMap["ASP"]).GetString().Trim(),
            row.Cell(headerMap["Unit Cost"]).GetString().Trim(),
            row.Cell(headerMap["Total Costs"]).GetString().Trim(),
            row.Cell(headerMap["GP"]).GetString().Trim(),
            row.Cell(headerMap["GP%"]).GetString().Trim(),
            headerMap.TryGetValue(RemarkHeader, out var remarkColumn) ? row.Cell(remarkColumn).GetString().Trim() : string.Empty,
            headerMap.TryGetValue(ExpectedValueHeader, out var expectedValueColumn) ? row.Cell(expectedValueColumn).GetString().Trim() : string.Empty);
    }

    private static bool TryNormalizeImportRow(ImportedPlanRow row, out NormalizedImportRow normalized, out string exceptionMessage, out string expectedValue)
    {
        normalized = default;
        exceptionMessage = string.Empty;
        expectedValue = string.Empty;

        var store = string.IsNullOrWhiteSpace(row.Store) ? row.SheetName : row.Store.Trim();
        if (!string.Equals(store, row.SheetName, StringComparison.OrdinalIgnoreCase))
        {
            exceptionMessage = "Store value does not match worksheet name.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(row.Department) || string.IsNullOrWhiteSpace(row.Class) || string.IsNullOrWhiteSpace(row.Subclass))
        {
            exceptionMessage = "Department, Class, and Subclass are required.";
            return false;
        }

        if (!int.TryParse(row.Year, NumberStyles.Integer, CultureInfo.InvariantCulture, out var year))
        {
            exceptionMessage = "Year must be numeric.";
            return false;
        }

        var monthIndex = Array.FindIndex(MonthLabels, month => string.Equals(month, row.Month, StringComparison.OrdinalIgnoreCase));
        if (monthIndex < 0)
        {
            exceptionMessage = "Month must be a 3-letter abbreviation.";
            return false;
        }

        var hasRevenue = TryParseDecimal(row.SalesRevenue, out var revenue);
        var hasQty = TryParseDecimal(row.SoldQty, out var quantity);
        var hasAsp = TryParseDecimal(row.Asp, out var asp);
        var hasUnitCost = TryParseDecimal(row.UnitCost, out var unitCost);
        var hasTotalCosts = TryParseDecimal(row.TotalCosts, out var totalCosts);
        var hasGrossProfit = TryParseDecimal(row.Gp, out var grossProfit);
        var hasGrossProfitPercent = TryParseDecimal(row.GpPercent.Replace("%", string.Empty, StringComparison.Ordinal), out var grossProfitPercent);

        if (hasQty)
        {
            quantity = PlanningMath.NormalizeQuantity(quantity);
        }

        if (!hasAsp && hasQty && hasRevenue)
        {
            asp = PlanningMath.DeriveAspFromRevenue(revenue, quantity);
            hasAsp = true;
        }

        asp = hasAsp ? PlanningMath.NormalizeAsp(asp) : 1.00m;

        if (!hasQty && hasRevenue)
        {
            quantity = PlanningMath.DeriveQuantityFromRevenue(revenue, asp);
            hasQty = true;
        }

        if (!hasRevenue && hasQty)
        {
            revenue = PlanningMath.CalculateRevenue(quantity, asp);
            hasRevenue = true;
        }

        if (!hasRevenue && !hasQty)
        {
            exceptionMessage = "At least Sales Revenue or Sold Qty must be provided.";
            return false;
        }

        if (!hasUnitCost && hasTotalCosts && hasQty)
        {
            unitCost = PlanningMath.DeriveUnitCostFromTotalCosts(totalCosts, quantity);
            hasUnitCost = true;
        }

        unitCost = hasUnitCost ? PlanningMath.NormalizeUnitCost(unitCost) : 0m;
        var normalizedRevenue = PlanningMath.CalculateRevenue(quantity, asp);
        var normalizedTotalCosts = PlanningMath.CalculateTotalCosts(quantity, unitCost);
        var normalizedGrossProfit = PlanningMath.CalculateGrossProfit(quantity, asp, unitCost);
        var normalizedGrossProfitPercent = PlanningMath.CalculateGrossProfitPercent(asp, unitCost);

        if (hasRevenue && normalizedRevenue != PlanningMath.NormalizeRevenue(revenue))
        {
            exceptionMessage = "Sales Revenue does not equal Sold Qty * ASP after normalization.";
            expectedValue = FormatWholeNumber(normalizedRevenue);
            return false;
        }

        if (hasTotalCosts && normalizedTotalCosts != PlanningMath.NormalizeTotalCosts(totalCosts))
        {
            exceptionMessage = "Total Costs does not equal Unit Cost * Sold Qty after normalization.";
            expectedValue = FormatWholeNumber(normalizedTotalCosts);
            return false;
        }

        if (hasGrossProfit && normalizedGrossProfit != PlanningMath.NormalizeGrossProfit(grossProfit))
        {
            exceptionMessage = "GP does not equal (ASP - Unit Cost) * Sold Qty after normalization.";
            expectedValue = FormatWholeNumber(normalizedGrossProfit);
            return false;
        }

        if (hasGrossProfitPercent && normalizedGrossProfitPercent != PlanningMath.NormalizeGrossProfitPercent(grossProfitPercent))
        {
            exceptionMessage = "GP% does not equal (ASP - Unit Cost) / ASP * 100 after normalization.";
            expectedValue = FormatPercent(normalizedGrossProfitPercent);
            return false;
        }

        normalized = new NormalizedImportRow(
            store,
            row.Department.Trim(),
            row.Class.Trim(),
            row.Subclass.Trim(),
            year,
            monthIndex + 1,
            normalizedRevenue,
            quantity,
            asp,
            unitCost,
            normalizedTotalCosts,
            normalizedGrossProfit,
            normalizedGrossProfitPercent);
        return true;
    }

    private static void WriteImportHeader(IXLWorksheet sheet, bool includeRemark = false)
    {
        var headers = includeRemark ? [..ImportHeaders, RemarkHeader, ExpectedValueHeader] : ImportHeaders;
        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteImportExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedPlanRow row, string exceptionMessage, string expectedValue)
    {
        sheet.Cell(rowIndex, 1).Value = row.Store;
        sheet.Cell(rowIndex, 2).Value = row.Department;
        sheet.Cell(rowIndex, 3).Value = row.Class;
        sheet.Cell(rowIndex, 4).Value = row.Subclass;
        sheet.Cell(rowIndex, 5).Value = row.Year;
        sheet.Cell(rowIndex, 6).Value = row.Month;
        sheet.Cell(rowIndex, 7).Value = row.SalesRevenue;
        sheet.Cell(rowIndex, 8).Value = row.SoldQty;
        sheet.Cell(rowIndex, 9).Value = row.Asp;
        sheet.Cell(rowIndex, 10).Value = row.UnitCost;
        sheet.Cell(rowIndex, 11).Value = row.TotalCosts;
        sheet.Cell(rowIndex, 12).Value = row.Gp;
        sheet.Cell(rowIndex, 13).Value = row.GpPercent;
        sheet.Cell(rowIndex, 14).Value = exceptionMessage;
        sheet.Cell(rowIndex, 15).Value = expectedValue;
        sheet.Row(rowIndex).Style.Fill.BackgroundColor = XLColor.LightPink;
    }

    private static string FormatWholeNumber(decimal value)
    {
        return value.ToString("0", CultureInfo.InvariantCulture);
    }

    private static string FormatPercent(decimal value)
    {
        return value.ToString("0.0", CultureInfo.InvariantCulture) + "%";
    }

    private static void RemoveEmptyWorksheets(XLWorkbook workbook)
    {
        var emptySheets = workbook.Worksheets.Where(sheet => sheet.RowsUsed().Count() <= 1).ToList();
        foreach (var sheet in emptySheets)
        {
            workbook.Worksheets.Delete(sheet.Name);
        }
    }

    private static long BuildMonthTimePeriodId(int fiscalYear, int monthNumber)
    {
        return (fiscalYear * 100L) + monthNumber;
    }

    private static bool TryParseDecimal(string rawValue, out decimal value)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            value = 0m;
            return false;
        }

        return decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }

    private static StoreProfileDto ToStoreProfileDto(StoreNodeMetadata store) => new(
        store.StoreId,
        store.StoreCode ?? $"STORE-{store.StoreId}",
        store.StoreLabel,
        store.State,
        store.ClusterLabel,
        store.Latitude,
        store.Longitude,
        store.RegionLabel,
        store.OpeningDate,
        store.Sssg,
        store.SalesType,
        store.Status,
        store.Storey,
        store.BuildingStatus,
        store.Gta,
        store.Nta,
        store.Rsom,
        store.Dm,
        store.Rental,
        store.LifecycleState,
        store.RampProfileCode,
        store.IsActive,
        store.StoreClusterRole,
        store.StoreCapacitySqFt,
        store.StoreFormatTier,
        store.CatchmentType,
        store.DemographicSegment,
        store.ClimateZone,
        store.FulfilmentEnabled,
        store.OnlineFulfilmentNode,
        store.StoreOpeningSeason,
        store.StoreClosureDate,
        store.RefurbishmentDate,
        store.StorePriority);

    private static StoreNodeMetadata NormalizeStoreProfile(UpsertStoreProfileRequest request) => new(
        request.StoreId ?? 0,
        NormalizeRequiredText(request.BranchName, request.StoreCode, "BranchName"),
        NormalizeRequiredText(request.ClusterLabel, null, "Branch Type"),
        NormalizeRequiredText(request.RegionLabel, null, "Region"),
        NormalizeOptionalText(request.LifecycleState) ?? "active",
        NormalizeOptionalText(request.RampProfileCode),
        null,
        null,
        NormalizeRequiredText(request.StoreCode, request.BranchName, "CompCode"),
        NormalizeOptionalText(request.State),
        request.Latitude,
        request.Longitude,
        NormalizeOptionalDate(request.OpeningDate),
        NormalizeOptionalText(request.Sssg),
        NormalizeOptionalText(request.SalesType),
        NormalizeOptionalText(request.Status),
        NormalizeOptionalText(request.Storey),
        NormalizeOptionalText(request.BuildingStatus),
        request.Gta,
        request.Nta,
        NormalizeOptionalText(request.Rsom),
        NormalizeOptionalText(request.Dm),
        request.Rental,
        request.IsActive,
        NormalizeOptionalText(request.StoreClusterRole),
        request.StoreCapacitySqFt,
        NormalizeOptionalText(request.StoreFormatTier),
        NormalizeOptionalText(request.CatchmentType),
        NormalizeOptionalText(request.DemographicSegment),
        NormalizeOptionalText(request.ClimateZone),
        request.FulfilmentEnabled,
        request.OnlineFulfilmentNode,
        NormalizeOptionalText(request.StoreOpeningSeason),
        NormalizeOptionalDate(request.StoreClosureDate),
        NormalizeOptionalDate(request.RefurbishmentDate),
        NormalizeOptionalText(request.StorePriority));

    private static string NormalizeRequiredText(string? preferred, string? fallback, string label)
    {
        var resolved = NormalizeOptionalText(preferred) ?? NormalizeOptionalText(fallback);
        if (resolved is null)
        {
            throw new InvalidOperationException($"{label} is required.");
        }

        return resolved;
    }

    private static string? NormalizeOptionalText(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static string? NormalizeOptionalDate(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
        {
            return parsed.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        return value.Trim();
    }

    private static void ValidateStoreProfileHeaders(IReadOnlyDictionary<string, int> headerMap)
    {
        foreach (var header in StoreProfileImportHeaders.Take(18))
        {
            if (!headerMap.ContainsKey(header))
            {
                throw new InvalidOperationException($"The store profile workbook is missing the required '{header}' column.");
            }
        }
    }

    private static ImportedStoreProfileRow ReadStoreProfileImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;

        return new ImportedStoreProfileRow(
            GetValue("CompCode"),
            GetValue("BranchName"),
            GetValue("State"),
            GetValue("Branch Type"),
            GetValue("Latitude"),
            GetValue("Longitude"),
            GetValue("Region"),
            GetValue("Opening Date"),
            GetValue("SSSG"),
            GetValue("Sales Type"),
            GetValue("Status"),
            GetValue("Storey"),
            GetValue("Building Status"),
            GetValue("GTA"),
            GetValue("NTA"),
            GetValue("RSOM"),
            GetValue("DM"),
            GetValue("Rental"),
            GetValue("Lifecycle State"),
            GetValue("Ramp Profile"),
            GetValue("Active"),
            GetValue("Store Cluster Role"),
            GetValue("Store Capacity SqFt"),
            GetValue("Store Format Tier"),
            GetValue("Catchment Type"),
            GetValue("Demographic Segment"),
            GetValue("Climate Zone"),
            GetValue("Fulfilment Enabled"),
            GetValue("Online Fulfilment Node"),
            GetValue("Store Opening Season"),
            GetValue("Store Closure Date"),
            GetValue("Refurbishment Date"),
            GetValue("Store Priority"),
            GetValue(RemarkHeader),
            GetValue(ExpectedValueHeader));
    }

    private static bool TryNormalizeStoreProfileImportRow(ImportedStoreProfileRow row, out StoreNodeMetadata normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        var storeCode = NormalizeOptionalText(row.CompCode);
        var branchName = NormalizeOptionalText(row.BranchName) ?? storeCode;
        var clusterLabel = NormalizeOptionalText(row.BranchType);
        var regionLabel = NormalizeOptionalText(row.Region);
        if (storeCode is null)
        {
            error = "CompCode is required.";
            return false;
        }

        if (branchName is null)
        {
            error = "BranchName is required.";
            return false;
        }

        if (clusterLabel is null)
        {
            error = "Branch Type is required.";
            return false;
        }

        if (regionLabel is null)
        {
            error = "Region is required.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.Latitude, out var latitude))
        {
            error = "Latitude must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.Longitude, out var longitude))
        {
            error = "Longitude must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.Gta, out var gta))
        {
            error = "GTA must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.Nta, out var nta))
        {
            error = "NTA must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.Rental, out var rental))
        {
            error = "Rental must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.StoreCapacitySqFt, out var storeCapacitySqFt))
        {
            error = "Store Capacity SqFt must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.FulfilmentEnabled, out var fulfilmentEnabled))
        {
            error = "Fulfilment Enabled must be true/false when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.OnlineFulfilmentNode, out var onlineFulfilmentNode))
        {
            error = "Online Fulfilment Node must be true/false when provided.";
            return false;
        }

        var openingDate = NormalizeWorkbookDate(row.OpeningDate);
        if (openingDate == "__INVALID__")
        {
            error = "Opening Date must be a valid Excel or calendar date.";
            return false;
        }

        var storeClosureDate = NormalizeWorkbookDate(row.StoreClosureDate);
        if (storeClosureDate == "__INVALID__")
        {
            error = "Store Closure Date must be a valid Excel or calendar date.";
            return false;
        }

        var refurbishmentDate = NormalizeWorkbookDate(row.RefurbishmentDate);
        if (refurbishmentDate == "__INVALID__")
        {
            error = "Refurbishment Date must be a valid Excel or calendar date.";
            return false;
        }

        var isActive = string.IsNullOrWhiteSpace(row.Active) || row.Active.Equals("true", StringComparison.OrdinalIgnoreCase) ||
            row.Active.Equals("yes", StringComparison.OrdinalIgnoreCase) || row.Active.Equals("active", StringComparison.OrdinalIgnoreCase) ||
            row.Active == "1";

        normalized = new StoreNodeMetadata(
            0,
            branchName,
            clusterLabel,
            regionLabel,
            NormalizeOptionalText(row.LifecycleState) ?? (isActive ? "active" : "inactive"),
            NormalizeOptionalText(row.RampProfile),
            null,
            null,
            storeCode,
            NormalizeOptionalText(row.State),
            latitude,
            longitude,
            openingDate is null or "__INVALID__" ? null : openingDate,
            NormalizeOptionalText(row.Sssg),
            NormalizeOptionalText(row.SalesType),
            NormalizeOptionalText(row.Status),
            NormalizeOptionalText(row.Storey),
            NormalizeOptionalText(row.BuildingStatus),
            gta,
            nta,
            NormalizeOptionalText(row.Rsom),
            NormalizeOptionalText(row.Dm),
            rental,
            isActive,
            NormalizeOptionalText(row.StoreClusterRole),
            storeCapacitySqFt,
            NormalizeOptionalText(row.StoreFormatTier),
            NormalizeOptionalText(row.CatchmentType),
            NormalizeOptionalText(row.DemographicSegment),
            NormalizeOptionalText(row.ClimateZone),
            fulfilmentEnabled ?? false,
            onlineFulfilmentNode ?? false,
            NormalizeOptionalText(row.StoreOpeningSeason),
            storeClosureDate is null or "__INVALID__" ? null : storeClosureDate,
            refurbishmentDate is null or "__INVALID__" ? null : refurbishmentDate,
            NormalizeOptionalText(row.StorePriority));
        return true;
    }

    private static bool TryParseOptionalDecimal(string rawValue, out decimal? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return true;
        }

        if (decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryParseOptionalInt(string rawValue, out int? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return true;
        }

        if (int.TryParse(rawValue, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            value = parsed;
            return true;
        }

        return false;
    }

    private static bool TryParseOptionalBoolean(string rawValue, out bool? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return true;
        }

        var normalized = rawValue.Trim();
        if (normalized.Equals("1", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("true", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("y", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("active", StringComparison.OrdinalIgnoreCase))
        {
            value = true;
            return true;
        }

        if (normalized.Equals("0", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("false", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("n", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("no", StringComparison.OrdinalIgnoreCase) ||
            normalized.Equals("inactive", StringComparison.OrdinalIgnoreCase))
        {
            value = false;
            return true;
        }

        return false;
    }

    private static string? NormalizeWorkbookDate(string rawValue)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            return null;
        }

        if (double.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out var oaDate))
        {
            try
            {
                return DateTime.FromOADate(oaDate).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            catch
            {
                return "__INVALID__";
            }
        }

        if (DateTime.TryParse(rawValue, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var parsed))
        {
            return parsed.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        return "__INVALID__";
    }

    private static void WriteStoreProfileImportHeader(IXLWorksheet sheet, bool includeRemark = false)
    {
        var headers = includeRemark ? [..StoreProfileImportHeaders, RemarkHeader] : StoreProfileImportHeaders;
        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteStoreProfileRow(IXLWorksheet sheet, int rowIndex, StoreProfileDto store)
    {
        sheet.Cell(rowIndex, 1).Value = store.StoreCode;
        sheet.Cell(rowIndex, 2).Value = store.BranchName;
        sheet.Cell(rowIndex, 3).Value = store.State;
        sheet.Cell(rowIndex, 4).Value = store.ClusterLabel;
        sheet.Cell(rowIndex, 5).Value = store.Latitude;
        sheet.Cell(rowIndex, 6).Value = store.Longitude;
        sheet.Cell(rowIndex, 7).Value = store.RegionLabel;
        sheet.Cell(rowIndex, 8).Value = store.OpeningDate;
        sheet.Cell(rowIndex, 9).Value = store.Sssg;
        sheet.Cell(rowIndex, 10).Value = store.SalesType;
        sheet.Cell(rowIndex, 11).Value = store.Status;
        sheet.Cell(rowIndex, 12).Value = store.Storey;
        sheet.Cell(rowIndex, 13).Value = store.BuildingStatus;
        sheet.Cell(rowIndex, 14).Value = store.Gta;
        sheet.Cell(rowIndex, 15).Value = store.Nta;
        sheet.Cell(rowIndex, 16).Value = store.Rsom;
        sheet.Cell(rowIndex, 17).Value = store.Dm;
        sheet.Cell(rowIndex, 18).Value = store.Rental;
        sheet.Cell(rowIndex, 19).Value = store.LifecycleState;
        sheet.Cell(rowIndex, 20).Value = store.RampProfileCode;
        sheet.Cell(rowIndex, 21).Value = store.IsActive ? "true" : "false";
        sheet.Cell(rowIndex, 22).Value = store.StoreClusterRole;
        sheet.Cell(rowIndex, 23).Value = store.StoreCapacitySqFt;
        sheet.Cell(rowIndex, 24).Value = store.StoreFormatTier;
        sheet.Cell(rowIndex, 25).Value = store.CatchmentType;
        sheet.Cell(rowIndex, 26).Value = store.DemographicSegment;
        sheet.Cell(rowIndex, 27).Value = store.ClimateZone;
        sheet.Cell(rowIndex, 28).Value = store.FulfilmentEnabled ? "true" : "false";
        sheet.Cell(rowIndex, 29).Value = store.OnlineFulfilmentNode ? "true" : "false";
        sheet.Cell(rowIndex, 30).Value = store.StoreOpeningSeason;
        sheet.Cell(rowIndex, 31).Value = store.StoreClosureDate;
        sheet.Cell(rowIndex, 32).Value = store.RefurbishmentDate;
        sheet.Cell(rowIndex, 33).Value = store.StorePriority;
    }

    private static void WriteStoreProfileExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedStoreProfileRow row, string error)
    {
        var values = new[]
        {
            row.CompCode, row.BranchName, row.State, row.BranchType, row.Latitude, row.Longitude, row.Region, row.OpeningDate,
            row.Sssg, row.SalesType, row.Status, row.Storey, row.BuildingStatus, row.Gta, row.Nta, row.Rsom, row.Dm, row.Rental,
            row.LifecycleState, row.RampProfile, row.Active, row.StoreClusterRole, row.StoreCapacitySqFt, row.StoreFormatTier,
            row.CatchmentType, row.DemographicSegment, row.ClimateZone, row.FulfilmentEnabled, row.OnlineFulfilmentNode,
            row.StoreOpeningSeason, row.StoreClosureDate, row.RefurbishmentDate, row.StorePriority, error
        };

        for (var index = 0; index < values.Length; index += 1)
        {
            sheet.Cell(rowIndex, index + 1).Value = values[index];
        }

        sheet.Row(rowIndex).Style.Fill.BackgroundColor = XLColor.LightPink;
    }

    private async Task<IReadOnlyList<ProductProfileMetadata>> LoadAllProductProfilesAsync(CancellationToken cancellationToken)
    {
        const int pageSize = 500;
        var page = 1;
        var items = new List<ProductProfileMetadata>();
        while (true)
        {
            var (profiles, totalCount) = await _repository.GetProductProfilesAsync(null, page, pageSize, cancellationToken);
            items.AddRange(profiles);
            if (items.Count >= totalCount || profiles.Count == 0)
            {
                break;
            }

            page += 1;
        }

        return items;
    }

    private static ProductProfileDto ToProductProfileDto(ProductProfileMetadata profile) => new(
        profile.SkuVariant,
        profile.Description,
        profile.Description2,
        profile.Price,
        profile.Cost,
        profile.DptNo,
        profile.ClssNo,
        profile.BrandNo,
        profile.Department,
        profile.Class,
        profile.Brand,
        profile.RevDepartment,
        profile.RevClass,
        profile.Subclass,
        profile.ProdGroup,
        profile.ProdType,
        profile.ActiveFlag,
        profile.OrderFlag,
        profile.BrandType,
        profile.LaunchMonth,
        profile.Gender,
        profile.Size,
        profile.Collection,
        profile.Promo,
        profile.RamadhanPromo,
        profile.IsActive,
        profile.Supplier,
        profile.LifecycleStage,
        profile.AgeStage,
        profile.GenderTarget,
        profile.Material,
        profile.PackSize,
        profile.SizeRange,
        profile.ColourFamily,
        profile.KviFlag,
        profile.MarkdownEligible,
        profile.MarkdownFloorPrice,
        profile.MinimumMarginPct,
        profile.PriceLadderGroup,
        profile.GoodBetterBestTier,
        profile.SeasonCode,
        profile.EventCode,
        profile.LaunchDate,
        profile.EndOfLifeDate,
        profile.SubstituteGroup,
        profile.CompanionGroup,
        profile.ReplenishmentType,
        profile.LeadTimeDays,
        profile.Moq,
        profile.CasePack,
        profile.StartingInventory,
        profile.ProjectedStockOnHand,
        profile.SellThroughTargetPct,
        profile.WeeksOfCoverTarget);

    private static ProductProfileMetadata NormalizeProductProfile(UpsertProductProfileRequest request) => new(
        NormalizeRequiredText(request.SkuVariant, null, "SKU Variant"),
        NormalizeRequiredText(request.Description, request.SkuVariant, "Description"),
        NormalizeOptionalText(request.Description2),
        PlanningMath.NormalizeAsp(request.Price),
        PlanningMath.NormalizeUnitCost(request.Cost),
        NormalizeRequiredText(request.DptNo, null, "DptNo"),
        NormalizeRequiredText(request.ClssNo, null, "ClssNo"),
        NormalizeOptionalText(request.BrandNo),
        NormalizeRequiredText(request.Department, null, "Department"),
        NormalizeRequiredText(request.Class, null, "Class"),
        NormalizeOptionalText(request.Brand),
        NormalizeOptionalText(request.RevDepartment),
        NormalizeOptionalText(request.RevClass),
        NormalizeRequiredText(request.Subclass, null, "Subclass"),
        NormalizeOptionalText(request.ProdGroup),
        NormalizeOptionalText(request.ProdType),
        NormalizeOptionalText(request.ActiveFlag),
        NormalizeOptionalText(request.OrderFlag),
        NormalizeOptionalText(request.BrandType),
        NormalizeOptionalText(request.LaunchMonth),
        NormalizeOptionalText(request.Gender),
        NormalizeOptionalText(request.Size),
        NormalizeOptionalText(request.Collection),
        NormalizeOptionalText(request.Promo),
        NormalizeOptionalText(request.RamadhanPromo),
        request.IsActive,
        NormalizeOptionalText(request.Supplier),
        NormalizeOptionalText(request.LifecycleStage),
        NormalizeOptionalText(request.AgeStage),
        NormalizeOptionalText(request.GenderTarget),
        NormalizeOptionalText(request.Material),
        NormalizeOptionalText(request.PackSize),
        NormalizeOptionalText(request.SizeRange),
        NormalizeOptionalText(request.ColourFamily),
        request.KviFlag,
        request.MarkdownEligible,
        request.MarkdownFloorPrice,
        request.MinimumMarginPct,
        NormalizeOptionalText(request.PriceLadderGroup),
        NormalizeOptionalText(request.GoodBetterBestTier),
        NormalizeOptionalText(request.SeasonCode),
        NormalizeOptionalText(request.EventCode),
        NormalizeOptionalDate(request.LaunchDate),
        NormalizeOptionalDate(request.EndOfLifeDate),
        NormalizeOptionalText(request.SubstituteGroup),
        NormalizeOptionalText(request.CompanionGroup),
        NormalizeOptionalText(request.ReplenishmentType),
        request.LeadTimeDays,
        request.Moq,
        request.CasePack,
        request.StartingInventory,
        request.ProjectedStockOnHand,
        request.SellThroughTargetPct,
        request.WeeksOfCoverTarget);

    private static ProductHierarchyCatalogRecord NormalizeProductHierarchy(ProductHierarchyCatalogRecord record) => new(
        NormalizeRequiredText(record.DptNo, null, "DptNo"),
        NormalizeRequiredText(record.ClssNo, null, "ClssNo"),
        NormalizeRequiredText(record.Department, null, "Department"),
        NormalizeRequiredText(record.Class, null, "Class"),
        NormalizeRequiredText(record.ProdGroup, "UNASSIGNED", "Prod Group"),
        record.IsActive);

    private static void ValidateProductProfileHeaders(IReadOnlyDictionary<string, int> headerMap)
    {
        foreach (var header in ProductProfileImportHeaders.Take(RequiredProductProfileImportHeaderCount))
        {
            if (!headerMap.ContainsKey(header))
            {
                throw new InvalidOperationException($"The product profile workbook is missing the required '{header}' column.");
            }
        }
    }

    private static void ValidateProductHierarchyHeaders(IReadOnlyDictionary<string, int> headerMap)
    {
        foreach (var header in ProductHierarchyImportHeaders)
        {
            if (!headerMap.ContainsKey(header))
            {
                throw new InvalidOperationException($"Sheet2 is missing the required '{header}' column.");
            }
        }
    }

    private static ImportedProductProfileRow ReadProductProfileImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedProductProfileRow(
            GetValue("SKU Variant"),
            GetValue("Description"),
            GetValue("Description2"),
            GetValue("Price"),
            GetValue("Cost"),
            GetValue("DptNo"),
            GetValue("ClssNo"),
            GetValue("BrandNo"),
            GetValue("Department"),
            GetValue("Class"),
            GetValue("Brand"),
            GetValue("Rev. Dept"),
            GetValue("Rev. Class"),
            GetValue("Subclass"),
            GetValue("Prod Group"),
            GetValue("Prod Type"),
            GetValue("Active Flag"),
            GetValue("Order Flag"),
            GetValue("Brand Type"),
            GetValue("Launch Month"),
            GetValue("Gender"),
            GetValue("Size"),
            GetValue("Collection"),
            GetValue("Promo"),
            GetValue("Ramadhan Promo"),
            GetValue("Supplier"),
            GetValue("Lifecycle Stage"),
            GetValue("Age Stage"),
            GetValue("Gender Target"),
            GetValue("Material"),
            GetValue("Pack Size"),
            GetValue("Size Range"),
            GetValue("Colour Family"),
            GetValue("KVI Flag"),
            GetValue("Markdown Eligible"),
            GetValue("Markdown Floor Price"),
            GetValue("Minimum Margin Pct"),
            GetValue("Price Ladder Group"),
            GetValue("Good Better Best Tier"),
            GetValue("Season Code"),
            GetValue("Event Code"),
            GetValue("Launch Date"),
            GetValue("End Of Life Date"),
            GetValue("Substitute Group"),
            GetValue("Companion Group"),
            GetValue("Replenishment Type"),
            GetValue("Lead Time Days"),
            GetValue("MOQ"),
            GetValue("Case Pack"),
            GetValue("Starting Inventory"),
            GetValue("Projected Stock On Hand"),
            GetValue("Sell Through Target Pct"),
            GetValue("Weeks Of Cover Target"),
            GetValue(RemarkHeader),
            GetValue(ExpectedValueHeader));
    }

    private static ImportedProductHierarchyRow ReadProductHierarchyImportRow(IXLRow row, IReadOnlyDictionary<string, int> headerMap)
    {
        string GetValue(string header) => headerMap.TryGetValue(header, out var index) ? row.Cell(index).GetFormattedString().Trim() : string.Empty;
        return new ImportedProductHierarchyRow(
            GetValue("DptNo"),
            GetValue("ClssNo"),
            GetValue("Dept"),
            GetValue("Class"),
            GetValue("Prod Group"));
    }

    private static bool TryNormalizeProductProfileImportRow(ImportedProductProfileRow row, out ProductProfileMetadata normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;

        if (!TryParseRequiredDecimal(row.Price, out var price))
        {
            error = "Price must be numeric.";
            return false;
        }

        if (!TryParseRequiredDecimal(row.Cost, out var cost))
        {
            error = "Cost must be numeric.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.KviFlag, out var kviFlag))
        {
            error = "KVI Flag must be true/false when provided.";
            return false;
        }

        if (!TryParseOptionalBoolean(row.MarkdownEligible, out var markdownEligible))
        {
            error = "Markdown Eligible must be true/false when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.MarkdownFloorPrice, out var markdownFloorPrice))
        {
            error = "Markdown Floor Price must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.MinimumMarginPct, out var minimumMarginPct))
        {
            error = "Minimum Margin Pct must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalInt(row.LeadTimeDays, out var leadTimeDays))
        {
            error = "Lead Time Days must be an integer when provided.";
            return false;
        }

        if (!TryParseOptionalInt(row.Moq, out var moq))
        {
            error = "MOQ must be an integer when provided.";
            return false;
        }

        if (!TryParseOptionalInt(row.CasePack, out var casePack))
        {
            error = "Case Pack must be an integer when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.StartingInventory, out var startingInventory))
        {
            error = "Starting Inventory must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.ProjectedStockOnHand, out var projectedStockOnHand))
        {
            error = "Projected Stock On Hand must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.SellThroughTargetPct, out var sellThroughTargetPct))
        {
            error = "Sell Through Target Pct must be numeric when provided.";
            return false;
        }

        if (!TryParseOptionalDecimal(row.WeeksOfCoverTarget, out var weeksOfCoverTarget))
        {
            error = "Weeks Of Cover Target must be numeric when provided.";
            return false;
        }

        var skuVariant = NormalizeOptionalText(row.SkuVariant);
        var description = NormalizeOptionalText(row.Description);
        var dptNo = NormalizeOptionalText(row.DptNo);
        var clssNo = NormalizeOptionalText(row.ClssNo);
        var department = NormalizeOptionalText(row.Department);
        var classLabel = NormalizeOptionalText(row.Class);
        var subclass = NormalizeOptionalText(row.Subclass);
        if (skuVariant is null)
        {
            error = "SKU Variant is required.";
            return false;
        }

        if (description is null)
        {
            error = "Description is required.";
            return false;
        }

        if (dptNo is null || clssNo is null)
        {
            error = "DptNo and ClssNo are required.";
            return false;
        }

        if (department is null || classLabel is null || subclass is null)
        {
            error = "Department, Class, and Subclass are required.";
            return false;
        }

        var activeFlag = NormalizeOptionalText(row.ActiveFlag);
        var isActive = activeFlag is null ||
            activeFlag.Equals("1", StringComparison.OrdinalIgnoreCase) ||
            activeFlag.Equals("true", StringComparison.OrdinalIgnoreCase) ||
            activeFlag.Equals("y", StringComparison.OrdinalIgnoreCase) ||
            activeFlag.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
            activeFlag.Equals("active", StringComparison.OrdinalIgnoreCase);

        var launchDate = NormalizeWorkbookDate(row.LaunchDate);
        if (launchDate == "__INVALID__")
        {
            error = "Launch Date must be a valid Excel or calendar date.";
            return false;
        }

        var endOfLifeDate = NormalizeWorkbookDate(row.EndOfLifeDate);
        if (endOfLifeDate == "__INVALID__")
        {
            error = "End Of Life Date must be a valid Excel or calendar date.";
            return false;
        }

        normalized = new ProductProfileMetadata(
            skuVariant,
            description,
            NormalizeOptionalText(row.Description2),
            PlanningMath.NormalizeAsp(price),
            PlanningMath.NormalizeUnitCost(cost),
            dptNo,
            clssNo,
            NormalizeOptionalText(row.BrandNo),
            department,
            classLabel,
            NormalizeOptionalText(row.Brand),
            NormalizeOptionalText(row.RevDepartment),
            NormalizeOptionalText(row.RevClass),
            subclass,
            NormalizeOptionalText(row.ProdGroup),
            NormalizeOptionalText(row.ProdType),
            activeFlag,
            NormalizeOptionalText(row.OrderFlag),
            NormalizeOptionalText(row.BrandType),
            NormalizeOptionalText(row.LaunchMonth),
            NormalizeOptionalText(row.Gender),
            NormalizeOptionalText(row.Size),
            NormalizeOptionalText(row.Collection),
            NormalizeOptionalText(row.Promo),
            NormalizeOptionalText(row.RamadhanPromo),
            isActive,
            NormalizeOptionalText(row.Supplier),
            NormalizeOptionalText(row.LifecycleStage),
            NormalizeOptionalText(row.AgeStage),
            NormalizeOptionalText(row.GenderTarget),
            NormalizeOptionalText(row.Material),
            NormalizeOptionalText(row.PackSize),
            NormalizeOptionalText(row.SizeRange),
            NormalizeOptionalText(row.ColourFamily),
            kviFlag ?? false,
            markdownEligible ?? true,
            markdownFloorPrice,
            minimumMarginPct,
            NormalizeOptionalText(row.PriceLadderGroup),
            NormalizeOptionalText(row.GoodBetterBestTier),
            NormalizeOptionalText(row.SeasonCode),
            NormalizeOptionalText(row.EventCode),
            launchDate is null or "__INVALID__" ? null : launchDate,
            endOfLifeDate is null or "__INVALID__" ? null : endOfLifeDate,
            NormalizeOptionalText(row.SubstituteGroup),
            NormalizeOptionalText(row.CompanionGroup),
            NormalizeOptionalText(row.ReplenishmentType),
            leadTimeDays,
            moq,
            casePack,
            startingInventory,
            projectedStockOnHand,
            sellThroughTargetPct,
            weeksOfCoverTarget);
        return true;
    }

    private static bool TryNormalizeProductHierarchyImportRow(ImportedProductHierarchyRow row, out ProductHierarchyCatalogRecord normalized, out string error)
    {
        normalized = default!;
        error = string.Empty;
        var dptNo = NormalizeOptionalText(row.DptNo);
        var clssNo = NormalizeOptionalText(row.ClssNo);
        var department = NormalizeOptionalText(row.Department);
        var classLabel = NormalizeOptionalText(row.Class);
        var prodGroup = NormalizeOptionalText(row.ProdGroup) ?? "UNASSIGNED";
        if (dptNo is null || clssNo is null)
        {
            error = "DptNo and ClssNo are required.";
            return false;
        }

        if (department is null || classLabel is null)
        {
            error = "Dept and Class are required.";
            return false;
        }

        normalized = new ProductHierarchyCatalogRecord(dptNo, clssNo, department, classLabel, prodGroup, true);
        return true;
    }

    private static bool TryParseRequiredDecimal(string rawValue, out decimal value)
    {
        return decimal.TryParse(rawValue, NumberStyles.Number, CultureInfo.InvariantCulture, out value);
    }

    private static void WriteProductProfileImportHeader(IXLWorksheet sheet, bool includeRemark = false)
    {
        var headers = includeRemark ? [..ProductProfileImportHeaders, RemarkHeader] : ProductProfileImportHeaders;
        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteProductHierarchyImportHeader(IXLWorksheet sheet)
    {
        for (var index = 0; index < ProductHierarchyImportHeaders.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = ProductHierarchyImportHeaders[index];
        }
    }

    private static void WriteProductProfileRow(IXLWorksheet sheet, int rowIndex, ProductProfileDto profile)
    {
        sheet.Cell(rowIndex, 1).Value = profile.SkuVariant;
        sheet.Cell(rowIndex, 2).Value = profile.Description;
        sheet.Cell(rowIndex, 3).Value = profile.Description2;
        sheet.Cell(rowIndex, 4).Value = profile.Price;
        sheet.Cell(rowIndex, 5).Value = profile.Cost;
        sheet.Cell(rowIndex, 6).Value = profile.DptNo;
        sheet.Cell(rowIndex, 7).Value = profile.ClssNo;
        sheet.Cell(rowIndex, 8).Value = profile.BrandNo;
        sheet.Cell(rowIndex, 9).Value = profile.Department;
        sheet.Cell(rowIndex, 10).Value = profile.Class;
        sheet.Cell(rowIndex, 11).Value = profile.Brand;
        sheet.Cell(rowIndex, 12).Value = profile.RevDepartment;
        sheet.Cell(rowIndex, 13).Value = profile.RevClass;
        sheet.Cell(rowIndex, 14).Value = profile.Subclass;
        sheet.Cell(rowIndex, 15).Value = profile.ProdGroup;
        sheet.Cell(rowIndex, 16).Value = profile.ProdType;
        sheet.Cell(rowIndex, 17).Value = profile.ActiveFlag;
        sheet.Cell(rowIndex, 18).Value = profile.OrderFlag;
        sheet.Cell(rowIndex, 19).Value = profile.BrandType;
        sheet.Cell(rowIndex, 20).Value = profile.LaunchMonth;
        sheet.Cell(rowIndex, 21).Value = profile.Gender;
        sheet.Cell(rowIndex, 22).Value = profile.Size;
        sheet.Cell(rowIndex, 23).Value = profile.Collection;
        sheet.Cell(rowIndex, 24).Value = profile.Promo;
        sheet.Cell(rowIndex, 25).Value = profile.RamadhanPromo;
        sheet.Cell(rowIndex, 26).Value = profile.Supplier;
        sheet.Cell(rowIndex, 27).Value = profile.LifecycleStage;
        sheet.Cell(rowIndex, 28).Value = profile.AgeStage;
        sheet.Cell(rowIndex, 29).Value = profile.GenderTarget;
        sheet.Cell(rowIndex, 30).Value = profile.Material;
        sheet.Cell(rowIndex, 31).Value = profile.PackSize;
        sheet.Cell(rowIndex, 32).Value = profile.SizeRange;
        sheet.Cell(rowIndex, 33).Value = profile.ColourFamily;
        sheet.Cell(rowIndex, 34).Value = profile.KviFlag ? "true" : "false";
        sheet.Cell(rowIndex, 35).Value = profile.MarkdownEligible ? "true" : "false";
        sheet.Cell(rowIndex, 36).Value = profile.MarkdownFloorPrice;
        sheet.Cell(rowIndex, 37).Value = profile.MinimumMarginPct;
        sheet.Cell(rowIndex, 38).Value = profile.PriceLadderGroup;
        sheet.Cell(rowIndex, 39).Value = profile.GoodBetterBestTier;
        sheet.Cell(rowIndex, 40).Value = profile.SeasonCode;
        sheet.Cell(rowIndex, 41).Value = profile.EventCode;
        sheet.Cell(rowIndex, 42).Value = profile.LaunchDate;
        sheet.Cell(rowIndex, 43).Value = profile.EndOfLifeDate;
        sheet.Cell(rowIndex, 44).Value = profile.SubstituteGroup;
        sheet.Cell(rowIndex, 45).Value = profile.CompanionGroup;
        sheet.Cell(rowIndex, 46).Value = profile.ReplenishmentType;
        sheet.Cell(rowIndex, 47).Value = profile.LeadTimeDays;
        sheet.Cell(rowIndex, 48).Value = profile.Moq;
        sheet.Cell(rowIndex, 49).Value = profile.CasePack;
        sheet.Cell(rowIndex, 50).Value = profile.StartingInventory;
        sheet.Cell(rowIndex, 51).Value = profile.ProjectedStockOnHand;
        sheet.Cell(rowIndex, 52).Value = profile.SellThroughTargetPct;
        sheet.Cell(rowIndex, 53).Value = profile.WeeksOfCoverTarget;
    }

    private static void WriteProductHierarchyRow(IXLWorksheet sheet, int rowIndex, ProductHierarchyCatalogRecord row)
    {
        sheet.Cell(rowIndex, 1).Value = row.DptNo;
        sheet.Cell(rowIndex, 2).Value = row.ClssNo;
        sheet.Cell(rowIndex, 3).Value = row.Department;
        sheet.Cell(rowIndex, 4).Value = row.Class;
        sheet.Cell(rowIndex, 5).Value = row.ProdGroup;
    }

    private static void WriteProductProfileExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedProductProfileRow row, string error)
    {
        var values = new[]
        {
            row.SkuVariant, row.Description, row.Description2, row.Price, row.Cost, row.DptNo, row.ClssNo, row.BrandNo,
            row.Department, row.Class, row.Brand, row.RevDepartment, row.RevClass, row.Subclass, row.ProdGroup, row.ProdType,
            row.ActiveFlag, row.OrderFlag, row.BrandType, row.LaunchMonth, row.Gender, row.Size, row.Collection, row.Promo, row.RamadhanPromo,
            row.Supplier, row.LifecycleStage, row.AgeStage, row.GenderTarget, row.Material, row.PackSize, row.SizeRange, row.ColourFamily,
            row.KviFlag, row.MarkdownEligible, row.MarkdownFloorPrice, row.MinimumMarginPct, row.PriceLadderGroup, row.GoodBetterBestTier,
            row.SeasonCode, row.EventCode, row.LaunchDate, row.EndOfLifeDate, row.SubstituteGroup, row.CompanionGroup,
            row.ReplenishmentType, row.LeadTimeDays, row.Moq, row.CasePack, row.StartingInventory, row.ProjectedStockOnHand,
            row.SellThroughTargetPct, row.WeeksOfCoverTarget,
            error
        };

        for (var index = 0; index < values.Length; index += 1)
        {
            sheet.Cell(rowIndex, index + 1).Value = values[index];
        }

        sheet.Row(rowIndex).Style.Fill.BackgroundColor = XLColor.LightPink;
    }

    private readonly record struct ImportedPlanRow(
        string SheetName,
        string Store,
        string Department,
        string Class,
        string Subclass,
        string Year,
        string Month,
        string SalesRevenue,
        string SoldQty,
        string Asp,
        string UnitCost,
        string TotalCosts,
        string Gp,
        string GpPercent,
        string Remark,
        string ExpectedValue);

    private readonly record struct ImportedStoreProfileRow(
        string CompCode,
        string BranchName,
        string State,
        string BranchType,
        string Latitude,
        string Longitude,
        string Region,
        string OpeningDate,
        string Sssg,
        string SalesType,
        string Status,
        string Storey,
        string BuildingStatus,
        string Gta,
        string Nta,
        string Rsom,
        string Dm,
        string Rental,
        string LifecycleState,
        string RampProfile,
        string Active,
        string StoreClusterRole,
        string StoreCapacitySqFt,
        string StoreFormatTier,
        string CatchmentType,
        string DemographicSegment,
        string ClimateZone,
        string FulfilmentEnabled,
        string OnlineFulfilmentNode,
        string StoreOpeningSeason,
        string StoreClosureDate,
        string RefurbishmentDate,
        string StorePriority,
        string Remark,
        string ExpectedValue);

    private readonly record struct ImportedProductProfileRow(
        string SkuVariant,
        string Description,
        string Description2,
        string Price,
        string Cost,
        string DptNo,
        string ClssNo,
        string BrandNo,
        string Department,
        string Class,
        string Brand,
        string RevDepartment,
        string RevClass,
        string Subclass,
        string ProdGroup,
        string ProdType,
        string ActiveFlag,
        string OrderFlag,
        string BrandType,
        string LaunchMonth,
        string Gender,
        string Size,
        string Collection,
        string Promo,
        string RamadhanPromo,
        string Supplier,
        string LifecycleStage,
        string AgeStage,
        string GenderTarget,
        string Material,
        string PackSize,
        string SizeRange,
        string ColourFamily,
        string KviFlag,
        string MarkdownEligible,
        string MarkdownFloorPrice,
        string MinimumMarginPct,
        string PriceLadderGroup,
        string GoodBetterBestTier,
        string SeasonCode,
        string EventCode,
        string LaunchDate,
        string EndOfLifeDate,
        string SubstituteGroup,
        string CompanionGroup,
        string ReplenishmentType,
        string LeadTimeDays,
        string Moq,
        string CasePack,
        string StartingInventory,
        string ProjectedStockOnHand,
        string SellThroughTargetPct,
        string WeeksOfCoverTarget,
        string Remark,
        string ExpectedValue);

    private readonly record struct ImportedProductHierarchyRow(
        string DptNo,
        string ClssNo,
        string Department,
        string Class,
        string ProdGroup);

    private readonly record struct NormalizedImportRow(
        string Store,
        string Department,
        string Class,
        string Subclass,
        int Year,
        int MonthIndex,
        decimal SalesRevenue,
        decimal SoldQty,
        decimal Asp,
        decimal UnitCost,
        decimal TotalCosts,
        decimal Gp,
        decimal GpPercent);
}
