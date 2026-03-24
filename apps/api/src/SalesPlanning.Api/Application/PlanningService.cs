using System.Globalization;
using ClosedXML.Excel;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed class PlanningService : IPlanningService
{
    private static readonly string[] MonthLabels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    private readonly IPlanningRepository _repository;
    private readonly ISplashAllocator _splashAllocator;

    public PlanningService(IPlanningRepository repository, ISplashAllocator splashAllocator)
    {
        _repository = repository;
        _splashAllocator = splashAllocator;
    }

    public Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, CancellationToken cancellationToken)
    {
        return _repository.GetGridSliceAsync(scenarioVersionId, cancellationToken);
    }

    public async Task<EditCellsResponse> ApplyEditsAsync(EditCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, cancellationToken);
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
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "manual-edit", cancellationToken);
        var actionId = await AppendAuditAsync("manual_edit", "manual", userId, request.Comment, deltas, cancellationToken);
        return new EditCellsResponse(actionId, deltas.Count, "applied");
    }

    public async Task<SplashResponse> ApplySplashAsync(SplashRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, cancellationToken);
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
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "splash", cancellationToken);
        var actionId = await AppendAuditAsync("splash", request.Method, userId, request.Comment, deltas, cancellationToken);
        return new SplashResponse(actionId, "applied", deltas.Count, deltas.Count(delta => delta.WasLocked));
    }

    public async Task<LockCellsResponse> ApplyLockAsync(LockCellsRequest request, string userId, CancellationToken cancellationToken)
    {
        var scenarioCells = (await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, cancellationToken))
            .Where(cell => cell.Coordinate.MeasureId == request.MeasureId)
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell);

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

            cell.IsLocked = request.Locked;
            cell.LockReason = request.Reason;
            cell.LockedBy = request.Locked ? userId : null;
            cell.RowVersion += 1;
        }

        await _repository.UpsertCellsAsync(scenarioCells.Values, cancellationToken);
        await AppendAuditAsync(request.Locked ? "lock" : "unlock", "lock", userId, request.Reason, [], cancellationToken);
        return new LockCellsResponse(request.Coordinates.Count, request.Locked);
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

    public async Task<GenerateNextYearResponse> GenerateNextYearAsync(GenerateNextYearRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        if (!metadata.TimePeriods.TryGetValue(request.SourceYearTimePeriodId, out var sourceYear) || !string.Equals(sourceYear.Grain, "year", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Year {request.SourceYearTimePeriodId} was not found.");
        }

        var sourceFiscalYear = (int)(request.SourceYearTimePeriodId / 100);
        var targetFiscalYear = sourceFiscalYear + 1;
        var targetYearTimePeriodId = targetFiscalYear * 100L;

        await _repository.EnsureYearAsync(request.ScenarioVersionId, targetFiscalYear, cancellationToken);
        metadata = await _repository.GetMetadataAsync(cancellationToken);

        var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, cancellationToken);
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
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "generate-next-year", cancellationToken);
        await AppendAuditAsync("generate_next_year", "copy-inputs", userId, $"Generated FY{targetFiscalYear % 100:00} from FY{sourceFiscalYear % 100:00}", deltas, cancellationToken);
        return new GenerateNextYearResponse(request.SourceYearTimePeriodId, targetYearTimePeriodId, "applied", copiedCount);
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

    public async Task<ApplyGrowthFactorResponse> ApplyGrowthFactorAsync(ApplyGrowthFactorRequest request, string userId, CancellationToken cancellationToken)
    {
        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var originalCells = await _repository.GetScenarioCellsAsync(request.ScenarioVersionId, cancellationToken);
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
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "growth-factor", cancellationToken);
        var actionId = await AppendAuditAsync("growth_factor", "growth-factor", userId, request.Comment, deltas, cancellationToken);
        return new ApplyGrowthFactorResponse(actionId, "applied", growthFactor, deltas.Count);
    }

    public async Task<SaveScenarioResponse> SaveScenarioAsync(SaveScenarioRequest request, string userId, CancellationToken cancellationToken)
    {
        await AppendAuditAsync("save", request.Mode, userId, $"Scenario {request.ScenarioVersionId} save checkpoint", [], cancellationToken);
        return new SaveScenarioResponse("saved", request.Mode, DateTimeOffset.UtcNow);
    }

    public async Task<ImportWorkbookResponse> ImportWorkbookAsync(long scenarioVersionId, Stream workbookStream, string fileName, string userId, CancellationToken cancellationToken)
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

        var metadata = await _repository.GetMetadataAsync(cancellationToken);
        var originalCells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
        var workingCells = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());
        var rowsProcessed = 0;
        var rowsCreated = 0;
        var touchedCoordinates = new HashSet<string>(StringComparer.Ordinal);
        var exceptionWorkbook = new XLWorkbook();
        var exceptionCountsBySheet = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var worksheet in workbook.Worksheets)
        {
            var headerMap = GetHeaderMap(worksheet);
            ValidateImportHeaders(headerMap);
            var exceptionSheet = exceptionWorkbook.AddWorksheet(worksheet.Name);
            WriteImportHeader(exceptionSheet);
            var exceptionRowIndex = 2;

            foreach (var row in worksheet.RowsUsed().Skip(1))
            {
                if (row.CellsUsed().All(cell => cell.IsEmpty()))
                {
                    continue;
                }

                rowsProcessed += 1;
                var importRow = ReadImportRow(worksheet.Name, row, headerMap);

                if (!TryNormalizeImportRow(importRow, out var normalized, out var exceptionMessage))
                {
                    WriteImportExceptionRow(exceptionSheet, exceptionRowIndex++, importRow, exceptionMessage);
                    exceptionCountsBySheet[worksheet.Name] = exceptionRowIndex - 2;
                    continue;
                }

                await _repository.EnsureYearAsync(scenarioVersionId, normalized.Year, cancellationToken);
                metadata = await _repository.GetMetadataAsync(cancellationToken);

                var storeNode = await EnsureNodeAsync(scenarioVersionId, "store", null, normalized.Store, cancellationToken);
                var departmentNode = await EnsureNodeAsync(scenarioVersionId, "department", storeNode.Node.ProductNodeId, normalized.Department, cancellationToken);
                var classNode = await EnsureNodeAsync(scenarioVersionId, "class", departmentNode.Node.ProductNodeId, normalized.Class, cancellationToken);
                rowsCreated += storeNode.CreatedCount + departmentNode.CreatedCount + classNode.CreatedCount;
                metadata = await _repository.GetMetadataAsync(cancellationToken);
                var refreshedCells = await _repository.GetScenarioCellsAsync(scenarioVersionId, cancellationToken);
                foreach (var cell in refreshedCells)
                {
                    if (!workingCells.ContainsKey(cell.Coordinate.Key))
                    {
                        workingCells[cell.Coordinate.Key] = cell.Clone();
                    }
                }

                var timePeriodId = BuildMonthTimePeriodId(normalized.Year, normalized.MonthIndex);
                ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, classNode.Node.StoreId, classNode.Node.ProductNodeId, timePeriodId), normalized.SoldQty, workingCells, metadata);
                ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, classNode.Node.StoreId, classNode.Node.ProductNodeId, timePeriodId), normalized.Asp, workingCells, metadata);
                ApplyLeafMeasureEdit(new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, classNode.Node.StoreId, classNode.Node.ProductNodeId, timePeriodId), normalized.UnitCost, workingCells, metadata);

                foreach (var measureId in PlanningMeasures.SupportedMeasureIds)
                {
                    touchedCoordinates.Add(new PlanningCellCoordinate(scenarioVersionId, measureId, classNode.Node.StoreId, classNode.Node.ProductNodeId, timePeriodId).Key);
                }
            }
        }

        RecalculateAll(workingCells, metadata, scenarioVersionId);
        var deltas = await PersistScenarioChangesAsync(originalCells, workingCells.Values, "import", cancellationToken);
        await AppendAuditAsync("import", "workbook", userId, $"Imported workbook {fileName}", deltas, cancellationToken);

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
            var classNodes = metadata.ProductNodes.Values
                .Where(node => node.StoreId == storeNode.StoreId && node.IsLeaf)
                .OrderBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
                .ToList();
            var monthPeriods = metadata.TimePeriods.Values
                .Where(period => string.Equals(period.Grain, "month", StringComparison.OrdinalIgnoreCase))
                .OrderBy(period => period.SortOrder)
                .ToList();

            foreach (var classNode in classNodes)
            {
                var departmentNode = metadata.ProductNodes[classNode.ParentProductNodeId!.Value];
                foreach (var monthPeriod in monthPeriods)
                {
                    var revenue = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SalesRevenue, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var quantity = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.SoldQuantity, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var asp = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.AverageSellingPrice, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var unitCost = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.UnitCost, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var totalCosts = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.TotalCosts, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var grossProfit = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfit, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;
                    var grossProfitPercent = cellLookup[new PlanningCellCoordinate(scenarioVersionId, PlanningMeasures.GrossProfitPercent, classNode.StoreId, classNode.ProductNodeId, monthPeriod.TimePeriodId).Key].EffectiveValue;

                    sheet.Cell(rowIndex, 1).Value = storeNode.Label;
                    sheet.Cell(rowIndex, 2).Value = departmentNode.Label;
                    sheet.Cell(rowIndex, 3).Value = classNode.Label;
                    sheet.Cell(rowIndex, 4).Value = monthPeriod.TimePeriodId / 100;
                    sheet.Cell(rowIndex, 5).Value = monthPeriod.Label;
                    sheet.Cell(rowIndex, 6).Value = revenue;
                    sheet.Cell(rowIndex, 7).Value = quantity;
                    sheet.Cell(rowIndex, 8).Value = asp;
                    sheet.Cell(rowIndex, 9).Value = unitCost;
                    sheet.Cell(rowIndex, 10).Value = totalCosts;
                    sheet.Cell(rowIndex, 11).Value = grossProfit;
                    sheet.Cell(rowIndex, 12).Value = grossProfitPercent;
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

    private async Task<IReadOnlyList<PlanningCellDeltaAudit>> PersistScenarioChangesAsync(
        IReadOnlyList<PlanningCell> originalCells,
        IEnumerable<PlanningCell> workingCells,
        string changeKind,
        CancellationToken cancellationToken)
    {
        var originalByKey = originalCells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var changedCells = new List<PlanningCell>();
        var deltas = new List<PlanningCellDeltaAudit>();

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

            if (original.EffectiveValue != updated.EffectiveValue)
            {
                deltas.Add(new PlanningCellDeltaAudit(updated.Coordinate, original.EffectiveValue, updated.EffectiveValue, original.IsLocked, changeKind));
            }
        }

        await _repository.UpsertCellsAsync(changedCells, cancellationToken);
        return deltas;
    }

    private async Task<long> AppendAuditAsync(
        string actionType,
        string method,
        string userId,
        string? comment,
        IReadOnlyList<PlanningCellDeltaAudit> deltas,
        CancellationToken cancellationToken)
    {
        var audit = new PlanningActionAudit(
            await _repository.GetNextActionIdAsync(cancellationToken),
            actionType,
            method,
            userId,
            comment,
            DateTimeOffset.UtcNow,
            deltas);

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
                .OrderBy(entry => entry.Key, StringComparer.OrdinalIgnoreCase)
                .Select(entry => new HierarchyDepartmentDto(
                    entry.Key,
                    entry.Value.OrderBy(value => value, StringComparer.OrdinalIgnoreCase).ToList()))
                .ToList());
    }

    private static bool HasMaterialChange(PlanningCell left, PlanningCell right)
    {
        return left.InputValue != right.InputValue ||
               left.OverrideValue != right.OverrideValue ||
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
        foreach (var requiredHeader in new[]
                 {
                     "Store", "Department", "Class", "Year", "Month",
                     "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"
                 })
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
            row.Cell(headerMap["Year"]).GetString().Trim(),
            row.Cell(headerMap["Month"]).GetString().Trim(),
            row.Cell(headerMap["Sales Revenue"]).GetString().Trim(),
            row.Cell(headerMap["Sold Qty"]).GetString().Trim(),
            row.Cell(headerMap["ASP"]).GetString().Trim(),
            row.Cell(headerMap["Unit Cost"]).GetString().Trim(),
            row.Cell(headerMap["Total Costs"]).GetString().Trim(),
            row.Cell(headerMap["GP"]).GetString().Trim(),
            row.Cell(headerMap["GP%"]).GetString().Trim());
    }

    private static bool TryNormalizeImportRow(ImportedPlanRow row, out NormalizedImportRow normalized, out string exceptionMessage)
    {
        normalized = default;
        exceptionMessage = string.Empty;

        var store = string.IsNullOrWhiteSpace(row.Store) ? row.SheetName : row.Store.Trim();
        if (!string.Equals(store, row.SheetName, StringComparison.OrdinalIgnoreCase))
        {
            exceptionMessage = "Store value does not match worksheet name.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(row.Department) || string.IsNullOrWhiteSpace(row.Class))
        {
            exceptionMessage = "Department and Class are required.";
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
            return false;
        }

        if (hasTotalCosts && normalizedTotalCosts != PlanningMath.NormalizeTotalCosts(totalCosts))
        {
            exceptionMessage = "Total Costs does not equal Unit Cost * Sold Qty after normalization.";
            return false;
        }

        if (hasGrossProfit && normalizedGrossProfit != PlanningMath.NormalizeGrossProfit(grossProfit))
        {
            exceptionMessage = "GP does not equal (ASP - Unit Cost) * Sold Qty after normalization.";
            return false;
        }

        if (hasGrossProfitPercent && normalizedGrossProfitPercent != PlanningMath.NormalizeGrossProfitPercent(grossProfitPercent))
        {
            exceptionMessage = "GP% does not equal (ASP - Unit Cost) / ASP * 100 after normalization.";
            return false;
        }

        normalized = new NormalizedImportRow(
            store,
            row.Department.Trim(),
            row.Class.Trim(),
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

    private static void WriteImportHeader(IXLWorksheet sheet)
    {
        var headers = new[]
        {
            "Store", "Department", "Class", "Year", "Month",
            "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"
        };
        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteImportExceptionRow(IXLWorksheet sheet, int rowIndex, ImportedPlanRow row, string exceptionMessage)
    {
        sheet.Cell(rowIndex, 1).Value = row.Store;
        sheet.Cell(rowIndex, 2).Value = row.Department;
        sheet.Cell(rowIndex, 3).Value = row.Class;
        sheet.Cell(rowIndex, 4).Value = row.Year;
        sheet.Cell(rowIndex, 5).Value = row.Month;
        sheet.Cell(rowIndex, 6).Value = row.SalesRevenue;
        sheet.Cell(rowIndex, 7).Value = row.SoldQty;
        sheet.Cell(rowIndex, 8).Value = row.Asp;
        sheet.Cell(rowIndex, 9).Value = row.UnitCost;
        sheet.Cell(rowIndex, 10).Value = row.TotalCosts;
        sheet.Cell(rowIndex, 11).Value = row.Gp;
        sheet.Cell(rowIndex, 12).Value = row.GpPercent;
        sheet.Row(rowIndex).Style.Fill.BackgroundColor = XLColor.LightPink;
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

    private readonly record struct ImportedPlanRow(
        string SheetName,
        string Store,
        string Department,
        string Class,
        string Year,
        string Month,
        string SalesRevenue,
        string SoldQty,
        string Asp,
        string UnitCost,
        string TotalCosts,
        string Gp,
        string GpPercent);

    private readonly record struct NormalizedImportRow(
        string Store,
        string Department,
        string Class,
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
