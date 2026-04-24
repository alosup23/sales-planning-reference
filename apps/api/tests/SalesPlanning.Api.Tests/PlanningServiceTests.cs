using ClosedXML.Excel;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using SalesPlanning.Api.Infrastructure;
using SalesPlanning.Api.Security;
using System.Security.Claims;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PlanningServiceTests
    : IClassFixture<PostgresPlanningTestFixture>, IAsyncLifetime
{
    private readonly IPlanningRepository _repository;
    private readonly PlanningService _service;
    private readonly PostgresPlanningTestFixture _fixture;

    public PlanningServiceTests(PostgresPlanningTestFixture fixture)
    {
        _fixture = fixture;
        _repository = fixture.Repository;
        _service = fixture.Service;
    }

    public Task InitializeAsync() => _fixture.ResetAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task<PlanningCell?> GetEffectiveCellAsync(PlanningCellCoordinate coordinate, string userId = "planner.one")
    {
        var committedCellTask = _repository.GetCellAsync(coordinate, CancellationToken.None);
        var draftCellsTask = _repository.GetDraftCellsAsync(coordinate.ScenarioVersionId, userId, [coordinate], CancellationToken.None);
        await Task.WhenAll(committedCellTask, draftCellsTask);

        return (await draftCellsTask).FirstOrDefault() ?? await committedCellTask;
    }

    [Fact]
    public async Task GetGridSliceAsync_ReturnsExpandedMeasureSetAndMultiYearShape()
    {
        var grid = await _service.GetGridSliceAsync(1, null, null, new[] { 2110L }, false, "planner.one", CancellationToken.None);

        Assert.Equal(7, grid.Measures.Count);
        Assert.Contains(grid.Measures, measure => measure.MeasureId == PlanningMeasures.GrossProfitPercent && measure.DisplayAsPercent);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202600);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202700);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks", "Cola" }));
    }

    [Fact]
    public async Task GetGridBranchRowsAsync_ReturnsOnlyDirectChildrenForTheRequestedBranch()
    {
        var branch = await _service.GetGridBranchRowsAsync(1, 2100, "planner.one", CancellationToken.None);

        Assert.Equal(2100, branch.ParentProductNodeId);
        Assert.NotEmpty(branch.Rows);
        Assert.All(branch.Rows, row => Assert.Equal(2, row.Level));
        Assert.Contains(branch.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks" }));
        Assert.DoesNotContain(branch.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks", "Cola" }));
    }

    [Fact]
    public async Task GetPlanningStoreScopesAsync_ReturnsStoreRootProductNodeIds()
    {
        var response = await _service.GetPlanningStoreScopesAsync(CancellationToken.None);
        var storeARoot = await _repository.FindProductNodeByPathAsync(["Store A"], CancellationToken.None);

        Assert.NotNull(storeARoot);
        var storeA = Assert.Single(response.Stores, store => store.StoreId == 101);
        Assert.Equal(storeARoot!.ProductNodeId, storeA.RootProductNodeId);
        Assert.Equal("Store A", storeA.BranchName);
    }

    [Fact]
    public async Task ApplyEditsAsync_WhenGrossProfitPercentChanges_RecalculatesAspRevenueAndGrossProfit()
    {
        var gpPercentCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, 101, 2111, 202603), CancellationToken.None);
        Assert.NotNull(gpPercentCell);

        var result = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.GrossProfitPercent,
                "Margin update",
                new[]
                {
                    new EditCellRequest(101, 2111, 202603, 30m, "input", gpPercentCell!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None);

        Assert.NotNull(result.Patch);
        Assert.True(result.Availability.CanUndo);
        Assert.Contains(result.Patch!.Cells, cell => cell.StoreId == 101 && cell.ProductNodeId == 2111 && cell.TimePeriodId == 202603 && cell.MeasureId == PlanningMeasures.GrossProfitPercent);
        Assert.Contains(result.Patch.Cells, cell => cell.StoreId == 101 && cell.ProductNodeId == 2111 && cell.TimePeriodId == 202603 && cell.MeasureId == PlanningMeasures.AverageSellingPrice);

        var quantityCell = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2111, 202603));
        var aspCell = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.AverageSellingPrice, 101, 2111, 202603));
        var unitCostCell = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.UnitCost, 101, 2111, 202603));
        var revenueCell = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603));
        var grossProfitCell = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfit, 101, 2111, 202603));

        Assert.NotNull(quantityCell);
        Assert.NotNull(aspCell);
        Assert.NotNull(unitCostCell);
        Assert.NotNull(revenueCell);
        Assert.NotNull(grossProfitCell);

        var expectedAsp = PlanningMath.DeriveAspFromGrossProfitPercent(unitCostCell!.EffectiveValue, 30m);
        Assert.Equal(expectedAsp, aspCell!.EffectiveValue);
        Assert.Equal(PlanningMath.CalculateRevenue(quantityCell!.EffectiveValue, aspCell.EffectiveValue), revenueCell!.EffectiveValue);
        Assert.Equal(PlanningMath.CalculateGrossProfit(quantityCell.EffectiveValue, aspCell.EffectiveValue, unitCostCell.EffectiveValue), grossProfitCell!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyLockAsync_WhenYearRevenueIsLocked_DescendantRevenueEditFails()
    {
        var lockResult = await _service.ApplyLockAsync(
            new LockCellsRequest(1, PlanningMeasures.SalesRevenue, true, "Freeze year", new[] { new LockCoordinateDto(101, 2100, 202600) }),
            "planner.one",
            CancellationToken.None);

        Assert.True(lockResult.Availability.CanUndo);

        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2121, 202603), CancellationToken.None);
        Assert.NotNull(revenueCell);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Blocked revenue edit",
                new[]
                {
                    new EditCellRequest(101, 2121, 202603, 900m, "input", revenueCell!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None));

        Assert.Contains("locked", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task UndoAndRedoAsync_ReversesLeafEditAndRestoresAvailability()
    {
        var revenueCoordinate = new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603);
        var beforeRevenue = await _repository.GetCellAsync(revenueCoordinate, CancellationToken.None);
        Assert.NotNull(beforeRevenue);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Undo revenue edit",
                [new EditCellRequest(101, 2111, 202603, beforeRevenue!.EffectiveValue + 125m, "input", beforeRevenue.RowVersion)]),
            "planner.one",
            CancellationToken.None);

        var editedRevenue = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(editedRevenue);
        Assert.NotEqual(beforeRevenue.EffectiveValue, editedRevenue!.EffectiveValue);

        var availabilityAfterEdit = await _service.GetUndoRedoAvailabilityAsync(1, "planner.one", CancellationToken.None);
        Assert.True(availabilityAfterEdit.CanUndo);
        Assert.False(availabilityAfterEdit.CanRedo);
        Assert.Equal(1, availabilityAfterEdit.UndoDepth);

        var undoResult = await _service.UndoAsync(1, "planner.one", CancellationToken.None);
        var afterUndo = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(afterUndo);
        Assert.Equal("applied", undoResult.Status);
        Assert.Equal(beforeRevenue.EffectiveValue, afterUndo!.EffectiveValue);
        Assert.False(undoResult.Availability.CanUndo);
        Assert.True(undoResult.Availability.CanRedo);

        var redoResult = await _service.RedoAsync(1, "planner.one", CancellationToken.None);
        var afterRedo = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(afterRedo);
        Assert.Equal("applied", redoResult.Status);
        Assert.Equal(editedRevenue.EffectiveValue, afterRedo!.EffectiveValue);
        Assert.True(redoResult.Availability.CanUndo);
        Assert.False(redoResult.Availability.CanRedo);
    }

    [Fact]
    public async Task UndoAndRedoAsync_ReversesLockStateChanges()
    {
        var revenueCoordinate = new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603);
        var beforeLock = await _repository.GetCellAsync(revenueCoordinate, CancellationToken.None);
        Assert.NotNull(beforeLock);
        Assert.False(beforeLock!.IsLocked);

        await _service.ApplyLockAsync(
            new LockCellsRequest(1, PlanningMeasures.SalesRevenue, true, "Planner hold", [new LockCoordinateDto(101, 2111, 202603)]),
            "planner.one",
            CancellationToken.None);

        var lockedCell = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(lockedCell);
        Assert.True(lockedCell!.IsLocked);
        Assert.Equal("Planner hold", lockedCell.LockReason);

        await _service.UndoAsync(1, "planner.one", CancellationToken.None);
        var afterUndo = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(afterUndo);
        Assert.False(afterUndo!.IsLocked);
        Assert.Null(afterUndo.LockReason);

        await _service.RedoAsync(1, "planner.one", CancellationToken.None);
        var afterRedo = await GetEffectiveCellAsync(revenueCoordinate);
        Assert.NotNull(afterRedo);
        Assert.True(afterRedo!.IsLocked);
        Assert.Equal("Planner hold", afterRedo.LockReason);
    }

    [Fact]
    public async Task UndoAndRedoAsync_ReversesLeafYearSplashAndRestoresAvailability()
    {
        var beforeDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        var beforeYearQuantity = beforeDepartment.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value;

        var splashResult = await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                beforeYearQuantity + 35m,
                "seasonality_profile",
                0,
                "Undo yearly quantity splash",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "planner.one",
            CancellationToken.None);

        Assert.Equal("applied", splashResult.Status);
        Assert.True(splashResult.Availability.CanUndo);

        var afterSplash = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        Assert.Equal(beforeYearQuantity + 35m, afterSplash.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);

        var undoResult = await _service.UndoAsync(1, "planner.one", CancellationToken.None);
        var afterUndo = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");

        Assert.Equal("applied", undoResult.Status);
        Assert.Equal(beforeYearQuantity, afterUndo.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.False(undoResult.Availability.CanUndo);
        Assert.True(undoResult.Availability.CanRedo);

        var redoResult = await _service.RedoAsync(1, "planner.one", CancellationToken.None);
        var afterRedo = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");

        Assert.Equal("applied", redoResult.Status);
        Assert.Equal(beforeYearQuantity + 35m, afterRedo.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.True(redoResult.Availability.CanUndo);
        Assert.False(redoResult.Availability.CanRedo);
    }

    [Fact]
    public async Task ImportWorkbookAsync_LoadsValidRowsAndReturnsExceptionWorkbookForInvalidRows()
    {
        using var workbook = new XLWorkbook();
        var storeSheet = workbook.AddWorksheet("Store B");
        WriteImportHeader(storeSheet);
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", "Vanilla", 2026, "Jan", 100m, 50m, 2m, 1.20m, 60m, 40m, 40m);
        WriteImportRow(storeSheet, 3, "Store B", "Frozen", "Gelato", "Chocolate", 2026, "Feb", 101m, 50m, 2m, 1.20m, 60m, 40m, 40m);

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var result = await _service.ImportWorkbookAsync(1, stream, "import.xlsx", "planner.one", CancellationToken.None);
        var importedClass = await _repository.FindProductNodeByPathAsync(new[] { "Store B", "Frozen", "Ice Cream" }, CancellationToken.None);
        Assert.NotNull(importedClass);
        var grid = await _service.GetGridSliceAsync(1, 102, null, new[] { importedClass!.ProductNodeId }, false, "planner.one", CancellationToken.None);

        Assert.Equal(2, result.RowsProcessed);
        Assert.NotNull(result.ExceptionWorkbookBase64);
        Assert.NotNull(result.ExceptionFileName);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store B", "Frozen", "Ice Cream", "Vanilla" }));

        using var exceptionStream = new MemoryStream(Convert.FromBase64String(result.ExceptionWorkbookBase64!));
        using var exceptionWorkbook = new XLWorkbook(exceptionStream);
        var exceptionSheet = exceptionWorkbook.Worksheet("Store B");
        Assert.Equal("Remark", exceptionSheet.Cell(1, 14).GetString());
        Assert.Equal("Expected Value", exceptionSheet.Cell(1, 15).GetString());
        Assert.Contains("Sales Revenue does not equal Sold Qty * ASP after normalization.", exceptionSheet.Cell(2, 14).GetString());
        Assert.Equal("100", exceptionSheet.Cell(2, 15).GetString());

        var mappings = await _service.GetHierarchyMappingsAsync(CancellationToken.None);
        var frozen = mappings.Departments.Single(department => department.DepartmentLabel == "Frozen");
        Assert.Contains(frozen.Classes, value => value.ClassLabel == "Ice Cream");
    }

    [Fact]
    public async Task ExportWorkbookAsync_ReturnsWorkbookMatchingImportContract()
    {
        var result = await _service.ExportWorkbookAsync(1, CancellationToken.None);

        Assert.NotEmpty(result.Content);
        Assert.EndsWith(".xlsx", result.FileName, StringComparison.OrdinalIgnoreCase);

        using var stream = new MemoryStream(result.Content);
        using var workbook = new XLWorkbook(stream);

        Assert.Contains("Store A", workbook.Worksheets.Select(sheet => sheet.Name));
        var worksheet = workbook.Worksheet("Store A");
        var headers = Enumerable.Range(1, 13)
            .Select(index => worksheet.Cell(1, index).GetString())
            .ToArray();

        Assert.Equal(
            new[]
            {
                "Store", "Department", "Class", "Subclass", "Year", "Month",
                "Sales Revenue", "Sold Qty", "ASP", "Unit Cost", "Total Costs", "GP", "GP%"
            },
            headers);

        Assert.True(worksheet.RowsUsed().Count() > 1);
        Assert.Equal("Store A", worksheet.Cell(2, 1).GetString());
        Assert.Equal("Jan", worksheet.Cell(2, 6).GetString());
    }

    [Fact]
    public async Task ExportWorkbookAsync_RoundTripsWithoutImportExceptions()
    {
        var export = await _service.ExportWorkbookAsync(1, CancellationToken.None);

        using var importStream = new MemoryStream(export.Content);
        var importResult = await _service.ImportWorkbookAsync(1, importStream, export.FileName, "planner.one", CancellationToken.None);

        Assert.Equal("applied", importResult.Status);
        Assert.Null(importResult.ExceptionWorkbookBase64);
        Assert.Null(importResult.ExceptionFileName);
    }

    [Fact]
    public async Task ImportWorkbookAsync_IgnoresOptionalRemarkAndExpectedValueColumns()
    {
        using var workbook = new XLWorkbook();
        var storeSheet = workbook.AddWorksheet("Store B");
        WriteImportHeader(storeSheet, includeRemark: true);
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", "Vanilla", 2026, "Jan", 100m, 50m, 2m, 1.20m, 60m, 40m, 40m, "ignore this", "100");

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var result = await _service.ImportWorkbookAsync(1, stream, "import.xlsx", "planner.one", CancellationToken.None);

        Assert.Null(result.ExceptionWorkbookBase64);
        Assert.Null(result.ExceptionFileName);
    }

    [Fact]
    public async Task SaveScenarioAsync_ReturnsCheckpointTimestamp()
    {
        var result = await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);
        Assert.Equal("saved", result.Status);
        Assert.Equal("manual", result.Mode);
    }

    [Fact]
    public async Task ApplyGrowthFactorAsync_OnLeafRevenue_ResetsGrowthFactorAndRollsUp()
    {
        var beforeYearRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600), CancellationToken.None);
        var beforeMonthRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603), CancellationToken.None);
        Assert.NotNull(beforeYearRevenue);
        Assert.NotNull(beforeMonthRevenue);

        var result = await _service.ApplyGrowthFactorAsync(
            new ApplyGrowthFactorRequest(
                1,
                PlanningMeasures.SalesRevenue,
                new SplashCoordinateDto(101, 2111, 202603),
                beforeMonthRevenue!.EffectiveValue,
                1.1m,
                "Leaf uplift",
                null),
            "planner.one",
            CancellationToken.None);

        Assert.NotNull(result.Patch);
        Assert.True(result.Availability.CanUndo);
        Assert.Contains(result.Patch!.Cells, cell => cell.StoreId == 101 && cell.ProductNodeId == 2111 && cell.TimePeriodId == 202603 && cell.MeasureId == PlanningMeasures.SalesRevenue);

        var updatedMonthRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603));
        var updatedYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600));
        Assert.NotNull(updatedMonthRevenue);
        Assert.NotNull(updatedYearRevenue);
        Assert.Equal(1.0m, updatedMonthRevenue!.GrowthFactor);
        Assert.True(updatedMonthRevenue.EffectiveValue > beforeMonthRevenue.EffectiveValue);
        Assert.True(updatedYearRevenue!.EffectiveValue > beforeYearRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnStoreAggregate_OnlySplashesInsideThatStoreScope()
    {
        using var workbook = new XLWorkbook();
        var storeSheet = workbook.AddWorksheet("Store B");
        WriteImportHeader(storeSheet);
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", "Vanilla", 2026, "Jan", 120m, 60m, 2m, 1.20m, 72m, 48m, 40m);
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        await _service.ImportWorkbookAsync(1, stream, "store-b.xlsx", "planner.one", CancellationToken.None);
        var metadata = await _repository.GetMetadataAsync(CancellationToken.None);
        var storeBRoot = metadata.ProductNodes.Values.Single(node => node.Level == 0 && node.Label == "Store B");
        var storeBLeaf = metadata.ProductNodes.Values.Single(node => node.StoreId == storeBRoot.StoreId && node.IsLeaf && node.Label == "Vanilla");
        var storeABefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202600), CancellationToken.None);
        var storeBLeafBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, storeBLeaf.StoreId, storeBLeaf.ProductNodeId, 202601), CancellationToken.None);
        Assert.NotNull(storeABefore);
        Assert.NotNull(storeBLeafBefore);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Store A annual plan",
                new[]
                {
                    new EditCellRequest(101, 2000, 202600, 25000m, "override", null)
                }),
            "planner.one",
            CancellationToken.None);

        var storeAAfter = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202600));
        var storeBLeafAfter = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, storeBLeaf.StoreId, storeBLeaf.ProductNodeId, 202601));

        Assert.NotNull(storeAAfter);
        Assert.NotNull(storeBLeafAfter);
        Assert.Equal(storeBLeafBefore!.EffectiveValue, storeBLeafAfter!.EffectiveValue);
        Assert.NotEqual(storeABefore!.EffectiveValue, storeAAfter!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnStoreMonthRevenue_PersistsRequestedAggregateValue()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Store A month plan",
                new[]
                {
                    new EditCellRequest(101, 2000, 202601, 12000m, "override", null)
                }),
            "planner.one",
            CancellationToken.None);

        var storeMonthRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202601));

        Assert.NotNull(storeMonthRevenue);
        Assert.Equal(12000m, storeMonthRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnClassYearRevenue_KeepsDepartmentYearTotalAlignedToChildClasses()
    {
        var softDrinksBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600), CancellationToken.None);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Class annual plan",
                new[]
                {
                    new EditCellRequest(101, 2110, 202600, 12000m, "override", null)
                }),
            "planner.one",
            CancellationToken.None);

        var departmentRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202600));
        var softDrinksRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600));
        var teaRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202600));

        Assert.NotNull(departmentRevenue);
        Assert.NotNull(softDrinksRevenue);
        Assert.NotNull(teaRevenue);
        Assert.NotNull(softDrinksBefore);
        Assert.NotEqual(softDrinksBefore!.EffectiveValue, softDrinksRevenue!.EffectiveValue);
        Assert.Equal(softDrinksRevenue!.EffectiveValue + teaRevenue!.EffectiveValue, departmentRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnLeafMonthRevenue_KeepsClassAndDepartmentMonthTotalsAligned()
    {
        var colaBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202601), CancellationToken.None);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Leaf month revenue update",
                new[]
                {
                    new EditCellRequest(101, 2111, 202601, 12345m, "input", colaBefore!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None);

        var colaRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202601));
        var sparklingFruitRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2112, 202601));
        var softDrinksRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202601));
        var teaRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202601));
        var departmentRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202601));

        Assert.NotNull(colaRevenue);
        Assert.NotNull(sparklingFruitRevenue);
        Assert.NotNull(softDrinksRevenue);
        Assert.NotNull(teaRevenue);
        Assert.NotNull(departmentRevenue);
        Assert.NotNull(colaBefore);
        Assert.NotEqual(colaBefore!.EffectiveValue, colaRevenue!.EffectiveValue);
        Assert.Equal(colaRevenue.EffectiveValue + sparklingFruitRevenue!.EffectiveValue, softDrinksRevenue!.EffectiveValue);
        Assert.Equal(softDrinksRevenue.EffectiveValue + teaRevenue!.EffectiveValue, departmentRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplySplashAsync_OnLeafYearRevenue_AcceptsServerSeasonalityProfileAndKeepsDepartmentYearTotalsAligned()
    {
        var colaYearBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600), CancellationToken.None);

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SalesRevenue,
                new SplashCoordinateDto(101, 2111, 202600),
                3900m,
                "seasonality_profile",
                0,
                "Leaf year revenue update",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "planner.one",
            CancellationToken.None);

        var colaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600));
        var colaFebRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202602));
        var sparklingFruitYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2112, 202600));
        var softDrinksYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600));
        var teaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202600));
        var departmentYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202600));

        Assert.NotNull(colaYearRevenue);
        Assert.NotNull(sparklingFruitYearRevenue);
        Assert.NotNull(softDrinksYearRevenue);
        Assert.NotNull(teaYearRevenue);
        Assert.NotNull(departmentYearRevenue);
        Assert.NotNull(colaFebRevenue);
        Assert.NotNull(colaYearBefore);
        Assert.NotEqual(colaYearBefore!.EffectiveValue, colaYearRevenue!.EffectiveValue);
        Assert.Equal(3900m, colaYearRevenue.EffectiveValue);
        Assert.Equal(750m, colaFebRevenue!.EffectiveValue);
        Assert.Equal(colaYearRevenue.EffectiveValue + sparklingFruitYearRevenue!.EffectiveValue, softDrinksYearRevenue!.EffectiveValue);
        Assert.Equal(softDrinksYearRevenue.EffectiveValue + teaYearRevenue!.EffectiveValue, departmentYearRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnLeafYearRevenue_KeepsYearRollupsAligned()
    {
        var colaYearBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600), CancellationToken.None);

        var result = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Leaf year revenue edit",
                new[]
                {
                    new EditCellRequest(101, 2111, 202600, 3900m, "override", null)
                }),
            "planner.one",
            CancellationToken.None);

        var colaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600));
        var sparklingFruitYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2112, 202600));
        var softDrinksYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600));
        var teaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202600));
        var departmentYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202600));

        Assert.NotNull(colaYearRevenue);
        Assert.NotNull(sparklingFruitYearRevenue);
        Assert.NotNull(softDrinksYearRevenue);
        Assert.NotNull(teaYearRevenue);
        Assert.NotNull(departmentYearRevenue);
        Assert.NotNull(colaYearBefore);
        Assert.NotEqual(colaYearBefore!.EffectiveValue, colaYearRevenue!.EffectiveValue);
        Assert.InRange(result.UpdatedCellCount, 1, 300);
        Assert.Equal(3900m, colaYearRevenue.EffectiveValue);
        Assert.Equal(colaYearRevenue.EffectiveValue + sparklingFruitYearRevenue!.EffectiveValue, softDrinksYearRevenue!.EffectiveValue);
        Assert.Equal(softDrinksYearRevenue.EffectiveValue + teaYearRevenue!.EffectiveValue, departmentYearRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnLeafYearRevenue_CanBeRepeatedWithoutBreakingRollups()
    {
        var firstResult = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Leaf year revenue edit 1",
                [new EditCellRequest(101, 2111, 202600, 3900m, "override", null)]),
            "planner.one",
            CancellationToken.None);
        var secondResult = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Leaf year revenue edit 2",
                [new EditCellRequest(101, 2111, 202600, 4200m, "override", null)]),
            "planner.one",
            CancellationToken.None);

        var colaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600));
        var colaFebRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202602));
        var sparklingFruitYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2112, 202600));
        var softDrinksYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600));
        var teaYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202600));
        var departmentYearRevenue = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202600));

        Assert.NotNull(colaYearRevenue);
        Assert.NotNull(colaFebRevenue);
        Assert.NotNull(sparklingFruitYearRevenue);
        Assert.NotNull(softDrinksYearRevenue);
        Assert.NotNull(teaYearRevenue);
        Assert.NotNull(departmentYearRevenue);
        Assert.True(secondResult.UpdatedCellCount > 0);
        Assert.Equal(4200m, colaYearRevenue!.EffectiveValue);
        Assert.Equal(750m, colaFebRevenue!.EffectiveValue);
        Assert.Equal(colaYearRevenue.EffectiveValue + sparklingFruitYearRevenue!.EffectiveValue, softDrinksYearRevenue!.EffectiveValue);
        Assert.Equal(softDrinksYearRevenue.EffectiveValue + teaYearRevenue!.EffectiveValue, departmentYearRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnLeafYearQuantity_CanBeRepeatedWithoutBreakingRollups()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit 1",
                [new EditCellRequest(101, 2111, 202600, 390m, "override", null)]),
            "planner.one",
            CancellationToken.None);
        var secondResult = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit 2",
                [new EditCellRequest(101, 2111, 202600, 420m, "override", null)]),
            "planner.one",
            CancellationToken.None);

        var colaYearQuantity = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2111, 202600));
        var sparklingFruitYearQuantity = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2112, 202600));
        var softDrinksYearQuantity = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2110, 202600));
        var teaYearQuantity = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2120, 202600));
        var departmentYearQuantity = await GetEffectiveCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2100, 202600));

        Assert.NotNull(colaYearQuantity);
        Assert.NotNull(sparklingFruitYearQuantity);
        Assert.NotNull(softDrinksYearQuantity);
        Assert.NotNull(teaYearQuantity);
        Assert.NotNull(departmentYearQuantity);
        Assert.True(secondResult.UpdatedCellCount > 0);
        Assert.Equal(420m, colaYearQuantity!.EffectiveValue);
        Assert.Equal(colaYearQuantity.EffectiveValue + sparklingFruitYearQuantity!.EffectiveValue, softDrinksYearQuantity!.EffectiveValue);
        Assert.Equal(softDrinksYearQuantity.EffectiveValue + teaYearQuantity!.EffectiveValue, departmentYearQuantity!.EffectiveValue);
    }

    [Fact]
    public async Task SaveScenarioAsync_AfterLeafYearQuantitySplash_DepartmentProjectionShowsCommittedLeafValueAcrossUserAliases()
    {
        const string legacyUserId = "11111111-2222-3333-4444-555555555555";
        var aliasedUserToken = PlanningUserIdentity.SerializePlanningUserContext(
            PlanningUserIdentity.CreatePlanningUserContext("planner.one@example.com", "planner.one@example.com", legacyUserId));

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                390m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            legacyUserId,
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), aliasedUserToken, CancellationToken.None);

        var request = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var departmentRows = await _service.GetGridViewChildrenAsync(request, "view:department:root", aliasedUserToken, CancellationToken.None);
        var beveragesRow = Assert.Single(departmentRows.Rows, row => row.Label == "Beverages");

        var storeRows = await _service.GetGridViewChildrenAsync(request, beveragesRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");

        var classRows = await _service.GetGridViewChildrenAsync(request, storeRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");

        var subclassRows = await _service.GetGridViewChildrenAsync(request, classRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");

        Assert.Equal(390m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Fact]
    public async Task ApplySplashAsync_AfterAliasSave_CanBeAppliedAgainAndRemainsVisibleAcrossViews()
    {
        const string legacyUserId = "11111111-2222-3333-4444-555555555555";
        var aliasedUserToken = PlanningUserIdentity.SerializePlanningUserContext(
            PlanningUserIdentity.CreatePlanningUserContext("planner.one@example.com", "planner.one@example.com", legacyUserId));

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                390m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash 1",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            legacyUserId,
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), aliasedUserToken, CancellationToken.None);

        var secondResult = await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                420m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash 2",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            aliasedUserToken,
            CancellationToken.None);

        var storeRequest = new PlanningGridViewRequest(1, "store", null, null, null, false);
        var storeRows = await _service.GetGridViewChildrenAsync(storeRequest, "view:store:root", aliasedUserToken, CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");

        var departmentRows = await _service.GetGridViewChildrenAsync(storeRequest, storeRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var departmentRow = Assert.Single(departmentRows.Rows, row => row.Label == "Beverages");

        var classRows = await _service.GetGridViewChildrenAsync(storeRequest, departmentRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");

        var subclassRows = await _service.GetGridViewChildrenAsync(storeRequest, classRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");

        var departmentRequest = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var departmentRootRows = await _service.GetGridViewChildrenAsync(departmentRequest, "view:department:root", aliasedUserToken, CancellationToken.None);
        var beveragesRow = Assert.Single(departmentRootRows.Rows, row => row.Label == "Beverages");
        var departmentStoreRows = await _service.GetGridViewChildrenAsync(departmentRequest, beveragesRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var departmentStoreRow = Assert.Single(departmentStoreRows.Rows, row => row.Label == "Store A");
        var departmentClassRows = await _service.GetGridViewChildrenAsync(departmentRequest, departmentStoreRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var departmentClassRow = Assert.Single(departmentClassRows.Rows, row => row.Label == "Soft Drinks");
        var departmentSubclassRows = await _service.GetGridViewChildrenAsync(departmentRequest, departmentClassRow.ViewRowId!, aliasedUserToken, CancellationToken.None);
        var departmentSubclassRow = Assert.Single(departmentSubclassRows.Rows, row => row.Label == "Cola");

        Assert.True(secondResult.CellsUpdated > 0);
        Assert.Equal(420m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.Equal(420m, departmentSubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Fact]
    public async Task SaveScenarioAsync_WhenPreferredUsernameDisappearsAcrossRequests_RemainsVisibleAcrossViewsAndEditableAgain()
    {
        var initialPrincipal = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim("preferred_username", "planner.one@example.com"),
            new Claim("oid", "11111111-2222-3333-4444-555555555555"),
            new Claim("name", "Planner One"),
        ], "Bearer"));
        var followUpPrincipal = new ClaimsPrincipal(new ClaimsIdentity(
        [
            new Claim("oid", "11111111-2222-3333-4444-555555555555"),
            new Claim("name", "Planner One"),
        ], "Bearer"));

        var initialToken = PlanningUserIdentity.ResolvePlanningUserToken(initialPrincipal, authEnabled: true);
        var followUpToken = PlanningUserIdentity.ResolvePlanningUserToken(followUpPrincipal, authEnabled: true);

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                390m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash 1",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            initialToken,
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), followUpToken, CancellationToken.None);

        var afterSaveDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola", "Beverages", followUpToken);
        var afterSaveStore = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola", followUpToken);

        Assert.Equal(390m, afterSaveDepartment.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.Equal(390m, afterSaveStore.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                420m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash 2",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            followUpToken,
            CancellationToken.None);

        var afterSecondDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola", "Beverages", followUpToken);
        var afterSecondStore = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola", followUpToken);

        Assert.Equal(420m, afterSecondDepartment.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.Equal(420m, afterSecondStore.SubclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Fact]
    public async Task ApplySplashAsync_OnLeafYearQuantity_RollsUpDepartmentAncestorsAcrossViews()
    {
        var departmentRequest = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var beforeDepartmentRows = await _service.GetGridViewChildrenAsync(departmentRequest, "view:department:root", "planner.one", CancellationToken.None);
        var beforeBeveragesRow = Assert.Single(beforeDepartmentRows.Rows, row => row.Label == "Beverages");
        var beforeStoreRows = await _service.GetGridViewChildrenAsync(departmentRequest, beforeBeveragesRow.ViewRowId!, "planner.one", CancellationToken.None);
        var beforeStoreRow = Assert.Single(beforeStoreRows.Rows, row => row.Label == "Store A");

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.SoldQuantity,
                new SplashCoordinateDto(101, 2111, 202600),
                390m,
                "seasonality_profile",
                0,
                "Leaf year quantity splash",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "planner.one",
            CancellationToken.None);

        var afterDepartmentRows = await _service.GetGridViewChildrenAsync(departmentRequest, "view:department:root", "planner.one", CancellationToken.None);
        var afterBeveragesRow = Assert.Single(afterDepartmentRows.Rows, row => row.Label == "Beverages");
        var afterStoreRows = await _service.GetGridViewChildrenAsync(departmentRequest, afterBeveragesRow.ViewRowId!, "planner.one", CancellationToken.None);
        var afterStoreRow = Assert.Single(afterStoreRows.Rows, row => row.Label == "Store A");
        var classRows = await _service.GetGridViewChildrenAsync(departmentRequest, afterStoreRow.ViewRowId!, "planner.one", CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");
        var subclassRows = await _service.GetGridViewChildrenAsync(departmentRequest, classRow.ViewRowId!, "planner.one", CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");

        Assert.Equal(390m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.NotEqual(beforeStoreRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value, afterStoreRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.NotEqual(beforeBeveragesRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value, afterBeveragesRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Fact]
    public async Task ApplySplashAsync_OnLeafYearGrossProfitPercent_SaveAndRepeatedEditRemainConsistentAcrossViews()
    {
        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.GrossProfitPercent,
                new SplashCoordinateDto(101, 2111, 202600),
                28.5m,
                "seasonality_profile",
                1,
                "Leaf year GP% splash 1",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "planner.one",
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);

        var afterFirstDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        var afterFirstStore = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola");

        Assert.Equal(28.5m, afterFirstDepartment.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);
        Assert.Equal(28.5m, afterFirstStore.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);

        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.GrossProfitPercent,
                new SplashCoordinateDto(101, 2111, 202600),
                31.0m,
                "seasonality_profile",
                1,
                "Leaf year GP% splash 2",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "planner.one",
            CancellationToken.None);

        var afterSecondDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        var afterSecondStore = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola");

        Assert.Equal(31.0m, afterSecondDepartment.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);
        Assert.Equal(31.0m, afterSecondStore.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);
    }

    [Fact]
    public async Task ApplySplashAsync_OnLeafYearGrossProfitPercent_WithAllDepartmentsScope_RemainsConsistentAcrossViews()
    {
        await _service.ApplySplashAsync(
            new SplashRequest(
                1,
                PlanningMeasures.GrossProfitPercent,
                new SplashCoordinateDto(101, 2111, 202600),
                28.5m,
                "seasonality_profile",
                1,
                "Leaf year GP% all departments splash",
                null,
                [new SplashScopeRootDto(101, 2111)]),
            "local.test.user",
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "local.test.user", CancellationToken.None);

        var departmentRows = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola", null, "local.test.user");
        var storeRows = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola", "local.test.user");

        Assert.Equal(28.5m, departmentRows.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);
        Assert.Equal(28.5m, storeRows.SubclassRow.Cells[202600].Measures[PlanningMeasures.GrossProfitPercent].Value);
    }

    [Fact]
    public async Task SaveScenarioAsync_AfterLeafYearQuantityEdit_DepartmentProjectionShowsCommittedLeafValue()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit",
                [new EditCellRequest(101, 2111, 202600, 390m, "override", null)]),
            "planner.one",
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);

        var request = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var departmentRows = await _service.GetGridViewChildrenAsync(request, "view:department:root", "planner.one", CancellationToken.None);
        var beveragesRow = Assert.Single(departmentRows.Rows, row => row.Label == "Beverages");

        var storeRows = await _service.GetGridViewChildrenAsync(request, beveragesRow.ViewRowId!, "planner.one", CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");

        var classRows = await _service.GetGridViewChildrenAsync(request, storeRow.ViewRowId!, "planner.one", CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");

        var subclassRows = await _service.GetGridViewChildrenAsync(request, classRow.ViewRowId!, "planner.one", CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");

        Assert.Equal(390m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Fact]
    public async Task SaveScenarioAsync_AfterLeafYearQuantityEdit_CanBeEditedAgainAndRemainsVisibleAcrossViews()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit 1",
                [new EditCellRequest(101, 2111, 202600, 390m, "override", null)]),
            "planner.one",
            CancellationToken.None);
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);

        var departmentRequest = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var departmentRows = await _service.GetGridViewChildrenAsync(departmentRequest, "view:department:root", "planner.one", CancellationToken.None);
        var beveragesRow = Assert.Single(departmentRows.Rows, row => row.Label == "Beverages");

        var storeRows = await _service.GetGridViewChildrenAsync(departmentRequest, beveragesRow.ViewRowId!, "planner.one", CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");

        var classRows = await _service.GetGridViewChildrenAsync(departmentRequest, storeRow.ViewRowId!, "planner.one", CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");

        var subclassRows = await _service.GetGridViewChildrenAsync(departmentRequest, classRow.ViewRowId!, "planner.one", CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");
        Assert.Equal(390m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);

        var secondResult = await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit 2",
                [new EditCellRequest(101, 2111, 202600, 420m, "override", null)]),
            "planner.one",
            CancellationToken.None);

        var storeRequest = new PlanningGridViewRequest(1, "store", null, null, null, false);
        var storeRowsAfterSecondEdit = await _service.GetGridViewChildrenAsync(storeRequest, "view:store:root", "planner.one", CancellationToken.None);
        var storeRowAfterSecondEdit = Assert.Single(storeRowsAfterSecondEdit.Rows, row => row.Label == "Store A");

        var departmentRowsAfterSecondEdit = await _service.GetGridViewChildrenAsync(storeRequest, storeRowAfterSecondEdit.ViewRowId!, "planner.one", CancellationToken.None);
        var departmentRow = Assert.Single(departmentRowsAfterSecondEdit.Rows, row => row.Label == "Beverages");

        var classRowsAfterSecondEdit = await _service.GetGridViewChildrenAsync(storeRequest, departmentRow.ViewRowId!, "planner.one", CancellationToken.None);
        var classRowAfterSecondEdit = Assert.Single(classRowsAfterSecondEdit.Rows, row => row.Label == "Soft Drinks");

        var subclassRowsAfterSecondEdit = await _service.GetGridViewChildrenAsync(storeRequest, classRowAfterSecondEdit.ViewRowId!, "planner.one", CancellationToken.None);
        var subclassRowAfterSecondEdit = Assert.Single(subclassRowsAfterSecondEdit.Rows, row => row.Label == "Cola");
        Assert.Equal(420m, subclassRowAfterSecondEdit.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
        Assert.True(secondResult.UpdatedCellCount > 0);
    }

    [Fact]
    public async Task GetGridViewChildrenAsync_AfterUnsavedLeafYearQuantityEdit_DepartmentProjectionShowsDraftLeafValue()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Leaf year quantity edit",
                [new EditCellRequest(101, 2111, 202600, 390m, "override", null)]),
            "planner.one",
            CancellationToken.None);

        var request = new PlanningGridViewRequest(1, "department", null, "Beverages", "department-store-class", false);
        var departmentRows = await _service.GetGridViewChildrenAsync(request, "view:department:root", "planner.one", CancellationToken.None);
        var beveragesRow = Assert.Single(departmentRows.Rows, row => row.Label == "Beverages");

        var storeRows = await _service.GetGridViewChildrenAsync(request, beveragesRow.ViewRowId!, "planner.one", CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");

        var classRows = await _service.GetGridViewChildrenAsync(request, storeRow.ViewRowId!, "planner.one", CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == "Soft Drinks");

        var subclassRows = await _service.GetGridViewChildrenAsync(request, classRow.ViewRowId!, "planner.one", CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == "Cola");

        Assert.Equal(390m, subclassRow.Cells[202600].Measures[PlanningMeasures.SoldQuantity].Value);
    }

    [Theory]
    [InlineData(PlanningMeasures.SalesRevenue, 3900, 4200)]
    [InlineData(PlanningMeasures.SoldQuantity, 390, 420)]
    [InlineData(PlanningMeasures.AverageSellingPrice, 12.25, 13.75)]
    [InlineData(PlanningMeasures.UnitCost, 7.25, 7.75)]
    [InlineData(PlanningMeasures.GrossProfitPercent, 28.5, 31.0)]
    public async Task SaveScenarioAsync_YearLeafEditsRemainConsistentAcrossStoreAndDepartmentViews_ForEveryEditableMeasure(
        long measureId,
        decimal firstValue,
        decimal secondValue)
    {
        var beforeDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        var beforeStoreValue = beforeDepartment.StoreRow.Cells[202600].Measures[measureId].Value;
        var beforeDepartmentValue = beforeDepartment.DepartmentRow.Cells[202600].Measures[measureId].Value;

        await ApplyLeafChangeAsync(measureId, 2111, 202600, firstValue, $"Year edit 1 for measure {measureId}");
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);

        var afterFirstDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        Assert.Equal(firstValue, afterFirstDepartment.SubclassRow.Cells[202600].Measures[measureId].Value);
        Assert.NotEqual(beforeStoreValue, afterFirstDepartment.StoreRow.Cells[202600].Measures[measureId].Value);
        Assert.NotEqual(beforeDepartmentValue, afterFirstDepartment.DepartmentRow.Cells[202600].Measures[measureId].Value);

        await ApplyLeafChangeAsync(measureId, 2111, 202600, secondValue, $"Year edit 2 for measure {measureId}");

        var afterSecondDepartment = await GetDepartmentPathRowsAsync("Beverages", "Soft Drinks", "Cola");
        var afterSecondStore = await GetStorePathRowsAsync("Beverages", "Soft Drinks", "Cola");

        Assert.Equal(secondValue, afterSecondDepartment.SubclassRow.Cells[202600].Measures[measureId].Value);
        Assert.Equal(secondValue, afterSecondStore.SubclassRow.Cells[202600].Measures[measureId].Value);
        Assert.NotEqual(afterFirstDepartment.StoreRow.Cells[202600].Measures[measureId].Value, afterSecondDepartment.StoreRow.Cells[202600].Measures[measureId].Value);
        Assert.NotEqual(afterFirstDepartment.DepartmentRow.Cells[202600].Measures[measureId].Value, afterSecondDepartment.DepartmentRow.Cells[202600].Measures[measureId].Value);
    }

    [Theory]
    [InlineData(PlanningMeasures.SalesRevenue, 1350, 1425)]
    [InlineData(PlanningMeasures.SoldQuantity, 125, 145)]
    [InlineData(PlanningMeasures.AverageSellingPrice, 11.75, 12.5)]
    [InlineData(PlanningMeasures.UnitCost, 7.1, 7.6)]
    [InlineData(PlanningMeasures.GrossProfitPercent, 26.5, 29.0)]
    public async Task SaveScenarioAsync_MonthLeafEditsRemainConsistentAcrossStoreAndDepartmentViews_ForEveryEditableMeasure(
        long measureId,
        decimal firstValue,
        decimal secondValue)
    {
        var beforeDepartment = await GetDepartmentPathRowsAsync("Beverages", "Tea", "Green Tea");
        var beforeStoreValue = beforeDepartment.StoreRow.Cells[202603].Measures[measureId].Value;
        var beforeDepartmentValue = beforeDepartment.DepartmentRow.Cells[202603].Measures[measureId].Value;

        await ApplyLeafChangeAsync(measureId, 2121, 202603, firstValue, $"Month edit 1 for measure {measureId}");
        await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);

        var afterFirstDepartment = await GetDepartmentPathRowsAsync("Beverages", "Tea", "Green Tea");
        Assert.Equal(firstValue, afterFirstDepartment.SubclassRow.Cells[202603].Measures[measureId].Value);
        Assert.NotEqual(beforeStoreValue, afterFirstDepartment.StoreRow.Cells[202603].Measures[measureId].Value);
        Assert.NotEqual(beforeDepartmentValue, afterFirstDepartment.DepartmentRow.Cells[202603].Measures[measureId].Value);

        await ApplyLeafChangeAsync(measureId, 2121, 202603, secondValue, $"Month edit 2 for measure {measureId}");

        var afterSecondDepartment = await GetDepartmentPathRowsAsync("Beverages", "Tea", "Green Tea");
        var afterSecondStore = await GetStorePathRowsAsync("Beverages", "Tea", "Green Tea");

        Assert.Equal(secondValue, afterSecondDepartment.SubclassRow.Cells[202603].Measures[measureId].Value);
        Assert.Equal(secondValue, afterSecondStore.SubclassRow.Cells[202603].Measures[measureId].Value);
        Assert.NotEqual(afterFirstDepartment.StoreRow.Cells[202603].Measures[measureId].Value, afterSecondDepartment.StoreRow.Cells[202603].Measures[measureId].Value);
        Assert.NotEqual(afterFirstDepartment.DepartmentRow.Cells[202603].Measures[measureId].Value, afterSecondDepartment.DepartmentRow.Cells[202603].Measures[measureId].Value);
    }

    [Fact]
    public async Task GenerateNextYearAsync_CopiesEditableInputsAndRecomputesCalculatedMeasures()
    {
        var result = await _service.GenerateNextYearAsync(new GenerateNextYearRequest(1, 202600), "planner.one", CancellationToken.None);
        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202703), CancellationToken.None);
        var quantityCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2111, 202703), CancellationToken.None);
        var gpPercentCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, 101, 2111, 202703), CancellationToken.None);

        Assert.Equal(202700, result.GeneratedYearTimePeriodId);
        Assert.True(result.CellsCopied > 0);
        Assert.NotNull(revenueCell);
        Assert.NotNull(quantityCell);
        Assert.NotNull(gpPercentCell);
        Assert.True(revenueCell!.InputValue is not null);
        Assert.True(quantityCell!.InputValue is not null);
        Assert.Null(gpPercentCell!.InputValue);
        Assert.True(gpPercentCell.EffectiveValue >= 0m);
    }

    [Fact]
    public async Task AddRowAsync_DepartmentAndClassUpdateHierarchyMappings()
    {
        var department = await _service.AddRowAsync(
            new AddRowRequest(1, "department", 2000, "Frozen", null),
            CancellationToken.None);

        await _service.AddRowAsync(
            new AddRowRequest(1, "class", department.ProductNodeId, "Ice Cream", null),
            CancellationToken.None);

        var mappings = await _service.GetHierarchyMappingsAsync(CancellationToken.None);
        var frozen = mappings.Departments.Single(departmentMapping => departmentMapping.DepartmentLabel == "Frozen");
        Assert.Contains(frozen.Classes, value => value.ClassLabel == "Ice Cream");
    }

    [Fact]
    public async Task DeleteYearAsync_RemovesYearFromGrid()
    {
        var result = await _service.DeleteYearAsync(new DeleteYearRequest(1, 202700), CancellationToken.None);
        var grid = await _service.GetGridSliceAsync(1, null, null, null, false, "planner.one", CancellationToken.None);

        Assert.True(result.DeletedCellCount > 0);
        Assert.DoesNotContain(grid.Periods, period => period.TimePeriodId == 202700 || period.ParentTimePeriodId == 202700);
    }

    [Fact]
    public async Task StoreProfileImportExport_RoundTripsBranchProfileShape()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Store Profile");
        WriteStoreProfileHeader(sheet);
        WriteStoreProfileRow(sheet, 2, "MKDW", "KL Downtown", "Kuala Lumpur", "Baby Centre", 3.121m, 101.612m, "Central 1", "2024-01-15", "Organic", "Store", "Active", "2", "Mall", 14850m, 7619m, "Alice", "Ben", 120000m, "active", "new-store-ramp", true);

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var importResult = await _service.ImportStoreProfilesAsync(stream, "branch-profile.xlsx", CancellationToken.None);
        var response = await _service.GetStoreProfilesAsync(CancellationToken.None);
        var imported = response.Stores.Single(store => store.StoreCode == "MKDW");

        Assert.Equal("applied", importResult.Status);
        Assert.Equal(1, importResult.RowsProcessed);
        Assert.Equal("KL Downtown", imported.BranchName);
        Assert.Equal("Baby Centre", imported.ClusterLabel);
        Assert.Equal("Central 1", imported.RegionLabel);

        var export = await _service.ExportStoreProfilesAsync(CancellationToken.None);
        using var exportStream = new MemoryStream(export.Content);
        using var exportedWorkbook = new XLWorkbook(exportStream);
        var exportSheet = exportedWorkbook.Worksheet("Store Profile");
        Assert.Equal("CompCode", exportSheet.Cell(1, 1).GetString());
        Assert.Equal("Branch Type", exportSheet.Cell(1, 4).GetString());
        Assert.Equal("Region", exportSheet.Cell(1, 7).GetString());
        Assert.Equal("MKDW", exportSheet.Cell(2, 1).GetString());
    }

    [Fact]
    public async Task UpsertStoreProfileOptionAsync_MaintainsEnumeratedValues()
    {
        var result = await _service.UpsertStoreProfileOptionAsync(
            new UpsertStoreProfileOptionRequest("clusterLabel", "Baby Mall", true),
            CancellationToken.None);

        Assert.Contains(result.Options, option => option.FieldName == "clusterLabel" && option.Value == "Baby Mall" && option.IsActive);
    }

    [Fact]
    public async Task InventoryProfileImportExport_RoundTripsWorkbookShape()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Inventory Profile");
        WriteInventoryProfileHeader(sheet);
        WriteRow(sheet, 2, "MKDW", "SKU-001", "125", "12", "5", "108", "20", "6.5", "55", "Active");

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var importResult = await _service.ImportInventoryProfilesAsync(stream, "inventory-profile.xlsx", CancellationToken.None);
        var response = await _service.GetInventoryProfilesAsync(null, 1, 50, CancellationToken.None);
        var imported = response.Profiles.Single(profile => profile.StoreCode == "MKDW" && profile.ProductCode == "SKU-001");

        Assert.Equal("applied", importResult.Status);
        Assert.Equal(1, importResult.RowsProcessed);
        Assert.Equal(125m, imported.StartingInventory);
        Assert.Equal(108m, imported.ProjectedStockOnHand);

        var export = await _service.ExportInventoryProfilesAsync(CancellationToken.None);
        using var exportStream = new MemoryStream(export.Content);
        using var exportedWorkbook = new XLWorkbook(exportStream);
        var exportSheet = exportedWorkbook.Worksheet("Inventory Profile");
        Assert.Equal("CompCode", exportSheet.Cell(1, 1).GetString());
        Assert.Equal("Product Code", exportSheet.Cell(1, 2).GetString());
        Assert.Equal("MKDW", exportSheet.Cell(2, 1).GetString());
        Assert.Equal("SKU-001", exportSheet.Cell(2, 2).GetString());
    }

    [Fact]
    public async Task PricingPolicyImportExport_RoundTripsWorkbookShape()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Pricing Policy");
        WritePricingPolicyHeader(sheet);
        WriteRow(sheet, 2, "Feeding", "Bottles", "Glass Bottles", "Tommee Tippee", "Premium", "29.90", "49.90", "24.90", "35", "true", "true", "Active");

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var importResult = await _service.ImportPricingPoliciesAsync(stream, "pricing-policy.xlsx", CancellationToken.None);
        var response = await _service.GetPricingPoliciesAsync(null, 1, 50, CancellationToken.None);
        var imported = response.Policies.Single(policy => policy.Department == "Feeding" && policy.Class == "Bottles" && policy.Subclass == "Glass Bottles");

        Assert.Equal("applied", importResult.Status);
        Assert.Equal(1, importResult.RowsProcessed);
        Assert.Equal(29.90m, imported.MinPrice);
        Assert.True(imported.KviFlag);

        var export = await _service.ExportPricingPoliciesAsync(CancellationToken.None);
        using var exportStream = new MemoryStream(export.Content);
        using var exportedWorkbook = new XLWorkbook(exportStream);
        var exportSheet = exportedWorkbook.Worksheet("Pricing Policy");
        Assert.Equal("Department", exportSheet.Cell(1, 1).GetString());
        Assert.Equal("Price Ladder Group", exportSheet.Cell(1, 5).GetString());
        Assert.Equal("Feeding", exportSheet.Cell(2, 1).GetString());
    }

    [Fact]
    public async Task SeasonalityEventImportExport_RoundTripsWorkbookShape()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Seasonality & Events");
        WriteSeasonalityEventHeader(sheet);
        WriteRow(sheet, 2, "Feeding", "Bottles", "Glass Bottles", "RAMADHAN", "MEGA-SALE", "3", "1.35", "Mar Wk1-Wk2", "true", "Active");

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var importResult = await _service.ImportSeasonalityEventProfilesAsync(stream, "seasonality-events.xlsx", CancellationToken.None);
        var response = await _service.GetSeasonalityEventProfilesAsync(null, 1, 50, CancellationToken.None);
        var imported = response.Profiles.Single(profile =>
            profile.Department == "Feeding"
            && profile.Class == "Bottles"
            && profile.Subclass == "Glass Bottles"
            && profile.Month == 3);

        Assert.Equal("applied", importResult.Status);
        Assert.Equal(1, importResult.RowsProcessed);
        Assert.Equal(1.35m, imported.Weight);
        Assert.True(imported.PeakFlag);

        var export = await _service.ExportSeasonalityEventProfilesAsync(CancellationToken.None);
        using var exportStream = new MemoryStream(export.Content);
        using var exportedWorkbook = new XLWorkbook(exportStream);
        var exportSheet = exportedWorkbook.Worksheet("Seasonality & Events");
        Assert.Equal("Season Code", exportSheet.Cell(1, 4).GetString());
        Assert.Equal("MEGA-SALE", exportSheet.Cell(2, 5).GetString());
    }

    [Fact]
    public async Task VendorSupplyImportExport_RoundTripsWorkbookShape()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Vendor Supply Profile");
        WriteVendorSupplyHeader(sheet);
        WriteRow(sheet, 2, "Pigeon", "Pigeon", "45", "100", "12", "Auto Replenish", "Net 30", "Active");

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var importResult = await _service.ImportVendorSupplyProfilesAsync(stream, "vendor-supply.xlsx", CancellationToken.None);
        var response = await _service.GetVendorSupplyProfilesAsync(null, 1, 50, CancellationToken.None);
        var imported = response.Profiles.Single(profile => profile.Supplier == "Pigeon" && profile.Brand == "Pigeon");

        Assert.Equal("applied", importResult.Status);
        Assert.Equal(1, importResult.RowsProcessed);
        Assert.Equal(45, imported.LeadTimeDays);
        Assert.Equal("Auto Replenish", imported.ReplenishmentType);

        var export = await _service.ExportVendorSupplyProfilesAsync(CancellationToken.None);
        using var exportStream = new MemoryStream(export.Content);
        using var exportedWorkbook = new XLWorkbook(exportStream);
        var exportSheet = exportedWorkbook.Worksheet("Vendor Supply Profile");
        Assert.Equal("Supplier", exportSheet.Cell(1, 1).GetString());
        Assert.Equal("Payment Terms", exportSheet.Cell(1, 7).GetString());
        Assert.Equal("Pigeon", exportSheet.Cell(2, 1).GetString());
    }

    private static void WriteImportHeader(IXLWorksheet sheet, bool includeRemark = false)
    {
        sheet.Cell(1, 1).Value = "Store";
        sheet.Cell(1, 2).Value = "Department";
        sheet.Cell(1, 3).Value = "Class";
        sheet.Cell(1, 4).Value = "Subclass";
        sheet.Cell(1, 5).Value = "Year";
        sheet.Cell(1, 6).Value = "Month";
        sheet.Cell(1, 7).Value = "Sales Revenue";
        sheet.Cell(1, 8).Value = "Sold Qty";
        sheet.Cell(1, 9).Value = "ASP";
        sheet.Cell(1, 10).Value = "Unit Cost";
        sheet.Cell(1, 11).Value = "Total Costs";
        sheet.Cell(1, 12).Value = "GP";
        sheet.Cell(1, 13).Value = "GP%";
        if (includeRemark)
        {
            sheet.Cell(1, 14).Value = "Remark";
            sheet.Cell(1, 15).Value = "Expected Value";
        }
    }

    private static void WriteImportRow(
        IXLWorksheet sheet,
        int rowIndex,
        string store,
        string department,
        string @class,
        string subclass,
        int year,
        string month,
        decimal revenue,
        decimal quantity,
        decimal asp,
        decimal unitCost,
        decimal totalCosts,
        decimal gp,
        decimal gpPercent,
        string? remark = null,
        string? expectedValue = null)
    {
        sheet.Cell(rowIndex, 1).Value = store;
        sheet.Cell(rowIndex, 2).Value = department;
        sheet.Cell(rowIndex, 3).Value = @class;
        sheet.Cell(rowIndex, 4).Value = subclass;
        sheet.Cell(rowIndex, 5).Value = year;
        sheet.Cell(rowIndex, 6).Value = month;
        sheet.Cell(rowIndex, 7).Value = revenue;
        sheet.Cell(rowIndex, 8).Value = quantity;
        sheet.Cell(rowIndex, 9).Value = asp;
        sheet.Cell(rowIndex, 10).Value = unitCost;
        sheet.Cell(rowIndex, 11).Value = totalCosts;
        sheet.Cell(rowIndex, 12).Value = gp;
        sheet.Cell(rowIndex, 13).Value = gpPercent;
        if (remark is not null)
        {
            sheet.Cell(rowIndex, 14).Value = remark;
        }

        if (expectedValue is not null)
        {
            sheet.Cell(rowIndex, 15).Value = expectedValue;
        }
    }

    private static void WriteStoreProfileHeader(IXLWorksheet sheet)
    {
        var headers = new[]
        {
            "CompCode", "BranchName", "State", "Branch Type", "Latitude", "Longitude", "Region", "Opening Date",
            "SSSG", "Sales Type", "Status", "Storey", "Building Status", "GTA", "NTA", "RSOM", "DM", "Rental",
            "Lifecycle State", "Ramp Profile", "Active"
        };

        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteStoreProfileRow(
        IXLWorksheet sheet,
        int rowIndex,
        string compCode,
        string branchName,
        string state,
        string branchType,
        decimal latitude,
        decimal longitude,
        string region,
        string openingDate,
        string sssg,
        string salesType,
        string status,
        string storey,
        string buildingStatus,
        decimal gta,
        decimal nta,
        string rsom,
        string dm,
        decimal rental,
        string lifecycleState,
        string rampProfile,
        bool active)
    {
        var values = new object[]
        {
            compCode, branchName, state, branchType, latitude, longitude, region, openingDate, sssg, salesType, status, storey,
            buildingStatus, gta, nta, rsom, dm, rental, lifecycleState, rampProfile, active ? "true" : "false"
        };

        for (var index = 0; index < values.Length; index += 1)
        {
            sheet.Cell(rowIndex, index + 1).Value = values[index].ToString();
        }
    }

    private static void WriteInventoryProfileHeader(IXLWorksheet sheet) =>
        WriteHeaderRow(sheet, "CompCode", "Product Code", "Starting Inventory", "Inbound Qty", "Reserved Qty", "Projected Stock On Hand", "Safety Stock", "Weeks Of Cover Target", "Sell Through Target Pct", "Status");

    private static void WritePricingPolicyHeader(IXLWorksheet sheet) =>
        WriteHeaderRow(sheet, "Department", "Class", "Subclass", "Brand", "Price Ladder Group", "Min Price", "Max Price", "Markdown Floor Price", "Minimum Margin Pct", "KVI Flag", "Markdown Eligible", "Status");

    private static void WriteSeasonalityEventHeader(IXLWorksheet sheet) =>
        WriteHeaderRow(sheet, "Department", "Class", "Subclass", "Season Code", "Event Code", "Month", "Weight", "Promo Window", "Peak Flag", "Status");

    private static void WriteVendorSupplyHeader(IXLWorksheet sheet) =>
        WriteHeaderRow(sheet, "Supplier", "Brand", "Lead Time Days", "MOQ", "Case Pack", "Replenishment Type", "Payment Terms", "Status");

    private async Task ApplyLeafChangeAsync(
        long measureId,
        long productNodeId,
        long timePeriodId,
        decimal newValue,
        string comment)
    {
        var coordinate = new PlanningCellCoordinate(1, measureId, 101, productNodeId, timePeriodId);
        var existingCell = await GetEffectiveCellAsync(coordinate);
        Assert.NotNull(existingCell);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                measureId,
                comment,
                [
                    new EditCellRequest(
                        101,
                        productNodeId,
                        timePeriodId,
                        newValue,
                        timePeriodId % 100 == 0 ? "override" : "input",
                        existingCell!.RowVersion)
                ]),
            "planner.one",
            CancellationToken.None);
    }

    private async Task<(GridRowDto DepartmentRow, GridRowDto StoreRow, GridRowDto ClassRow, GridRowDto SubclassRow)> GetDepartmentPathRowsAsync(
        string departmentLabel,
        string classLabel,
        string subclassLabel,
        string? selectedDepartmentLabel = null,
        string userId = "planner.one")
    {
        var request = new PlanningGridViewRequest(1, "department", null, selectedDepartmentLabel, "department-store-class", false);
        var departmentRows = await _service.GetGridViewChildrenAsync(request, "view:department:root", userId, CancellationToken.None);
        var departmentRow = Assert.Single(departmentRows.Rows, row => row.Label == departmentLabel);
        var storeRows = await _service.GetGridViewChildrenAsync(request, departmentRow.ViewRowId!, userId, CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");
        var classRows = await _service.GetGridViewChildrenAsync(request, storeRow.ViewRowId!, userId, CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == classLabel);
        var subclassRows = await _service.GetGridViewChildrenAsync(request, classRow.ViewRowId!, userId, CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == subclassLabel);
        return (departmentRow, storeRow, classRow, subclassRow);
    }

    private async Task<(GridRowDto StoreRow, GridRowDto DepartmentRow, GridRowDto ClassRow, GridRowDto SubclassRow)> GetStorePathRowsAsync(
        string departmentLabel,
        string classLabel,
        string subclassLabel,
        string userId = "planner.one")
    {
        var request = new PlanningGridViewRequest(1, "store", null, null, null, false);
        var storeRows = await _service.GetGridViewChildrenAsync(request, "view:store:root", userId, CancellationToken.None);
        var storeRow = Assert.Single(storeRows.Rows, row => row.Label == "Store A");
        var departmentRows = await _service.GetGridViewChildrenAsync(request, storeRow.ViewRowId!, userId, CancellationToken.None);
        var departmentRow = Assert.Single(departmentRows.Rows, row => row.Label == departmentLabel);
        var classRows = await _service.GetGridViewChildrenAsync(request, departmentRow.ViewRowId!, userId, CancellationToken.None);
        var classRow = Assert.Single(classRows.Rows, row => row.Label == classLabel);
        var subclassRows = await _service.GetGridViewChildrenAsync(request, classRow.ViewRowId!, userId, CancellationToken.None);
        var subclassRow = Assert.Single(subclassRows.Rows, row => row.Label == subclassLabel);
        return (storeRow, departmentRow, classRow, subclassRow);
    }

    private static void WriteHeaderRow(IXLWorksheet sheet, params string[] headers)
    {
        for (var index = 0; index < headers.Length; index += 1)
        {
            sheet.Cell(1, index + 1).Value = headers[index];
        }
    }

    private static void WriteRow(IXLWorksheet sheet, int rowIndex, params string[] values)
    {
        for (var index = 0; index < values.Length; index += 1)
        {
            sheet.Cell(rowIndex, index + 1).Value = values[index];
        }
    }
}
