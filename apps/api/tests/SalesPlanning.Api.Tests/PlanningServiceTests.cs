using ClosedXML.Excel;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using SalesPlanning.Api.Infrastructure;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PlanningServiceTests
{
    private readonly InMemoryPlanningRepository _repository = new();
    private readonly PlanningService _service;

    public PlanningServiceTests()
    {
        _service = new PlanningService(_repository, new SplashAllocator());
    }

    [Fact]
    public async Task GetGridSliceAsync_ReturnsExpandedMeasureSetAndMultiYearShape()
    {
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

        Assert.Equal(7, grid.Measures.Count);
        Assert.Contains(grid.Measures, measure => measure.MeasureId == PlanningMeasures.GrossProfitPercent && measure.DisplayAsPercent);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202600);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202700);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks" }));
    }

    [Fact]
    public async Task ApplyEditsAsync_WhenGrossProfitPercentChanges_RecalculatesAspRevenueAndGrossProfit()
    {
        var gpPercentCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, 101, 2110, 202603), CancellationToken.None);
        Assert.NotNull(gpPercentCell);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.GrossProfitPercent,
                "Margin update",
                new[]
                {
                    new EditCellRequest(101, 2110, 202603, 30m, "input", gpPercentCell!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None);

        var quantityCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2110, 202603), CancellationToken.None);
        var aspCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.AverageSellingPrice, 101, 2110, 202603), CancellationToken.None);
        var unitCostCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.UnitCost, 101, 2110, 202603), CancellationToken.None);
        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202603), CancellationToken.None);
        var grossProfitCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfit, 101, 2110, 202603), CancellationToken.None);

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
        await _service.ApplyLockAsync(
            new LockCellsRequest(1, PlanningMeasures.SalesRevenue, true, "Freeze year", new[] { new LockCoordinateDto(101, 2100, 202600) }),
            "manager.one",
            CancellationToken.None);

        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202603), CancellationToken.None);
        Assert.NotNull(revenueCell);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SalesRevenue,
                "Blocked revenue edit",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 900m, "input", revenueCell!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None));

        Assert.Contains("locked", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ImportWorkbookAsync_LoadsValidRowsAndReturnsExceptionWorkbookForInvalidRows()
    {
        using var workbook = new XLWorkbook();
        var storeSheet = workbook.AddWorksheet("Store B");
        WriteImportHeader(storeSheet);
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", 2026, "Jan", 100m, 50m, 2m, 1.20m, 60m, 40m, 40m);
        WriteImportRow(storeSheet, 3, "Store B", "Frozen", "Gelato", 2026, "Feb", 101m, 50m, 2m, 1.20m, 60m, 40m, 40m);

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var result = await _service.ImportWorkbookAsync(1, stream, "import.xlsx", "planner.one", CancellationToken.None);
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

        Assert.Equal(2, result.RowsProcessed);
        Assert.NotNull(result.ExceptionWorkbookBase64);
        Assert.NotNull(result.ExceptionFileName);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store B", "Frozen", "Ice Cream" }));

        var mappings = await _service.GetHierarchyMappingsAsync(CancellationToken.None);
        var frozen = mappings.Departments.Single(department => department.DepartmentLabel == "Frozen");
        Assert.Contains("Ice Cream", frozen.ClassLabels);
    }

    [Fact]
    public async Task SaveScenarioAsync_ReturnsCheckpointTimestamp()
    {
        var result = await _service.SaveScenarioAsync(new SaveScenarioRequest(1, "manual"), "planner.one", CancellationToken.None);
        Assert.Equal("saved", result.Status);
        Assert.Equal("manual", result.Mode);
    }

    [Fact]
    public async Task ApplyGrowthFactorAsync_OnLeafRevenue_UpdatesGrowthFactorAndRollsUp()
    {
        var beforeYearRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600), CancellationToken.None);
        var beforeMonthRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202603), CancellationToken.None);
        Assert.NotNull(beforeYearRevenue);
        Assert.NotNull(beforeMonthRevenue);

        await _service.ApplyGrowthFactorAsync(
            new ApplyGrowthFactorRequest(
                1,
                PlanningMeasures.SalesRevenue,
                new SplashCoordinateDto(101, 2110, 202603),
                beforeMonthRevenue!.EffectiveValue,
                1.1m,
                "Leaf uplift",
                null),
            "planner.one",
            CancellationToken.None);

        var updatedMonthRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202603), CancellationToken.None);
        var updatedYearRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600), CancellationToken.None);
        Assert.NotNull(updatedMonthRevenue);
        Assert.NotNull(updatedYearRevenue);
        Assert.Equal(1.1m, updatedMonthRevenue!.GrowthFactor);
        Assert.True(updatedMonthRevenue.EffectiveValue > beforeMonthRevenue.EffectiveValue);
        Assert.True(updatedYearRevenue!.EffectiveValue > beforeYearRevenue!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnStoreAggregate_OnlySplashesInsideThatStoreScope()
    {
        using var workbook = new XLWorkbook();
        var storeSheet = workbook.AddWorksheet("Store B");
        WriteImportHeader(storeSheet);
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", 2026, "Jan", 120m, 60m, 2m, 1.20m, 72m, 48m, 40m);
        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        await _service.ImportWorkbookAsync(1, stream, "store-b.xlsx", "planner.one", CancellationToken.None);
        var metadata = await _repository.GetMetadataAsync(CancellationToken.None);
        var storeBRoot = metadata.ProductNodes.Values.Single(node => node.Level == 0 && node.Label == "Store B");
        var storeABefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202600), CancellationToken.None);
        var storeBBefore = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, storeBRoot.StoreId, storeBRoot.ProductNodeId, 202600), CancellationToken.None);
        Assert.NotNull(storeABefore);
        Assert.NotNull(storeBBefore);

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

        var storeAAfter = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202600), CancellationToken.None);
        var storeBAfter = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, storeBRoot.StoreId, storeBRoot.ProductNodeId, 202600), CancellationToken.None);

        Assert.NotNull(storeAAfter);
        Assert.NotNull(storeBAfter);
        Assert.Equal(storeBBefore!.EffectiveValue, storeBAfter!.EffectiveValue);
        Assert.NotEqual(storeABefore!.EffectiveValue, storeAAfter!.EffectiveValue);
    }

    [Fact]
    public async Task GenerateNextYearAsync_CopiesEditableInputsAndRecomputesCalculatedMeasures()
    {
        var result = await _service.GenerateNextYearAsync(new GenerateNextYearRequest(1, 202600), "planner.one", CancellationToken.None);
        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202703), CancellationToken.None);
        var quantityCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2110, 202703), CancellationToken.None);
        var gpPercentCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, 101, 2110, 202703), CancellationToken.None);

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
        Assert.Contains("Ice Cream", frozen.ClassLabels);
    }

    [Fact]
    public async Task DeleteYearAsync_RemovesYearFromGrid()
    {
        var result = await _service.DeleteYearAsync(new DeleteYearRequest(1, 202700), CancellationToken.None);
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

        Assert.True(result.DeletedCellCount > 0);
        Assert.DoesNotContain(grid.Periods, period => period.TimePeriodId == 202700 || period.ParentTimePeriodId == 202700);
    }

    private static void WriteImportHeader(IXLWorksheet sheet)
    {
        sheet.Cell(1, 1).Value = "Store";
        sheet.Cell(1, 2).Value = "Department";
        sheet.Cell(1, 3).Value = "Class";
        sheet.Cell(1, 4).Value = "Year";
        sheet.Cell(1, 5).Value = "Month";
        sheet.Cell(1, 6).Value = "Sales Revenue";
        sheet.Cell(1, 7).Value = "Sold Qty";
        sheet.Cell(1, 8).Value = "ASP";
        sheet.Cell(1, 9).Value = "Unit Cost";
        sheet.Cell(1, 10).Value = "Total Costs";
        sheet.Cell(1, 11).Value = "GP";
        sheet.Cell(1, 12).Value = "GP%";
    }

    private static void WriteImportRow(
        IXLWorksheet sheet,
        int rowIndex,
        string store,
        string department,
        string @class,
        int year,
        string month,
        decimal revenue,
        decimal quantity,
        decimal asp,
        decimal unitCost,
        decimal totalCosts,
        decimal gp,
        decimal gpPercent)
    {
        sheet.Cell(rowIndex, 1).Value = store;
        sheet.Cell(rowIndex, 2).Value = department;
        sheet.Cell(rowIndex, 3).Value = @class;
        sheet.Cell(rowIndex, 4).Value = year;
        sheet.Cell(rowIndex, 5).Value = month;
        sheet.Cell(rowIndex, 6).Value = revenue;
        sheet.Cell(rowIndex, 7).Value = quantity;
        sheet.Cell(rowIndex, 8).Value = asp;
        sheet.Cell(rowIndex, 9).Value = unitCost;
        sheet.Cell(rowIndex, 10).Value = totalCosts;
        sheet.Cell(rowIndex, 11).Value = gp;
        sheet.Cell(rowIndex, 12).Value = gpPercent;
    }
}
