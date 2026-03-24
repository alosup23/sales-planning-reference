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
    public async Task GetGridSliceAsync_ReturnsMultiMeasureMultiYearShape()
    {
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

        Assert.Equal(3, grid.Measures.Count);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202600);
        Assert.Contains(grid.Periods, period => period.TimePeriodId == 202700);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks" }));
    }

    [Fact]
    public async Task ApplyEditsAsync_WhenSoldQtyChanges_RecalculatesSalesRevenueAndAspRemainsRounded()
    {
        var quantityCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2110, 202603), CancellationToken.None);
        Assert.NotNull(quantityCell);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                PlanningMeasures.SoldQuantity,
                "Qty adjustment",
                new[]
                {
                    new EditCellRequest(101, 2110, 202603, 200m, "input", quantityCell!.RowVersion)
                }),
            "planner.one",
            CancellationToken.None);

        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202603), CancellationToken.None);
        var aspCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.AverageSellingPrice, 101, 2110, 202603), CancellationToken.None);

        Assert.NotNull(revenueCell);
        Assert.NotNull(aspCell);
        Assert.Equal(decimal.Ceiling(200m * aspCell!.EffectiveValue), revenueCell!.EffectiveValue);
        Assert.Equal(Math.Round(aspCell.EffectiveValue, 2, MidpointRounding.AwayFromZero), aspCell.EffectiveValue);
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
        WriteImportRow(storeSheet, 2, "Store B", "Frozen", "Ice Cream", 2026, "Jan", 100m, 50m, 2m);
        WriteImportRow(storeSheet, 3, "Store B", "Frozen", "Gelato", 2026, "Feb", 101m, 50m, 2m);

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
    }

    private static void WriteImportRow(IXLWorksheet sheet, int rowIndex, string store, string department, string @class, int year, string month, decimal revenue, decimal quantity, decimal asp)
    {
        sheet.Cell(rowIndex, 1).Value = store;
        sheet.Cell(rowIndex, 2).Value = department;
        sheet.Cell(rowIndex, 3).Value = @class;
        sheet.Cell(rowIndex, 4).Value = year;
        sheet.Cell(rowIndex, 5).Value = month;
        sheet.Cell(rowIndex, 6).Value = revenue;
        sheet.Cell(rowIndex, 7).Value = quantity;
        sheet.Cell(rowIndex, 8).Value = asp;
    }
}
