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
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store A", "Beverages", "Soft Drinks", "Cola" }));
    }

    [Fact]
    public async Task ApplyEditsAsync_WhenGrossProfitPercentChanges_RecalculatesAspRevenueAndGrossProfit()
    {
        var gpPercentCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfitPercent, 101, 2111, 202603), CancellationToken.None);
        Assert.NotNull(gpPercentCell);

        await _service.ApplyEditsAsync(
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

        var quantityCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SoldQuantity, 101, 2111, 202603), CancellationToken.None);
        var aspCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.AverageSellingPrice, 101, 2111, 202603), CancellationToken.None);
        var unitCostCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.UnitCost, 101, 2111, 202603), CancellationToken.None);
        var revenueCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603), CancellationToken.None);
        var grossProfitCell = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.GrossProfit, 101, 2111, 202603), CancellationToken.None);

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
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

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

        await _service.ApplyGrowthFactorAsync(
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

        var updatedMonthRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202603), CancellationToken.None);
        var updatedYearRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2111, 202600), CancellationToken.None);
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

        var storeAAfter = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2000, 202600), CancellationToken.None);
        var storeBLeafAfter = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, storeBLeaf.StoreId, storeBLeaf.ProductNodeId, 202601), CancellationToken.None);

        Assert.NotNull(storeAAfter);
        Assert.NotNull(storeBLeafAfter);
        Assert.Equal(storeBLeafBefore!.EffectiveValue, storeBLeafAfter!.EffectiveValue);
        Assert.NotEqual(storeABefore!.EffectiveValue, storeAAfter!.EffectiveValue);
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

        var departmentRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2100, 202600), CancellationToken.None);
        var softDrinksRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2110, 202600), CancellationToken.None);
        var teaRevenue = await _repository.GetCellAsync(new PlanningCellCoordinate(1, PlanningMeasures.SalesRevenue, 101, 2120, 202600), CancellationToken.None);

        Assert.NotNull(departmentRevenue);
        Assert.NotNull(softDrinksRevenue);
        Assert.NotNull(teaRevenue);
        Assert.NotNull(softDrinksBefore);
        Assert.NotEqual(softDrinksBefore!.EffectiveValue, softDrinksRevenue!.EffectiveValue);
        Assert.Equal(softDrinksRevenue!.EffectiveValue + teaRevenue!.EffectiveValue, departmentRevenue!.EffectiveValue);
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
        var grid = await _service.GetGridSliceAsync(1, CancellationToken.None);

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
}
