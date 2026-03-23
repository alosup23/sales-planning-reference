using ClosedXML.Excel;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
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
    public async Task ApplyEditsAsync_RecalculatesAncestorYearTotalsWithinSameTransaction()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                1,
                "Leaf adjustment",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 333m, "input", 2)
                }),
            "planner.one",
            CancellationToken.None);

        var beverageYear = await _repository.GetCellAsync(new(1, 1, 101, 2100, 202600), CancellationToken.None);
        var storeYear = await _repository.GetCellAsync(new(1, 1, 101, 2000, 202600), CancellationToken.None);

        Assert.NotNull(beverageYear);
        Assert.NotNull(storeYear);
        Assert.Equal(12008m, beverageYear!.EffectiveValue);
        Assert.Equal(17331m, storeYear!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyLockAsync_WhenAggregateCellIsUnlocked_DescendantEditsAreAllowedAgain()
    {
        var scenarioVersionId = 1L;
        var measureId = 1L;
        var coordinate = new LockCoordinateDto(101, 2100, 202600);

        await _service.ApplyLockAsync(
            new LockCellsRequest(scenarioVersionId, measureId, true, "Freeze aggregate", new[] { coordinate }),
            "manager.one",
            CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyEditsAsync(
            new EditCellsRequest(
                scenarioVersionId,
                measureId,
                "Leaf adjustment",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 500m, "input", 2)
                }),
            "planner.one",
            CancellationToken.None));

        await _service.ApplyLockAsync(
            new LockCellsRequest(scenarioVersionId, measureId, false, "Release aggregate", new[] { coordinate }),
            "manager.one",
            CancellationToken.None);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                scenarioVersionId,
                measureId,
                "Leaf adjustment after unlock",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 500m, "input", 2)
                }),
            "planner.one",
            CancellationToken.None);

        var unlockedAggregate = await _repository.GetCellAsync(new(1, 1, 101, 2100, 202600), CancellationToken.None);
        Assert.NotNull(unlockedAggregate);
        Assert.False(unlockedAggregate!.IsLocked);
        Assert.Equal(12175m, unlockedAggregate.EffectiveValue);
    }

    [Fact]
    public async Task ApplySplashAsync_RejectsLockedSourceCell()
    {
        await _service.ApplyLockAsync(
            new LockCellsRequest(1, 1, true, "Hold year", new[] { new LockCoordinateDto(101, 2110, 202600) }),
            "manager.one",
            CancellationToken.None);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplySplashAsync(
            new SplashRequest(
                1,
                1,
                new SplashCoordinateDto(101, 2110, 202600),
                12000m,
                "seasonality_profile",
                0,
                "Attempt on locked source",
                new Dictionary<long, decimal>
                {
                    [202601] = 8, [202602] = 12, [202603] = 7, [202604] = 7,
                    [202605] = 8, [202606] = 8, [202607] = 9, [202608] = 9,
                    [202609] = 8, [202610] = 7, [202611] = 8, [202612] = 9
                }),
            "planner.one",
            CancellationToken.None));

        Assert.Contains("is locked", exception.Message);
    }

    [Fact]
    public async Task ApplyEditsAsync_OnCategoryMonthEdit_SplashesAcrossLeafRowsAndKeepsTotalsCorrect()
    {
        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                1,
                "Category month update",
                new[]
                {
                    new EditCellRequest(101, 2100, 202603, 1200m, "override", 1)
                }),
            "planner.one",
            CancellationToken.None);

        var beverageMonth = await _repository.GetCellAsync(new(1, 1, 101, 2100, 202603), CancellationToken.None);
        var softDrinksMonth = await _repository.GetCellAsync(new(1, 1, 101, 2110, 202603), CancellationToken.None);
        var teaMonth = await _repository.GetCellAsync(new(1, 1, 101, 2120, 202603), CancellationToken.None);

        Assert.NotNull(beverageMonth);
        Assert.NotNull(softDrinksMonth);
        Assert.NotNull(teaMonth);
        Assert.Equal(1200m, beverageMonth!.EffectiveValue);
        Assert.Equal(880m, softDrinksMonth!.EffectiveValue);
        Assert.Equal(320m, teaMonth!.EffectiveValue);
    }

    [Fact]
    public async Task ApplyEditsAsync_RejectsDescendantEditWhenAncestorCellIsLocked()
    {
        await _service.ApplyLockAsync(
            new LockCellsRequest(1, 1, true, "Freeze beverages year", new[] { new LockCoordinateDto(101, 2100, 202600) }),
            "manager.one",
            CancellationToken.None);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyEditsAsync(
            new EditCellsRequest(
                1,
                1,
                "Leaf edit under locked aggregate",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 333m, "input", 2)
                }),
            "planner.one",
            CancellationToken.None));

        Assert.Contains("is locked", exception.Message);
    }

    [Fact]
    public async Task ImportWorkbookAsync_CreatesRowsAndLoadsLeafMonthValues()
    {
        using var workbook = new XLWorkbook();
        var sheet = workbook.AddWorksheet("Plan");
        sheet.Cell(1, 1).Value = "Store";
        sheet.Cell(1, 2).Value = "Category";
        sheet.Cell(1, 3).Value = "Subcategory";
        sheet.Cell(1, 4).Value = "Jan";
        sheet.Cell(1, 5).Value = "Feb";
        sheet.Cell(2, 1).Value = "Store B";
        sheet.Cell(2, 2).Value = "Frozen";
        sheet.Cell(2, 3).Value = "Ice Cream";
        sheet.Cell(2, 4).Value = 100;
        sheet.Cell(2, 5).Value = 110;

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        stream.Position = 0;

        var result = await _service.ImportWorkbookAsync(1, 1, stream, "import.xlsx", "planner.one", CancellationToken.None);
        var grid = await _service.GetGridSliceAsync(1, 1, CancellationToken.None);

        Assert.Equal(1, result.RowsProcessed);
        Assert.True(result.RowsCreated >= 3);
        Assert.Contains(grid.Rows, row => row.Path.SequenceEqual(new[] { "Store B", "Frozen", "Ice Cream" }));
        var importedLeaf = grid.Rows.Single(row => row.Path.SequenceEqual(new[] { "Store B", "Frozen", "Ice Cream" }));
        Assert.Equal(100m, importedLeaf.Cells[202601].Value);
        Assert.Equal(110m, importedLeaf.Cells[202602].Value);
        Assert.Equal(210m, importedLeaf.Cells[202600].Value);
    }
}
