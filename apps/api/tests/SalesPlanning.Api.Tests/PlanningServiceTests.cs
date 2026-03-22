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
    public async Task ApplyLockAsync_WhenUnlockingSystemAggregateLock_RestoresRollupValue()
    {
        var scenarioVersionId = 1L;
        var measureId = 1L;
        var coordinate = new LockCoordinateDto(101, 2100, 202600);

        await _service.ApplyLockAsync(
            new LockCellsRequest(scenarioVersionId, measureId, true, "Freeze aggregate", new[] { coordinate }),
            "manager.one",
            CancellationToken.None);

        await _service.ApplyEditsAsync(
            new EditCellsRequest(
                scenarioVersionId,
                measureId,
                "Leaf adjustment",
                new[]
                {
                    new EditCellRequest(101, 2120, 202603, 500m, "input", 2)
                }),
            "planner.one",
            CancellationToken.None);

        var lockedAggregate = await _repository.GetCellAsync(new(1, 1, 101, 2100, 202600), CancellationToken.None);
        Assert.NotNull(lockedAggregate);
        Assert.True(lockedAggregate!.IsLocked);
        Assert.True(lockedAggregate.IsSystemGeneratedOverride);

        await _service.ApplyLockAsync(
            new LockCellsRequest(scenarioVersionId, measureId, false, "Release aggregate", new[] { coordinate }),
            "manager.one",
            CancellationToken.None);

        var unlockedAggregate = await _repository.GetCellAsync(new(1, 1, 101, 2100, 202600), CancellationToken.None);
        Assert.NotNull(unlockedAggregate);
        Assert.False(unlockedAggregate!.IsLocked);
        Assert.Null(unlockedAggregate.OverrideValue);
        Assert.False(unlockedAggregate.IsSystemGeneratedOverride);
        Assert.Equal(unlockedAggregate.DerivedValue, unlockedAggregate.EffectiveValue);
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

        Assert.Equal("The source cell is locked and cannot be used for splash.", exception.Message);
    }
}
