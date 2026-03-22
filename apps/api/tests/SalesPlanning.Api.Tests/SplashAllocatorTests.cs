using SalesPlanning.Api.Application;
using SalesPlanning.Api.Domain;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class SplashAllocatorTests
{
    private readonly SplashAllocator _allocator = new();

    [Fact]
    public void Allocate_RespectsLockedCells_AndDistributesResidualDeterministically()
    {
        var targets = new[]
        {
            CreateTarget(202601, 0, false, 8),
            CreateTarget(202602, 1500, true, 12),
            CreateTarget(202603, 0, false, 7),
            CreateTarget(202604, 0, false, 7),
            CreateTarget(202605, 0, false, 8),
            CreateTarget(202606, 0, false, 8),
            CreateTarget(202607, 0, false, 9),
            CreateTarget(202608, 0, false, 9),
            CreateTarget(202609, 0, false, 8),
            CreateTarget(202610, 0, false, 7),
            CreateTarget(202611, 0, false, 8),
            CreateTarget(202612, 0, false, 9)
        };

        var allocations = _allocator.Allocate(12000m, targets, 0);
        var allocationMap = allocations.ToDictionary(item => item.Cell.Coordinate.TimePeriodId, item => item.NewValue);

        Assert.Equal(955m, allocationMap[202601]);
        Assert.DoesNotContain(202602, allocationMap.Keys);
        Assert.Equal(835m, allocationMap[202603]);
        Assert.Equal(1074m, allocationMap[202607]);
        Assert.Equal(954m, allocationMap[202611]);
        Assert.Equal(10500m, allocations.Sum(item => item.NewValue));
    }

    [Fact]
    public void Allocate_Throws_WhenAllTargetsAreLocked()
    {
        var targets = new[]
        {
            CreateTarget(202601, 100, true, 1),
            CreateTarget(202602, 200, true, 1)
        };

        var exception = Assert.Throws<InvalidOperationException>(() => _allocator.Allocate(500m, targets, 0));
        Assert.Equal("All target cells are locked.", exception.Message);
    }

    private static SplashTarget CreateTarget(long timePeriodId, decimal effectiveValue, bool isLocked, decimal weight)
    {
        return new SplashTarget(
            new PlanningCell
            {
                Coordinate = new PlanningCellCoordinate(1, 1, 101, 2110, timePeriodId),
                EffectiveValue = effectiveValue,
                IsLocked = isLocked
            },
            weight);
    }
}
