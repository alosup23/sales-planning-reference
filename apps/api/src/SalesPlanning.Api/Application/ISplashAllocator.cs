using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public interface ISplashAllocator
{
    IReadOnlyList<SplashAllocation> Allocate(decimal total, IReadOnlyList<SplashTarget> targets, int scale);
}

public sealed record SplashTarget(PlanningCell Cell, decimal Weight);

public sealed record SplashAllocation(PlanningCell Cell, decimal NewValue);

