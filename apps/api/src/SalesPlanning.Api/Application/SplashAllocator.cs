using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed class SplashAllocator : ISplashAllocator
{
    public IReadOnlyList<SplashAllocation> Allocate(decimal total, IReadOnlyList<SplashTarget> targets, int scale)
    {
        var lockedTotal = targets.Where(t => t.Cell.IsLocked).Sum(t => t.Cell.EffectiveValue);
        var unlocked = targets.Where(t => !t.Cell.IsLocked).ToList();

        if (unlocked.Count == 0)
        {
            throw new InvalidOperationException("All target cells are locked.");
        }

        var remaining = total - lockedTotal;
        if (remaining < 0)
        {
            throw new InvalidOperationException("Locked cells exceed the requested splash total.");
        }

        var weightSum = unlocked.Sum(t => Math.Max(0m, t.Weight));
        if (weightSum <= 0)
        {
            throw new InvalidOperationException("No positive weights exist for unlocked targets.");
        }

        var unit = scale switch
        {
            <= 0 => 1m,
            _ => 1m / (decimal)Math.Pow(10, scale)
        };

        var staged = unlocked
            .Select(target =>
            {
                var raw = remaining * target.Weight / weightSum;
                var roundedDown = Math.Floor(raw / unit) * unit;
                return new
                {
                    Target = target,
                    Raw = raw,
                    Rounded = roundedDown,
                    Fraction = raw - roundedDown
                };
            })
            .OrderByDescending(x => x.Fraction)
            .ThenBy(x => x.Target.Cell.Coordinate.TimePeriodId)
            .ToList();

        var assigned = staged.Sum(x => x.Rounded);
        var residual = remaining - assigned;
        var residualUnits = unit == 0 ? 0 : (int)Math.Round(residual / unit, MidpointRounding.AwayFromZero);

        for (var i = 0; i < residualUnits; i++)
        {
            var index = i % staged.Count;
            staged[index] = new
            {
                staged[index].Target,
                staged[index].Raw,
                Rounded = staged[index].Rounded + unit,
                staged[index].Fraction
            };
        }

        return staged
            .Select(x => new SplashAllocation(x.Target.Cell, decimal.Round(x.Rounded, scale, MidpointRounding.AwayFromZero)))
            .ToList();
    }
}

