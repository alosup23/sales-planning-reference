namespace SalesPlanning.Api.Domain;

public static class PlanningMath
{
    public static decimal NormalizeAsp(decimal value)
    {
        var sanitized = value <= 0m ? 1.00m : value;
        return Math.Round(sanitized, 2, MidpointRounding.AwayFromZero);
    }

    public static decimal NormalizeUnitCost(decimal value)
    {
        return Math.Round(Math.Max(value, 0m), 2, MidpointRounding.AwayFromZero);
    }

    public static decimal NormalizeQuantity(decimal value)
    {
        return Math.Round(Math.Max(value, 0m), 0, MidpointRounding.AwayFromZero);
    }

    public static decimal NormalizeRevenue(decimal value)
    {
        return decimal.Ceiling(Math.Max(value, 0m));
    }

    public static decimal NormalizeTotalCosts(decimal value)
    {
        return decimal.Ceiling(Math.Max(value, 0m));
    }

    public static decimal NormalizeGrossProfit(decimal value)
    {
        return decimal.Ceiling(value);
    }

    public static decimal NormalizeGrossProfitPercent(decimal value)
    {
        return Math.Round(value, 1, MidpointRounding.AwayFromZero);
    }

    public static decimal NormalizeGrowthFactor(decimal value)
    {
        return Math.Round(Math.Max(value, 0m), 2, MidpointRounding.AwayFromZero);
    }

    public static decimal NormalizeMeasureValue(long measureId, decimal value)
    {
        return measureId switch
        {
            PlanningMeasures.SalesRevenue => NormalizeRevenue(value),
            PlanningMeasures.SoldQuantity => NormalizeQuantity(value),
            PlanningMeasures.AverageSellingPrice => NormalizeAsp(value),
            PlanningMeasures.UnitCost => NormalizeUnitCost(value),
            PlanningMeasures.TotalCosts => NormalizeTotalCosts(value),
            PlanningMeasures.GrossProfit => NormalizeGrossProfit(value),
            PlanningMeasures.GrossProfitPercent => NormalizeGrossProfitPercent(value),
            _ => value
        };
    }

    public static decimal ApplyGrowthFactor(long measureId, decimal baseValue, decimal growthFactor)
    {
        return NormalizeMeasureValue(measureId, baseValue * NormalizeGrowthFactor(growthFactor));
    }

    public static decimal CalculateRevenue(decimal quantity, decimal asp)
    {
        return NormalizeRevenue(NormalizeQuantity(quantity) * NormalizeAsp(asp));
    }

    public static decimal CalculateTotalCosts(decimal quantity, decimal unitCost)
    {
        return NormalizeTotalCosts(NormalizeQuantity(quantity) * NormalizeUnitCost(unitCost));
    }

    public static decimal CalculateGrossProfit(decimal quantity, decimal asp, decimal unitCost)
    {
        return NormalizeGrossProfit((NormalizeAsp(asp) - NormalizeUnitCost(unitCost)) * NormalizeQuantity(quantity));
    }

    public static decimal CalculateGrossProfitPercent(decimal asp, decimal unitCost)
    {
        var normalizedAsp = NormalizeAsp(asp);
        var normalizedUnitCost = NormalizeUnitCost(unitCost);
        if (normalizedAsp <= 0m)
        {
            return 0m;
        }

        return NormalizeGrossProfitPercent(((normalizedAsp - normalizedUnitCost) / normalizedAsp) * 100m);
    }

    public static decimal DeriveAspFromRevenue(decimal revenue, decimal quantity)
    {
        var normalizedQuantity = NormalizeQuantity(quantity);
        if (normalizedQuantity <= 0m)
        {
            return 1.00m;
        }

        return NormalizeAsp(revenue / normalizedQuantity);
    }

    public static decimal ResolveAspForRevenue(decimal quantity, decimal revenue)
    {
        var normalizedQuantity = NormalizeQuantity(quantity);
        var normalizedRevenue = NormalizeRevenue(revenue);
        if (normalizedQuantity <= 0m)
        {
            return normalizedRevenue <= 0m ? 1.00m : NormalizeAsp(normalizedRevenue);
        }

        var derivedAsp = DeriveAspFromRevenue(normalizedRevenue, normalizedQuantity);
        if (CalculateRevenue(normalizedQuantity, derivedAsp) == normalizedRevenue)
        {
            return derivedAsp;
        }

        var baseCents = decimal.ToInt32(decimal.Round(derivedAsp * 100m, 0, MidpointRounding.AwayFromZero));
        for (var offset = 1; offset <= 500; offset += 1)
        {
            foreach (var sign in new[] { -1, 1 })
            {
                var candidateCents = baseCents + (offset * sign);
                if (candidateCents <= 0)
                {
                    continue;
                }

                var candidateAsp = NormalizeAsp(candidateCents / 100m);
                if (CalculateRevenue(normalizedQuantity, candidateAsp) == normalizedRevenue)
                {
                    return candidateAsp;
                }
            }
        }

        return derivedAsp;
    }

    public static decimal DeriveQuantityFromRevenue(decimal revenue, decimal asp)
    {
        var normalizedAsp = NormalizeAsp(asp);
        if (normalizedAsp <= 0m)
        {
            return 0m;
        }

        return NormalizeQuantity(Math.Round(revenue / normalizedAsp, 0, MidpointRounding.AwayFromZero));
    }

    public static decimal DeriveUnitCostFromTotalCosts(decimal totalCosts, decimal quantity)
    {
        var normalizedQuantity = NormalizeQuantity(quantity);
        if (normalizedQuantity <= 0m)
        {
            return 0m;
        }

        return NormalizeUnitCost(totalCosts / normalizedQuantity);
    }

    public static decimal ResolveUnitCostForTotalCosts(decimal quantity, decimal totalCosts)
    {
        var normalizedQuantity = NormalizeQuantity(quantity);
        var targetTotalCosts = NormalizeTotalCosts(totalCosts);
        if (normalizedQuantity <= 0m)
        {
            return 0m;
        }

        var derivedUnitCost = DeriveUnitCostFromTotalCosts(targetTotalCosts, normalizedQuantity);
        if (CalculateTotalCosts(normalizedQuantity, derivedUnitCost) == targetTotalCosts)
        {
            return derivedUnitCost;
        }

        var baseCents = decimal.ToInt32(decimal.Round(derivedUnitCost * 100m, 0, MidpointRounding.AwayFromZero));
        for (var offset = 1; offset <= 500; offset += 1)
        {
            foreach (var sign in new[] { -1, 1 })
            {
                var candidateCents = baseCents + (offset * sign);
                if (candidateCents < 0)
                {
                    continue;
                }

                var candidateUnitCost = NormalizeUnitCost(candidateCents / 100m);
                if (CalculateTotalCosts(normalizedQuantity, candidateUnitCost) == targetTotalCosts)
                {
                    return candidateUnitCost;
                }
            }
        }

        return derivedUnitCost;
    }

    public static decimal DeriveAspFromGrossProfitPercent(decimal unitCost, decimal grossProfitPercent)
    {
        var normalizedUnitCost = NormalizeUnitCost(unitCost);
        var normalizedGrossProfitPercent = NormalizeGrossProfitPercent(grossProfitPercent);
        if (normalizedGrossProfitPercent >= 100m)
        {
            throw new InvalidOperationException("GP% must be less than 100.0.");
        }

        var denominator = 1m - (normalizedGrossProfitPercent / 100m);
        if (denominator <= 0m)
        {
            throw new InvalidOperationException("GP% produces an invalid ASP.");
        }

        if (normalizedUnitCost <= 0m)
        {
            throw new InvalidOperationException("Unit Cost must be greater than zero before GP% can be edited.");
        }

        return NormalizeAsp(normalizedUnitCost / denominator);
    }

    public static decimal ResolveAspForGrossProfitPercent(decimal unitCost, decimal grossProfitPercent)
    {
        var normalizedUnitCost = NormalizeUnitCost(unitCost);
        var targetGrossProfitPercent = NormalizeGrossProfitPercent(grossProfitPercent);
        var derivedAsp = DeriveAspFromGrossProfitPercent(normalizedUnitCost, targetGrossProfitPercent);
        if (CalculateGrossProfitPercent(derivedAsp, normalizedUnitCost) == targetGrossProfitPercent)
        {
            return derivedAsp;
        }

        var baseCents = decimal.ToInt32(decimal.Round(derivedAsp * 100m, 0, MidpointRounding.AwayFromZero));
        for (var offset = 1; offset <= 500; offset += 1)
        {
            foreach (var sign in new[] { -1, 1 })
            {
                var candidateCents = baseCents + (offset * sign);
                if (candidateCents <= 0)
                {
                    continue;
                }

                var candidateAsp = NormalizeAsp(candidateCents / 100m);
                if (CalculateGrossProfitPercent(candidateAsp, normalizedUnitCost) == targetGrossProfitPercent)
                {
                    return candidateAsp;
                }
            }
        }

        return derivedAsp;
    }

    public static (decimal Asp, decimal UnitCost) ResolveLeafStateForGrossProfitPercent(
        decimal currentAsp,
        decimal currentUnitCost,
        decimal grossProfitPercent)
    {
        var normalizedCurrentAsp = NormalizeAsp(currentAsp);
        var normalizedCurrentUnitCost = NormalizeUnitCost(currentUnitCost);
        var targetGrossProfitPercent = NormalizeGrossProfitPercent(grossProfitPercent);
        var derivedAsp = ResolveAspForGrossProfitPercent(normalizedCurrentUnitCost, targetGrossProfitPercent);

        var bestAsp = derivedAsp;
        var bestUnitCost = normalizedCurrentUnitCost;
        var bestScore = decimal.MaxValue;

        var baseCents = decimal.ToInt32(decimal.Round(derivedAsp * 100m, 0, MidpointRounding.AwayFromZero));
        for (var aspOffset = 0; aspOffset <= 500; aspOffset += 1)
        {
            foreach (var aspSign in new[] { -1, 1 })
            {
                var candidateAspCents = baseCents + (aspOffset * aspSign);
                if (candidateAspCents <= 0)
                {
                    continue;
                }

                var candidateAsp = NormalizeAsp(candidateAspCents / 100m);
                var idealUnitCost = NormalizeUnitCost(candidateAsp * (1m - (targetGrossProfitPercent / 100m)));
                var baseUnitCostCents = decimal.ToInt32(decimal.Round(idealUnitCost * 100m, 0, MidpointRounding.AwayFromZero));
                for (var costOffset = 0; costOffset <= 50; costOffset += 1)
                {
                    foreach (var costSign in new[] { -1, 1 })
                    {
                        var candidateUnitCostCents = baseUnitCostCents + (costOffset * costSign);
                        if (candidateUnitCostCents < 0)
                        {
                            continue;
                        }

                        var candidateUnitCost = NormalizeUnitCost(candidateUnitCostCents / 100m);
                        if (CalculateGrossProfitPercent(candidateAsp, candidateUnitCost) != targetGrossProfitPercent)
                        {
                            continue;
                        }

                        var score = Math.Abs(candidateAsp - normalizedCurrentAsp) + Math.Abs(candidateUnitCost - normalizedCurrentUnitCost);
                        if (score < bestScore)
                        {
                            bestScore = score;
                            bestAsp = candidateAsp;
                            bestUnitCost = candidateUnitCost;
                        }
                    }
                }
            }
        }

        return (bestAsp, bestUnitCost);
    }

    public static decimal DefaultSeedUnitCost(decimal asp)
    {
        return NormalizeUnitCost(Math.Max(NormalizeAsp(asp) * 0.6m, 0.25m));
    }
}
