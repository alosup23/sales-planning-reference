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

    public static decimal DefaultSeedUnitCost(decimal asp)
    {
        return NormalizeUnitCost(Math.Max(NormalizeAsp(asp) * 0.6m, 0.25m));
    }
}
