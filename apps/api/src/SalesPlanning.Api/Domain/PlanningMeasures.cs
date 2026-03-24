namespace SalesPlanning.Api.Domain;

public static class PlanningMeasures
{
    public const long SalesRevenue = 1;
    public const long SoldQuantity = 2;
    public const long AverageSellingPrice = 3;
    public const long UnitCost = 4;
    public const long TotalCosts = 5;
    public const long GrossProfit = 6;
    public const long GrossProfitPercent = 7;

    public static readonly PlanningMeasureDefinition[] Definitions =
    [
        new(SalesRevenue, "Sales Revenue", 0, false, false, true, true),
        new(SoldQuantity, "Sold Qty", 0, false, false, true, true),
        new(AverageSellingPrice, "ASP", 2, true, false, true, true),
        new(UnitCost, "Unit Cost", 2, true, false, true, true),
        new(TotalCosts, "Total Costs", 0, false, false, false, false),
        new(GrossProfit, "GP", 0, false, false, false, false),
        new(GrossProfitPercent, "GP%", 1, true, true, true, true)
    ];

    public static IReadOnlyList<long> SupportedMeasureIds => Definitions.Select(definition => definition.MeasureId).ToList();

    public static PlanningMeasureDefinition GetDefinition(long measureId)
    {
        return Definitions.Single(definition => definition.MeasureId == measureId);
    }
}

public sealed record PlanningMeasureDefinition(
    long MeasureId,
    string Label,
    int DecimalPlaces,
    bool DerivedAtAggregateLevels,
    bool DisplayAsPercent,
    bool EditableAtLeaf,
    bool EditableAtAggregate);
