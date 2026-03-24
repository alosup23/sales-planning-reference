namespace SalesPlanning.Api.Domain;

public static class PlanningMeasures
{
    public const long SalesRevenue = 1;
    public const long SoldQuantity = 2;
    public const long AverageSellingPrice = 3;

    public static readonly PlanningMeasureDefinition[] Definitions =
    [
        new(SalesRevenue, "Sales Revenue", 0, false),
        new(SoldQuantity, "Sold Qty", 0, false),
        new(AverageSellingPrice, "ASP", 2, true)
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
    bool DerivedAtAggregateLevels);
