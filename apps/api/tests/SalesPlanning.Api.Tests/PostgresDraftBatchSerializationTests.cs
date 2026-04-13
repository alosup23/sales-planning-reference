using SalesPlanning.Api.Domain;
using SalesPlanning.Api.Infrastructure.Postgres;
using System.Reflection;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PostgresDraftBatchSerializationTests
{
    [Fact]
    public void SerializeDraftBatchDeltas_RoundTripsCompactPayload()
    {
        var deltas = new[]
        {
            new PlanningCommandCellDelta(
                new PlanningCellCoordinate(1, 2, 101, 2111, 202600),
                new PlanningCellState(10m, 11m, false, 12m, 13m, 1.0m, false, null, null, 2, "leaf"),
                new PlanningCellState(20m, 21m, true, 22m, 23m, 1.1m, true, "manual", "planner.one", 3, "calculated"),
                "splash")
        };

        var serializeMethod = typeof(PostgresPlanningRepository).GetMethod(
            "SerializeDraftBatchDeltas",
            BindingFlags.Static | BindingFlags.NonPublic);
        var deserializeMethod = typeof(PostgresPlanningRepository).GetMethod(
            "DeserializeDraftBatchDeltas",
            BindingFlags.Static | BindingFlags.NonPublic);

        Assert.NotNull(serializeMethod);
        Assert.NotNull(deserializeMethod);

        var json = Assert.IsType<string>(serializeMethod!.Invoke(null, [deltas]));
        var roundTrip = Assert.IsType<List<PlanningCommandCellDelta>>(deserializeMethod!.Invoke(null, [json]));

        Assert.Contains("\"v\":2", json);
        Assert.Single(roundTrip);
        Assert.Equal(deltas[0], roundTrip[0]);
    }
}
