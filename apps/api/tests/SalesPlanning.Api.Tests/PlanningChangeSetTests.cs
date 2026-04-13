using System.Reflection;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Domain;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PlanningChangeSetTests
{
    private static readonly MethodInfo BuildChangedCellsAndDeltasMethod =
        typeof(PlanningService).GetMethod("BuildChangedCellsAndDeltas", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("BuildChangedCellsAndDeltas method not found.");

    [Fact]
    public void BuildChangedCellsAndDeltas_NewMaterializedCell_ProducesDeltaAndInitialRowVersion()
    {
        var coordinate = new PlanningCellCoordinate(7, 101, 3, 55, 202601);
        var workingCell = new PlanningCell
        {
            Coordinate = coordinate,
            InputValue = 125m,
            OverrideValue = null,
            IsSystemGeneratedOverride = false,
            DerivedValue = 125m,
            EffectiveValue = 125m,
            GrowthFactor = 1.0m,
            IsLocked = false,
            CellKind = "leaf"
        };

        var result = InvokeBuildChangedCellsAndDeltas([], [workingCell], "edit");

        Assert.Single(result.ChangedCells);
        Assert.Single(result.Deltas);
        Assert.Equal(1, result.ChangedCells[0].RowVersion);
        Assert.Equal(0, result.Deltas[0].OldState.RowVersion);
        Assert.Equal(1, result.Deltas[0].NewState.RowVersion);
        Assert.Equal(125m, result.Deltas[0].NewState.InputValue);
    }

    [Fact]
    public void BuildChangedCellsAndDeltas_NewDefaultCell_SkipsNoOpPersistence()
    {
        var coordinate = new PlanningCellCoordinate(7, 101, 3, 55, 202601);
        var workingCell = new PlanningCell
        {
            Coordinate = coordinate,
            InputValue = null,
            OverrideValue = null,
            IsSystemGeneratedOverride = false,
            DerivedValue = 0m,
            EffectiveValue = 0m,
            GrowthFactor = 1.0m,
            IsLocked = false,
            CellKind = "leaf"
        };

        var result = InvokeBuildChangedCellsAndDeltas([], [workingCell], "edit");

        Assert.Empty(result.ChangedCells);
        Assert.Empty(result.Deltas);
    }

    private static (List<PlanningCell> ChangedCells, List<PlanningCommandCellDelta> Deltas) InvokeBuildChangedCellsAndDeltas(
        IReadOnlyList<PlanningCell> originalCells,
        IEnumerable<PlanningCell> workingCells,
        string changeKind)
    {
        var result = BuildChangedCellsAndDeltasMethod.Invoke(null, [originalCells, workingCells, changeKind]);
        Assert.NotNull(result);
        return ((List<PlanningCell> ChangedCells, List<PlanningCommandCellDelta> Deltas))result!;
    }
}
