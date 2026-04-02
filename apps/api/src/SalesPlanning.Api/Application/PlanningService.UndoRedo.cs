using System.Text.Json;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Application;

public sealed partial class PlanningService
{
    private const int UndoRedoLimit = 30;

    private static PlanningGridPatchDto? BuildGridPatch(long scenarioVersionId, IReadOnlyList<PlanningCommandCellDelta> deltas)
    {
        if (deltas.Count == 0)
        {
            return null;
        }

        return new PlanningGridPatchDto(
            scenarioVersionId,
            deltas.Select(delta => new GridCellPatchDto(
                    delta.Coordinate.StoreId,
                    delta.Coordinate.ProductNodeId,
                    delta.Coordinate.TimePeriodId,
                    delta.Coordinate.MeasureId,
                    new GridCellDto(
                        delta.NewState.EffectiveValue,
                        delta.NewState.GrowthFactor,
                        delta.NewState.IsLocked,
                        string.Equals(delta.NewState.CellKind, "calculated", StringComparison.OrdinalIgnoreCase),
                        delta.NewState.OverrideValue is not null,
                        delta.NewState.RowVersion,
                        delta.NewState.CellKind)))
                .ToList());
    }

    private static IReadOnlyList<PlanningCellDeltaAudit> BuildAuditDeltas(IReadOnlyList<PlanningCommandCellDelta> deltas)
    {
        return deltas
            .Where(delta => delta.OldState.EffectiveValue != delta.NewState.EffectiveValue)
            .Select(delta => new PlanningCellDeltaAudit(
                delta.Coordinate,
                delta.OldState.EffectiveValue,
                delta.NewState.EffectiveValue,
                delta.OldState.IsLocked,
                delta.ChangeKind))
            .ToList();
    }

    private async Task<long> AppendCommandBatchAsync(
        long scenarioVersionId,
        string userId,
        string commandKind,
        object? commandScope,
        IReadOnlyList<PlanningCommandCellDelta> deltas,
        CancellationToken cancellationToken)
    {
        if (deltas.Count == 0)
        {
            return 0;
        }

        var primaryUserId = GetPrimaryPlanningUserId(userId);
        var batch = new PlanningCommandBatch(
            await _repository.GetNextCommandBatchIdAsync(cancellationToken),
            scenarioVersionId,
            primaryUserId,
            commandKind,
            commandScope is null ? null : JsonSerializer.Serialize(commandScope),
            false,
            null,
            DateTimeOffset.UtcNow,
            null,
            deltas);

        await _repository.AppendCommandBatchAsync(batch, cancellationToken);
        return batch.CommandBatchId;
    }

    private async Task<long> AppendDraftCommandBatchAsync(
        long scenarioVersionId,
        string userId,
        string commandKind,
        object? commandScope,
        IReadOnlyList<PlanningCommandCellDelta> deltas,
        CancellationToken cancellationToken)
    {
        if (deltas.Count == 0)
        {
            return 0;
        }

        var primaryUserId = GetPrimaryPlanningUserId(userId);
        var batch = new PlanningCommandBatch(
            await _repository.GetNextDraftCommandBatchIdAsync(cancellationToken),
            scenarioVersionId,
            primaryUserId,
            commandKind,
            commandScope is null ? null : JsonSerializer.Serialize(commandScope),
            false,
            null,
            DateTimeOffset.UtcNow,
            null,
            deltas);

        await _repository.AppendDraftCommandBatchAsync(batch, cancellationToken);
        return batch.CommandBatchId;
    }

    private async Task<PlanningUndoRedoAvailability> GetWorkingUndoRedoAvailabilityAsync(
        long scenarioVersionId,
        string userId,
        CancellationToken cancellationToken)
    {
        var draftAvailability = await _repository.GetDraftUndoRedoAvailabilityAsync(scenarioVersionId, userId, UndoRedoLimit, cancellationToken);
        if (draftAvailability.CanUndo || draftAvailability.CanRedo)
        {
            return draftAvailability;
        }

        return await _repository.GetUndoRedoAvailabilityAsync(scenarioVersionId, GetPrimaryPlanningUserId(userId), UndoRedoLimit, cancellationToken);
    }

    private static IReadOnlyList<PlanningCommandCellDelta> InvertCommandDeltas(IReadOnlyList<PlanningCommandCellDelta> deltas)
    {
        return deltas
            .Select(delta => new PlanningCommandCellDelta(delta.Coordinate, delta.NewState, delta.OldState, delta.ChangeKind))
            .ToList();
    }

    private static UndoRedoAvailabilityDto ToUndoRedoAvailabilityDto(PlanningUndoRedoAvailability availability) =>
        new(
            availability.CanUndo,
            availability.CanRedo,
            availability.UndoDepth,
            availability.RedoDepth,
            availability.Limit);
}
