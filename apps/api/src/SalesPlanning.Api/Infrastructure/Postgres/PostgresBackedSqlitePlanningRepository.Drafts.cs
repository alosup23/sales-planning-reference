using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository
{
    private async Task<IReadOnlyList<PlanningCell>> GetDraftCellsDirectAsync(
        long scenarioVersionId,
        string userId,
        IEnumerable<PlanningCellCoordinate> coordinates,
        CancellationToken cancellationToken)
    {
        var coordinateList = coordinates
            .Where(coordinate => coordinate.ScenarioVersionId == scenarioVersionId)
            .DistinctBy(coordinate => coordinate.Key)
            .ToList();
        if (coordinateList.Count == 0)
        {
            return [];
        }

        return await ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            var cells = new List<PlanningCell>();
            const string sql = """
                select p.scenario_version_id,
                       p.measure_id,
                       p.store_id,
                       p.product_node_id,
                       p.time_period_id,
                       p.input_value,
                       p.override_value,
                       p.is_system_generated_override,
                       p.derived_value,
                       p.effective_value,
                       p.growth_factor,
                       p.is_locked,
                       p.lock_reason,
                       p.locked_by,
                       p.row_version,
                       p.cell_kind
                from planning_draft_cells p
                inner join unnest(
                    @scenarioVersionIds,
                    @measureIds,
                    @storeIds,
                    @productNodeIds,
                    @timePeriodIds)
                    as requested(
                        scenario_version_id,
                        measure_id,
                        store_id,
                        product_node_id,
                        time_period_id)
                    on requested.scenario_version_id = p.scenario_version_id
                   and requested.measure_id = p.measure_id
                   and requested.store_id = p.store_id
                   and requested.product_node_id = p.product_node_id
                   and requested.time_period_id = p.time_period_id
                where p.user_id = @userId;
                """;

            foreach (var coordinateChunk in coordinateList.Chunk(BulkWriteChunkSize))
            {
                await using var command = new NpgsqlCommand(sql, connection, transaction)
                {
                    CommandTimeout = 300
                };
                command.Parameters.AddWithValue("@userId", userId);
                command.Parameters.Add(CreateArrayParameter("@scenarioVersionIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.ScenarioVersionId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.MeasureId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.StoreId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.ProductNodeId).ToArray()));
                command.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, coordinateChunk.Select(coordinate => coordinate.TimePeriodId).ToArray()));

                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    cells.Add(ReadPlanningCellDirect(reader));
                }
            }

            return cells.Select(cell => cell.Clone()).ToList();
        }, cancellationToken);
    }

    private static async Task UpsertDraftPlanningCellsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        IReadOnlyList<PlanningCell> cells,
        CancellationToken cancellationToken)
    {
        if (cells.Count == 0)
        {
            return;
        }

        var orderedCells = cells
            .GroupBy(cell => cell.Coordinate.Key, StringComparer.Ordinal)
            .Select(group => group
                .OrderByDescending(cell => cell.RowVersion)
                .First())
            .OrderBy(cell => cell.Coordinate.MeasureId)
            .ThenBy(cell => cell.Coordinate.StoreId)
            .ThenBy(cell => cell.Coordinate.ProductNodeId)
            .ThenBy(cell => cell.Coordinate.TimePeriodId)
            .ToList();
        var stageTableName = $"planning_draft_cells_stage_{Guid.NewGuid():N}";

        await using (var createStageCommand = new NpgsqlCommand(
            $"""
            create temp table {stageTableName} (
                scenario_version_id bigint not null,
                user_id text not null,
                measure_id bigint not null,
                store_id bigint not null,
                product_node_id bigint not null,
                time_period_id bigint not null,
                input_value numeric null,
                override_value numeric null,
                is_system_generated_override integer not null,
                derived_value numeric not null,
                effective_value numeric not null,
                growth_factor numeric not null,
                is_locked integer not null,
                lock_reason text null,
                locked_by text null,
                row_version bigint not null,
                cell_kind text not null
            ) on commit drop;
            """,
            connection,
            transaction))
        {
            createStageCommand.CommandTimeout = 300;
            await createStageCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var importer = await connection.BeginBinaryImportAsync(
                         $"""
                          copy {stageTableName} (
                              scenario_version_id,
                              user_id,
                              measure_id,
                              store_id,
                              product_node_id,
                              time_period_id,
                              input_value,
                              override_value,
                              is_system_generated_override,
                              derived_value,
                              effective_value,
                              growth_factor,
                              is_locked,
                              lock_reason,
                              locked_by,
                              row_version,
                              cell_kind)
                          from stdin (format binary)
                          """,
                         cancellationToken))
        {
            foreach (var cell in orderedCells)
            {
                await importer.StartRowAsync(cancellationToken);
                await importer.WriteAsync(scenarioVersionId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(userId, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.MeasureId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.StoreId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.ProductNodeId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.Coordinate.TimePeriodId, NpgsqlDbType.Bigint, cancellationToken);
                if (cell.InputValue is { } inputValue)
                {
                    await importer.WriteAsync(inputValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (cell.OverrideValue is { } overrideValue)
                {
                    await importer.WriteAsync(overrideValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(cell.IsSystemGeneratedOverride ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync(cell.DerivedValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.EffectiveValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.GrowthFactor, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(cell.IsLocked ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                if (cell.LockReason is { } lockReason)
                {
                    await importer.WriteAsync(lockReason, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (cell.LockedBy is { } lockedBy)
                {
                    await importer.WriteAsync(lockedBy, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(cell.RowVersion, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(cell.CellKind, NpgsqlDbType.Text, cancellationToken);
            }

            await importer.CompleteAsync(cancellationToken);
        }

        await using (var mergeCommand = new NpgsqlCommand(
            $"""
            insert into planning_draft_cells (
                scenario_version_id,
                user_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                input_value,
                override_value,
                is_system_generated_override,
                derived_value,
                effective_value,
                growth_factor,
                is_locked,
                lock_reason,
                locked_by,
                row_version,
                cell_kind,
                updated_at)
            select
                source.scenario_version_id,
                source.user_id,
                source.measure_id,
                source.store_id,
                source.product_node_id,
                source.time_period_id,
                source.input_value,
                source.override_value,
                source.is_system_generated_override,
                source.derived_value,
                source.effective_value,
                source.growth_factor,
                source.is_locked,
                source.lock_reason,
                source.locked_by,
                source.row_version,
                source.cell_kind,
                now()
            from {stageTableName} as source
            on conflict (scenario_version_id, user_id, measure_id, store_id, product_node_id, time_period_id)
            do update set
                input_value = excluded.input_value,
                override_value = excluded.override_value,
                is_system_generated_override = excluded.is_system_generated_override,
                derived_value = excluded.derived_value,
                effective_value = excluded.effective_value,
                growth_factor = excluded.growth_factor,
                is_locked = excluded.is_locked,
                lock_reason = excluded.lock_reason,
                locked_by = excluded.locked_by,
                row_version = excluded.row_version,
                cell_kind = excluded.cell_kind,
                updated_at = excluded.updated_at;
            """,
            connection,
            transaction))
        {
            mergeCommand.CommandTimeout = 300;
            await mergeCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private Task<long> GetNextDraftCommandBatchIdDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("select nextval(pg_get_serial_sequence('planning_draft_command_batches', 'command_batch_id'));", connection, transaction);
                var value = await command.ExecuteScalarAsync(ct);
                return Convert.ToInt64(value);
            },
            cancellationToken);

    private async Task AppendDraftCommandBatchDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PlanningCommandBatch batch,
        CancellationToken cancellationToken)
    {
        await InvalidateDraftRedoStackDirectAsync(connection, transaction, batch.ScenarioVersionId, batch.UserId, batch.CommandBatchId, cancellationToken);

        await using (var headerCommand = new NpgsqlCommand(
            """
            insert into planning_draft_command_batches (
                command_batch_id,
                scenario_version_id,
                user_id,
                command_kind,
                command_scope_json,
                is_undone,
                superseded_by_batch_id,
                created_at,
                undone_at)
            values (
                @commandBatchId,
                @scenarioVersionId,
                @userId,
                @commandKind,
                @commandScopeJson,
                @isUndone,
                @supersededByBatchId,
                @createdAt,
                @undoneAt);
            """,
            connection,
            transaction))
        {
            headerCommand.Parameters.AddWithValue("@commandBatchId", batch.CommandBatchId);
            headerCommand.Parameters.AddWithValue("@scenarioVersionId", batch.ScenarioVersionId);
            headerCommand.Parameters.AddWithValue("@userId", batch.UserId);
            headerCommand.Parameters.AddWithValue("@commandKind", batch.CommandKind);
            headerCommand.Parameters.AddWithValue("@commandScopeJson", (object?)batch.CommandScopeJson ?? DBNull.Value);
            headerCommand.Parameters.AddWithValue("@isUndone", batch.IsUndone ? 1 : 0);
            headerCommand.Parameters.AddWithValue("@supersededByBatchId", (object?)batch.SupersededByBatchId ?? DBNull.Value);
            headerCommand.Parameters.AddWithValue("@createdAt", batch.CreatedAt);
            headerCommand.Parameters.AddWithValue("@undoneAt", (object?)batch.UndoneAt ?? DBNull.Value);
            await headerCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var deltas = batch.Deltas as IReadOnlyList<PlanningCommandCellDelta> ?? batch.Deltas.ToList();
        if (deltas.Count == 0)
        {
            return;
        }

        var stageTableName = $"planning_draft_command_deltas_stage_{Guid.NewGuid():N}";

        await using (var createStageCommand = new NpgsqlCommand(
            $"""
            create temp table {stageTableName} (
                scenario_version_id bigint not null,
                measure_id bigint not null,
                store_id bigint not null,
                product_node_id bigint not null,
                time_period_id bigint not null,
                old_input_value numeric null,
                new_input_value numeric null,
                old_override_value numeric null,
                new_override_value numeric null,
                old_is_system_generated_override integer not null,
                new_is_system_generated_override integer not null,
                old_derived_value numeric not null,
                new_derived_value numeric not null,
                old_effective_value numeric not null,
                new_effective_value numeric not null,
                old_growth_factor numeric not null,
                new_growth_factor numeric not null,
                old_is_locked integer not null,
                new_is_locked integer not null,
                old_lock_reason text null,
                new_lock_reason text null,
                old_locked_by text null,
                new_locked_by text null,
                old_row_version bigint not null,
                new_row_version bigint not null,
                old_cell_kind text not null,
                new_cell_kind text not null,
                change_kind text not null
            ) on commit drop;
            """,
            connection,
            transaction))
        {
            createStageCommand.CommandTimeout = 300;
            await createStageCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var importer = await connection.BeginBinaryImportAsync(
                         $"""
                          copy {stageTableName} (
                              scenario_version_id,
                              measure_id,
                              store_id,
                              product_node_id,
                              time_period_id,
                              old_input_value,
                              new_input_value,
                              old_override_value,
                              new_override_value,
                              old_is_system_generated_override,
                              new_is_system_generated_override,
                              old_derived_value,
                              new_derived_value,
                              old_effective_value,
                              new_effective_value,
                              old_growth_factor,
                              new_growth_factor,
                              old_is_locked,
                              new_is_locked,
                              old_lock_reason,
                              new_lock_reason,
                              old_locked_by,
                              new_locked_by,
                              old_row_version,
                              new_row_version,
                              old_cell_kind,
                              new_cell_kind,
                              change_kind)
                          from stdin (format binary)
                          """,
                         cancellationToken))
        {
            foreach (var delta in deltas)
            {
                await importer.StartRowAsync(cancellationToken);
                await importer.WriteAsync(delta.Coordinate.ScenarioVersionId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.Coordinate.MeasureId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.Coordinate.StoreId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.Coordinate.ProductNodeId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.Coordinate.TimePeriodId, NpgsqlDbType.Bigint, cancellationToken);
                if (delta.OldState.InputValue is { } oldInputValue)
                {
                    await importer.WriteAsync(oldInputValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.NewState.InputValue is { } newInputValue)
                {
                    await importer.WriteAsync(newInputValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.OldState.OverrideValue is { } oldOverrideValue)
                {
                    await importer.WriteAsync(oldOverrideValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.NewState.OverrideValue is { } newOverrideValue)
                {
                    await importer.WriteAsync(newOverrideValue, NpgsqlDbType.Numeric, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(delta.OldState.IsSystemGeneratedOverride ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync(delta.NewState.IsSystemGeneratedOverride ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync(delta.OldState.DerivedValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.NewState.DerivedValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.OldState.EffectiveValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.NewState.EffectiveValue, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.OldState.GrowthFactor, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.NewState.GrowthFactor, NpgsqlDbType.Numeric, cancellationToken);
                await importer.WriteAsync(delta.OldState.IsLocked ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync(delta.NewState.IsLocked ? 1 : 0, NpgsqlDbType.Integer, cancellationToken);
                if (delta.OldState.LockReason is { } oldLockReason)
                {
                    await importer.WriteAsync(oldLockReason, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.NewState.LockReason is { } newLockReason)
                {
                    await importer.WriteAsync(newLockReason, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.OldState.LockedBy is { } oldLockedBy)
                {
                    await importer.WriteAsync(oldLockedBy, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                if (delta.NewState.LockedBy is { } newLockedBy)
                {
                    await importer.WriteAsync(newLockedBy, NpgsqlDbType.Text, cancellationToken);
                }
                else
                {
                    await importer.WriteNullAsync(cancellationToken);
                }

                await importer.WriteAsync(delta.OldState.RowVersion, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.NewState.RowVersion, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(delta.OldState.CellKind, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(delta.NewState.CellKind, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(delta.ChangeKind, NpgsqlDbType.Text, cancellationToken);
            }

            await importer.CompleteAsync(cancellationToken);
        }

        await using (var mergeCommand = new NpgsqlCommand(
            $"""
            insert into planning_draft_command_cell_deltas (
                command_batch_id,
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                old_input_value,
                new_input_value,
                old_override_value,
                new_override_value,
                old_is_system_generated_override,
                new_is_system_generated_override,
                old_derived_value,
                new_derived_value,
                old_effective_value,
                new_effective_value,
                old_growth_factor,
                new_growth_factor,
                old_is_locked,
                new_is_locked,
                old_lock_reason,
                new_lock_reason,
                old_locked_by,
                new_locked_by,
                old_row_version,
                new_row_version,
                old_cell_kind,
                new_cell_kind,
                change_kind)
            select
                @commandBatchId,
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                old_input_value,
                new_input_value,
                old_override_value,
                new_override_value,
                old_is_system_generated_override,
                new_is_system_generated_override,
                old_derived_value,
                new_derived_value,
                old_effective_value,
                new_effective_value,
                old_growth_factor,
                new_growth_factor,
                old_is_locked,
                new_is_locked,
                old_lock_reason,
                new_lock_reason,
                old_locked_by,
                new_locked_by,
                old_row_version,
                new_row_version,
                old_cell_kind,
                new_cell_kind,
                change_kind
            from {stageTableName};
            """,
            connection,
            transaction))
        {
            mergeCommand.CommandTimeout = 300;
            mergeCommand.Parameters.AddWithValue("@commandBatchId", batch.CommandBatchId);
            await mergeCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private Task<PlanningUndoRedoAvailability> GetDraftUndoRedoAvailabilityDirectAsync(
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(async (connection, transaction, ct) =>
        {
            await using var command = new NpgsqlCommand(
            """
            with retained_history as (
                select is_undone
                from planning_draft_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select
                coalesce(sum(case when is_undone = 0 then 1 else 0 end), 0),
                coalesce(sum(case when is_undone = 1 then 1 else 0 end), 0)
            from retained_history;
            """,
            connection,
            transaction);
            command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
            command.Parameters.AddWithValue("@userId", userId);
            command.Parameters.AddWithValue("@limit", limit);

            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return new PlanningUndoRedoAvailability(false, false, 0, 0, limit);
            }

            var undoDepth = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
            var redoDepth = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            return new PlanningUndoRedoAvailability(undoDepth > 0, redoDepth > 0, undoDepth, redoDepth, limit);
        }, cancellationToken);

    private Task<PlanningCommandBatch?> UndoLatestDraftCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken) =>
        UndoOrRedoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, true, cancellationToken);

    private Task<PlanningCommandBatch?> RedoLatestDraftCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken) =>
        UndoOrRedoLatestDraftCommandDirectAsync(connection, transaction, scenarioVersionId, userId, limit, false, cancellationToken);

    private async Task<PlanningCommandBatch?> UndoOrRedoLatestDraftCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        bool applyOldState,
        CancellationToken cancellationToken)
    {
        var batch = applyOldState
            ? await LoadDraftUndoCandidateDirectAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken)
            : await LoadDraftRedoCandidateDirectAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken);
        if (batch is null)
        {
            return null;
        }

        var cells = batch.Deltas
            .Select(delta => (applyOldState ? delta.OldState : delta.NewState).ToPlanningCell(delta.Coordinate))
            .ToList();
        await UpsertDraftPlanningCellsAsync(connection, transaction, scenarioVersionId, userId, cells, cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            update planning_draft_command_batches
            set is_undone = @isUndone,
                undone_at = @undoneAt
            where command_batch_id = @commandBatchId;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@commandBatchId", batch.CommandBatchId);
        command.Parameters.AddWithValue("@isUndone", applyOldState ? 1 : 0);
        command.Parameters.AddWithValue("@undoneAt", applyOldState ? DateTimeOffset.UtcNow : (object)DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);

        return applyOldState
            ? batch with { IsUndone = true, UndoneAt = DateTimeOffset.UtcNow }
            : batch with { IsUndone = false, UndoneAt = null };
    }

    private static async Task InvalidateDraftRedoStackDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        long supersedingBatchId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            update planning_draft_command_batches
            set superseded_by_batch_id = @supersedingBatchId
            where scenario_version_id = @scenarioVersionId
              and user_id = @userId
              and is_undone = 1
              and superseded_by_batch_id is null;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@supersedingBatchId", supersedingBatchId);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("@userId", userId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadDraftUndoCandidateDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            with retained_history as (
                select command_batch_id
                from planning_draft_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select command_batch_id
            from planning_draft_command_batches
            where command_batch_id in (select command_batch_id from retained_history)
              and is_undone = 0
            order by command_batch_id desc
            limit 1;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("@userId", userId);
        command.Parameters.AddWithValue("@limit", limit);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull
            ? null
            : await LoadDraftCommandBatchDirectAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadDraftRedoCandidateDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            with retained_history as (
                select command_batch_id
                from planning_draft_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select command_batch_id
            from planning_draft_command_batches
            where command_batch_id in (select command_batch_id from retained_history)
              and is_undone = 1
            order by undone_at desc, command_batch_id desc
            limit 1;
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("@userId", userId);
        command.Parameters.AddWithValue("@limit", limit);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null or DBNull
            ? null
            : await LoadDraftCommandBatchDirectAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadDraftCommandBatchDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long commandBatchId,
        CancellationToken cancellationToken)
    {
        long batchId;
        long batchScenarioVersionId;
        string batchUserId;
        string commandKind;
        string? commandScopeJson;
        bool isUndone;
        long? supersededByBatchId;
        DateTimeOffset createdAt;
        DateTimeOffset? undoneAt;

        await using (var headerCommand = new NpgsqlCommand(
            """
            select command_batch_id,
                   scenario_version_id,
                   user_id,
                   command_kind,
                   command_scope_json,
                   is_undone,
                   superseded_by_batch_id,
                   created_at,
                   undone_at
            from planning_draft_command_batches
            where command_batch_id = @commandBatchId;
            """,
            connection,
            transaction))
        {
            headerCommand.Parameters.AddWithValue("@commandBatchId", commandBatchId);
            await using var headerReader = await headerCommand.ExecuteReaderAsync(cancellationToken);
            if (!await headerReader.ReadAsync(cancellationToken))
            {
                return null;
            }

            batchId = headerReader.GetInt64(0);
            batchScenarioVersionId = headerReader.GetInt64(1);
            batchUserId = headerReader.GetString(2);
            commandKind = headerReader.GetString(3);
            commandScopeJson = headerReader.IsDBNull(4) ? null : headerReader.GetString(4);
            isUndone = headerReader.GetInt32(5) == 1;
            supersededByBatchId = headerReader.IsDBNull(6) ? null : headerReader.GetInt64(6);
            createdAt = headerReader.GetFieldValue<DateTimeOffset>(7);
            undoneAt = headerReader.IsDBNull(8) ? null : headerReader.GetFieldValue<DateTimeOffset>(8);
        }

        var deltas = new List<PlanningCommandCellDelta>();
        await using (var deltaCommand = new NpgsqlCommand(
            """
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   old_input_value,
                   new_input_value,
                   old_override_value,
                   new_override_value,
                   old_is_system_generated_override,
                   new_is_system_generated_override,
                   old_derived_value,
                   new_derived_value,
                   old_effective_value,
                   new_effective_value,
                   old_growth_factor,
                   new_growth_factor,
                   old_is_locked,
                   new_is_locked,
                   old_lock_reason,
                   new_lock_reason,
                   old_locked_by,
                   new_locked_by,
                   old_row_version,
                   new_row_version,
                   old_cell_kind,
                   new_cell_kind,
                   change_kind
            from planning_draft_command_cell_deltas
            where command_batch_id = @commandBatchId
            order by command_delta_id asc;
            """,
            connection,
            transaction))
        {
            deltaCommand.Parameters.AddWithValue("@commandBatchId", commandBatchId);
            await using var deltaReader = await deltaCommand.ExecuteReaderAsync(cancellationToken);
            while (await deltaReader.ReadAsync(cancellationToken))
            {
                var coordinate = new PlanningCellCoordinate(
                    deltaReader.GetInt64(0),
                    deltaReader.GetInt64(1),
                    deltaReader.GetInt64(2),
                    deltaReader.GetInt64(3),
                    deltaReader.GetInt64(4));
                var oldState = new PlanningCellState(
                    ReadNullableDecimalDirect(deltaReader, 5),
                    ReadNullableDecimalDirect(deltaReader, 7),
                    deltaReader.GetInt32(9) == 1,
                    ReadDecimalDirect(deltaReader, 11),
                    ReadDecimalDirect(deltaReader, 13),
                    ReadDecimalDirect(deltaReader, 15),
                    deltaReader.GetInt32(17) == 1,
                    deltaReader.IsDBNull(19) ? null : deltaReader.GetString(19),
                    deltaReader.IsDBNull(21) ? null : deltaReader.GetString(21),
                    deltaReader.GetInt64(23),
                    deltaReader.GetString(25));
                var newState = new PlanningCellState(
                    ReadNullableDecimalDirect(deltaReader, 6),
                    ReadNullableDecimalDirect(deltaReader, 8),
                    deltaReader.GetInt32(10) == 1,
                    ReadDecimalDirect(deltaReader, 12),
                    ReadDecimalDirect(deltaReader, 14),
                    ReadDecimalDirect(deltaReader, 16),
                    deltaReader.GetInt32(18) == 1,
                    deltaReader.IsDBNull(20) ? null : deltaReader.GetString(20),
                    deltaReader.IsDBNull(22) ? null : deltaReader.GetString(22),
                    deltaReader.GetInt64(24),
                    deltaReader.GetString(26));
                deltas.Add(new PlanningCommandCellDelta(coordinate, oldState, newState, deltaReader.GetString(27)));
            }
        }

        return new PlanningCommandBatch(
            batchId,
            batchScenarioVersionId,
            batchUserId,
            commandKind,
            commandScopeJson,
            isUndone,
            supersededByBatchId,
            createdAt,
            undoneAt,
            deltas);
    }

    private static async Task CommitDraftDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        CancellationToken cancellationToken)
    {
        await using (var mergeCommand = new NpgsqlCommand(
            """
            insert into planning_cells (
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                input_value,
                override_value,
                is_system_generated_override,
                derived_value,
                effective_value,
                growth_factor,
                is_locked,
                lock_reason,
                locked_by,
                row_version,
                cell_kind)
            select
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                input_value,
                override_value,
                is_system_generated_override,
                derived_value,
                effective_value,
                growth_factor,
                is_locked,
                lock_reason,
                locked_by,
                row_version,
                cell_kind
            from planning_draft_cells
            where scenario_version_id = @scenarioVersionId
              and user_id = @userId
            on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
            do update set
                input_value = excluded.input_value,
                override_value = excluded.override_value,
                is_system_generated_override = excluded.is_system_generated_override,
                derived_value = excluded.derived_value,
                effective_value = excluded.effective_value,
                growth_factor = excluded.growth_factor,
                is_locked = excluded.is_locked,
                lock_reason = excluded.lock_reason,
                locked_by = excluded.locked_by,
                row_version = excluded.row_version,
                cell_kind = excluded.cell_kind;
            """,
            connection,
            transaction))
        {
            mergeCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
            mergeCommand.Parameters.AddWithValue("@userId", userId);
            await mergeCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var deleteCommand = new NpgsqlCommand(
            """
            delete from planning_draft_cells
            where scenario_version_id = @scenarioVersionId
              and user_id = @userId;
            """,
            connection,
            transaction);
        deleteCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteCommand.Parameters.AddWithValue("@userId", userId);
        await deleteCommand.ExecuteNonQueryAsync(cancellationToken);

        await using var deleteDeltaCommand = new NpgsqlCommand(
            """
            delete from planning_draft_command_cell_deltas
            where command_batch_id in (
                select command_batch_id
                from planning_draft_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
            );
            """,
            connection,
            transaction);
        deleteDeltaCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteDeltaCommand.Parameters.AddWithValue("@userId", userId);
        await deleteDeltaCommand.ExecuteNonQueryAsync(cancellationToken);

        await using var deleteBatchCommand = new NpgsqlCommand(
            """
            delete from planning_draft_command_batches
            where scenario_version_id = @scenarioVersionId
              and user_id = @userId;
            """,
            connection,
            transaction);
        deleteBatchCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteBatchCommand.Parameters.AddWithValue("@userId", userId);
        await deleteBatchCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}
