using Npgsql;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository
{
    private Task<long> GetNextActionIdDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("select coalesce(max(action_id), 1000) + 1 from audits;", connection, transaction);
                var value = await command.ExecuteScalarAsync(ct);
                return Convert.ToInt64(value);
            },
            cancellationToken);

    private async Task AppendAuditDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PlanningActionAudit audit,
        CancellationToken cancellationToken)
    {
        await using (var command = new NpgsqlCommand(
            """
            insert into audits (action_id, action_type, method, user_id, comment, created_at)
            values (@actionId, @actionType, @method, @userId, @comment, @createdAt);
            """,
            connection,
            transaction))
        {
            command.Parameters.AddWithValue("@actionId", audit.ActionId);
            command.Parameters.AddWithValue("@actionType", audit.ActionType);
            command.Parameters.AddWithValue("@method", audit.Method);
            command.Parameters.AddWithValue("@userId", audit.UserId);
            command.Parameters.AddWithValue("@comment", (object?)audit.Comment ?? DBNull.Value);
            command.Parameters.AddWithValue("@createdAt", audit.CreatedAt.ToString("O"));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        var nextAuditDeltaId = await GetNextAuditDeltaIdDirectAsync(connection, transaction, cancellationToken);
        foreach (var delta in audit.Deltas)
        {
            await using var command = new NpgsqlCommand(
                """
                insert into audit_deltas (
                    audit_delta_id,
                    action_id,
                    scenario_version_id,
                    measure_id,
                    store_id,
                    product_node_id,
                    time_period_id,
                    old_value,
                    new_value,
                    was_locked,
                    change_kind)
                values (
                    @auditDeltaId,
                    @actionId,
                    @scenarioVersionId,
                    @measureId,
                    @storeId,
                    @productNodeId,
                    @timePeriodId,
                    @oldValue,
                    @newValue,
                    @wasLocked,
                    @changeKind);
                """,
                connection,
                transaction);
            command.Parameters.AddWithValue("@auditDeltaId", nextAuditDeltaId);
            command.Parameters.AddWithValue("@actionId", audit.ActionId);
            command.Parameters.AddWithValue("@scenarioVersionId", delta.Coordinate.ScenarioVersionId);
            command.Parameters.AddWithValue("@measureId", delta.Coordinate.MeasureId);
            command.Parameters.AddWithValue("@storeId", delta.Coordinate.StoreId);
            command.Parameters.AddWithValue("@productNodeId", delta.Coordinate.ProductNodeId);
            command.Parameters.AddWithValue("@timePeriodId", delta.Coordinate.TimePeriodId);
            command.Parameters.AddWithValue("@oldValue", delta.OldValue);
            command.Parameters.AddWithValue("@newValue", delta.NewValue);
            command.Parameters.AddWithValue("@wasLocked", delta.WasLocked ? 1 : 0);
            command.Parameters.AddWithValue("@changeKind", delta.ChangeKind);
            await command.ExecuteNonQueryAsync(cancellationToken);
            nextAuditDeltaId += 1;
        }
    }

    private Task<IReadOnlyList<PlanningActionAudit>> GetAuditDirectAsync(
        long scenarioVersionId,
        long measureId,
        long storeId,
        long productNodeId,
        CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync<IReadOnlyList<PlanningActionAudit>>(async (connection, transaction, ct) =>
        {
            var audits = new List<PlanningActionAudit>();
            var headers = new List<(long ActionId, string ActionType, string Method, string UserId, string? Comment, DateTimeOffset CreatedAt)>();

            await using (var auditCommand = new NpgsqlCommand(
                """
                select distinct a.action_id,
                                a.action_type,
                                a.method,
                                a.user_id,
                                a.comment,
                                a.created_at
                from audits a
                inner join audit_deltas d on d.action_id = a.action_id
                where d.scenario_version_id = @scenarioVersionId
                  and d.measure_id = @measureId
                  and d.store_id = @storeId
                  and d.product_node_id = @productNodeId
                order by a.created_at desc;
                """,
                connection,
                transaction))
            {
                auditCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                auditCommand.Parameters.AddWithValue("@measureId", measureId);
                auditCommand.Parameters.AddWithValue("@storeId", storeId);
                auditCommand.Parameters.AddWithValue("@productNodeId", productNodeId);

                await using var reader = await auditCommand.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    headers.Add((
                        reader.GetInt64(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.IsDBNull(4) ? null : reader.GetString(4),
                        DateTimeOffset.Parse(reader.GetString(5))));
                }
            }

            foreach (var header in headers)
            {
                var deltas = new List<PlanningCellDeltaAudit>();
                await using var deltaCommand = new NpgsqlCommand(
                    """
                    select scenario_version_id,
                           measure_id,
                           store_id,
                           product_node_id,
                           time_period_id,
                           old_value,
                           new_value,
                           was_locked,
                           change_kind
                    from audit_deltas
                    where action_id = @actionId
                    order by audit_delta_id asc;
                    """,
                    connection,
                    transaction);
                deltaCommand.Parameters.AddWithValue("@actionId", header.ActionId);

                await using var reader = await deltaCommand.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    deltas.Add(new PlanningCellDeltaAudit(
                        new PlanningCellCoordinate(
                            reader.GetInt64(0),
                            reader.GetInt64(1),
                            reader.GetInt64(2),
                            reader.GetInt64(3),
                            reader.GetInt64(4)),
                        ReadDecimalDirect(reader, 5),
                        ReadDecimalDirect(reader, 6),
                        reader.GetInt32(7) == 1,
                        reader.GetString(8)));
                }

                audits.Add(new PlanningActionAudit(
                    header.ActionId,
                    header.ActionType,
                    header.Method,
                    header.UserId,
                    header.Comment,
                    header.CreatedAt,
                    deltas));
            }

            return audits;
        }, cancellationToken);

    private Task<long> GetNextCommandBatchIdDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("select coalesce(max(command_batch_id), 0) + 1 from planning_command_batches;", connection, transaction);
                var value = await command.ExecuteScalarAsync(ct);
                return Convert.ToInt64(value);
            },
            cancellationToken);

    private async Task AppendCommandBatchDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PlanningCommandBatch batch,
        CancellationToken cancellationToken)
    {
        await InvalidateRedoStackDirectAsync(connection, transaction, batch.ScenarioVersionId, batch.UserId, batch.CommandBatchId, cancellationToken);

        await using (var headerCommand = new NpgsqlCommand(
            """
            insert into planning_command_batches (
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

        var nextCommandDeltaId = await GetNextCommandDeltaIdDirectAsync(connection, transaction, cancellationToken);
        foreach (var delta in batch.Deltas)
        {
            await InsertCommandDeltaDirectAsync(connection, transaction, nextCommandDeltaId, batch.CommandBatchId, delta, cancellationToken);
            nextCommandDeltaId += 1;
        }
    }

    private static async Task<long> GetNextAuditDeltaIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("select coalesce(max(audit_delta_id), 0) + 1 from audit_deltas;", connection, transaction);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(value);
    }

    private static async Task<long> GetNextCommandDeltaIdDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("select coalesce(max(command_delta_id), 0) + 1 from planning_command_cell_deltas;", connection, transaction);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(value);
    }

    private Task<PlanningCommandBatch?> UndoLatestCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken) =>
        UndoOrRedoLatestCommandDirectAsync(
            connection,
            transaction,
            scenarioVersionId,
            userId,
            limit,
            applyOldState: true,
            cancellationToken);

    private Task<PlanningCommandBatch?> RedoLatestCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken) =>
        UndoOrRedoLatestCommandDirectAsync(
            connection,
            transaction,
            scenarioVersionId,
            userId,
            limit,
            applyOldState: false,
            cancellationToken);

    private async Task<PlanningCommandBatch?> UndoOrRedoLatestCommandDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        bool applyOldState,
        CancellationToken cancellationToken)
    {
        var batch = applyOldState
            ? await LoadUndoCandidateDirectAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken)
            : await LoadRedoCandidateDirectAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken);
        if (batch is null)
        {
            return null;
        }

        var cells = batch.Deltas
            .Select(delta => (applyOldState ? delta.OldState : delta.NewState).ToPlanningCell(delta.Coordinate))
            .ToList();
        await UpsertPlanningCellsAsync(connection, transaction, cells, cancellationToken);

        await using var command = new NpgsqlCommand(
            """
            update planning_command_batches
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

    private static async Task InsertCommandDeltaDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long commandDeltaId,
        long commandBatchId,
        PlanningCommandCellDelta delta,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            insert into planning_command_cell_deltas (
                command_delta_id,
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
            values (
                @commandDeltaId,
                @commandBatchId,
                @scenarioVersionId,
                @measureId,
                @storeId,
                @productNodeId,
                @timePeriodId,
                @oldInputValue,
                @newInputValue,
                @oldOverrideValue,
                @newOverrideValue,
                @oldIsSystemGeneratedOverride,
                @newIsSystemGeneratedOverride,
                @oldDerivedValue,
                @newDerivedValue,
                @oldEffectiveValue,
                @newEffectiveValue,
                @oldGrowthFactor,
                @newGrowthFactor,
                @oldIsLocked,
                @newIsLocked,
                @oldLockReason,
                @newLockReason,
                @oldLockedBy,
                @newLockedBy,
                @oldRowVersion,
                @newRowVersion,
                @oldCellKind,
                @newCellKind,
                @changeKind);
            """,
            connection,
            transaction);
        command.Parameters.AddWithValue("@commandDeltaId", commandDeltaId);
        command.Parameters.AddWithValue("@commandBatchId", commandBatchId);
        command.Parameters.AddWithValue("@scenarioVersionId", delta.Coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("@measureId", delta.Coordinate.MeasureId);
        command.Parameters.AddWithValue("@storeId", delta.Coordinate.StoreId);
        command.Parameters.AddWithValue("@productNodeId", delta.Coordinate.ProductNodeId);
        command.Parameters.AddWithValue("@timePeriodId", delta.Coordinate.TimePeriodId);
        command.Parameters.AddWithValue("@oldInputValue", (object?)delta.OldState.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("@newInputValue", (object?)delta.NewState.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("@oldOverrideValue", (object?)delta.OldState.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("@newOverrideValue", (object?)delta.NewState.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("@oldIsSystemGeneratedOverride", delta.OldState.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("@newIsSystemGeneratedOverride", delta.NewState.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("@oldDerivedValue", delta.OldState.DerivedValue);
        command.Parameters.AddWithValue("@newDerivedValue", delta.NewState.DerivedValue);
        command.Parameters.AddWithValue("@oldEffectiveValue", delta.OldState.EffectiveValue);
        command.Parameters.AddWithValue("@newEffectiveValue", delta.NewState.EffectiveValue);
        command.Parameters.AddWithValue("@oldGrowthFactor", delta.OldState.GrowthFactor);
        command.Parameters.AddWithValue("@newGrowthFactor", delta.NewState.GrowthFactor);
        command.Parameters.AddWithValue("@oldIsLocked", delta.OldState.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("@newIsLocked", delta.NewState.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("@oldLockReason", (object?)delta.OldState.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("@newLockReason", (object?)delta.NewState.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("@oldLockedBy", (object?)delta.OldState.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("@newLockedBy", (object?)delta.NewState.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("@oldRowVersion", delta.OldState.RowVersion);
        command.Parameters.AddWithValue("@newRowVersion", delta.NewState.RowVersion);
        command.Parameters.AddWithValue("@oldCellKind", delta.OldState.CellKind);
        command.Parameters.AddWithValue("@newCellKind", delta.NewState.CellKind);
        command.Parameters.AddWithValue("@changeKind", delta.ChangeKind);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InvalidateRedoStackDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        string userId,
        long supersedingBatchId,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            update planning_command_batches
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

    private static async Task<PlanningCommandBatch?> LoadUndoCandidateDirectAsync(
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
                from planning_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select command_batch_id
            from planning_command_batches
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
            : await LoadCommandBatchDirectAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadRedoCandidateDirectAsync(
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
                from planning_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = @userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit @limit
            )
            select command_batch_id
            from planning_command_batches
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
            : await LoadCommandBatchDirectAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadCommandBatchDirectAsync(
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
            from planning_command_batches
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
            from planning_command_cell_deltas
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

    private static decimal? ReadNullableDecimalDirect(NpgsqlDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : ReadDecimalDirect(reader, ordinal);
    }
}
