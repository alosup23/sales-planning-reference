using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresBackedSqlitePlanningRepository
{
    private Task<long> GetNextActionIdDirectAsync(CancellationToken cancellationToken) =>
        ExecuteDirectReadAsync(
            async (connection, transaction, ct) =>
            {
                await using var command = new NpgsqlCommand("select nextval('audits_action_id_seq');", connection, transaction);
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

        var deltas = audit.Deltas as IReadOnlyList<PlanningCellDeltaAudit> ?? audit.Deltas.ToList();
        if (deltas.Count == 0)
        {
            return;
        }

        const string sql = """
            insert into audit_deltas (
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
            select
                @actionId,
                input_rows.scenario_version_id,
                input_rows.measure_id,
                input_rows.store_id,
                input_rows.product_node_id,
                input_rows.time_period_id,
                input_rows.old_value,
                input_rows.new_value,
                input_rows.was_locked,
                input_rows.change_kind
            from unnest(
                @scenarioVersionIds,
                @measureIds,
                @storeIds,
                @productNodeIds,
                @timePeriodIds,
                @oldValues,
                @newValues,
                @wasLockedValues,
                @changeKinds)
                as input_rows(
                    scenario_version_id,
                    measure_id,
                    store_id,
                    product_node_id,
                    time_period_id,
                    old_value,
                    new_value,
                    was_locked,
                    change_kind);
            """;

        foreach (var deltaChunk in deltas.Chunk(BulkWriteChunkSize))
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction)
            {
                CommandTimeout = 300
            };
            command.Parameters.AddWithValue("@actionId", audit.ActionId);
            command.Parameters.Add(CreateArrayParameter("@scenarioVersionIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.ScenarioVersionId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.MeasureId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.StoreId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.ProductNodeId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.TimePeriodId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@wasLockedValues", NpgsqlDbType.Integer, deltaChunk.Select(delta => delta.WasLocked ? 1 : 0).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@changeKinds", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.ChangeKind).ToArray()));
            await command.ExecuteNonQueryAsync(cancellationToken);
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
                await using var command = new NpgsqlCommand("select nextval(pg_get_serial_sequence('planning_command_batches', 'command_batch_id'));", connection, transaction);
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

        var deltas = batch.Deltas as IReadOnlyList<PlanningCommandCellDelta> ?? batch.Deltas.ToList();
        if (deltas.Count == 0)
        {
            return;
        }

        const string sql = """
            insert into planning_command_cell_deltas (
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
                input_rows.scenario_version_id,
                input_rows.measure_id,
                input_rows.store_id,
                input_rows.product_node_id,
                input_rows.time_period_id,
                input_rows.old_input_value,
                input_rows.new_input_value,
                input_rows.old_override_value,
                input_rows.new_override_value,
                input_rows.old_is_system_generated_override,
                input_rows.new_is_system_generated_override,
                input_rows.old_derived_value,
                input_rows.new_derived_value,
                input_rows.old_effective_value,
                input_rows.new_effective_value,
                input_rows.old_growth_factor,
                input_rows.new_growth_factor,
                input_rows.old_is_locked,
                input_rows.new_is_locked,
                input_rows.old_lock_reason,
                input_rows.new_lock_reason,
                input_rows.old_locked_by,
                input_rows.new_locked_by,
                input_rows.old_row_version,
                input_rows.new_row_version,
                input_rows.old_cell_kind,
                input_rows.new_cell_kind,
                input_rows.change_kind
            from unnest(
                @scenarioVersionIds,
                @measureIds,
                @storeIds,
                @productNodeIds,
                @timePeriodIds,
                @oldInputValues,
                @newInputValues,
                @oldOverrideValues,
                @newOverrideValues,
                @oldIsSystemGeneratedOverrideValues,
                @newIsSystemGeneratedOverrideValues,
                @oldDerivedValues,
                @newDerivedValues,
                @oldEffectiveValues,
                @newEffectiveValues,
                @oldGrowthFactors,
                @newGrowthFactors,
                @oldIsLockedValues,
                @newIsLockedValues,
                @oldLockReasons,
                @newLockReasons,
                @oldLockedByValues,
                @newLockedByValues,
                @oldRowVersions,
                @newRowVersions,
                @oldCellKinds,
                @newCellKinds,
                @changeKinds)
                as input_rows(
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
                    change_kind);
            """;

        foreach (var deltaChunk in deltas.Chunk(BulkWriteChunkSize))
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction)
            {
                CommandTimeout = 300
            };
            command.Parameters.AddWithValue("@commandBatchId", batch.CommandBatchId);
            command.Parameters.Add(CreateArrayParameter("@scenarioVersionIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.ScenarioVersionId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.MeasureId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.StoreId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.ProductNodeId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.Coordinate.TimePeriodId).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldInputValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldState.InputValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newInputValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewState.InputValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldOverrideValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldState.OverrideValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newOverrideValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewState.OverrideValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldIsSystemGeneratedOverrideValues", NpgsqlDbType.Integer, deltaChunk.Select(delta => delta.OldState.IsSystemGeneratedOverride ? 1 : 0).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newIsSystemGeneratedOverrideValues", NpgsqlDbType.Integer, deltaChunk.Select(delta => delta.NewState.IsSystemGeneratedOverride ? 1 : 0).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldDerivedValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldState.DerivedValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newDerivedValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewState.DerivedValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldEffectiveValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldState.EffectiveValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newEffectiveValues", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewState.EffectiveValue).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldGrowthFactors", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.OldState.GrowthFactor).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newGrowthFactors", NpgsqlDbType.Numeric, deltaChunk.Select(delta => delta.NewState.GrowthFactor).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldIsLockedValues", NpgsqlDbType.Integer, deltaChunk.Select(delta => delta.OldState.IsLocked ? 1 : 0).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newIsLockedValues", NpgsqlDbType.Integer, deltaChunk.Select(delta => delta.NewState.IsLocked ? 1 : 0).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldLockReasons", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.OldState.LockReason).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newLockReasons", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.NewState.LockReason).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldLockedByValues", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.OldState.LockedBy).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newLockedByValues", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.NewState.LockedBy).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldRowVersions", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.OldState.RowVersion).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newRowVersions", NpgsqlDbType.Bigint, deltaChunk.Select(delta => delta.NewState.RowVersion).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@oldCellKinds", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.OldState.CellKind).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@newCellKinds", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.NewState.CellKind).ToArray()));
            command.Parameters.Add(CreateArrayParameter("@changeKinds", NpgsqlDbType.Text, deltaChunk.Select(delta => delta.ChangeKind).ToArray()));
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
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
