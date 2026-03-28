using Microsoft.Data.Sqlite;
using SalesPlanning.Api.Domain;

namespace SalesPlanning.Api.Infrastructure;

public sealed partial class SqlitePlanningRepository
{
    public async Task<long> GetNextCommandBatchIdAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "select coalesce(max(command_batch_id), 0) + 1 from planning_command_batches;";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(value);
    }

    public async Task AppendCommandBatchAsync(PlanningCommandBatch batch, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

            await InvalidateRedoStackAsync(connection, transaction, batch.ScenarioVersionId, batch.UserId, batch.CommandBatchId, cancellationToken);

            await using (var headerCommand = connection.CreateCommand())
            {
                headerCommand.Transaction = transaction;
                headerCommand.CommandText = """
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
                        $commandBatchId,
                        $scenarioVersionId,
                        $userId,
                        $commandKind,
                        $commandScopeJson,
                        $isUndone,
                        $supersededByBatchId,
                        $createdAt,
                        $undoneAt);
                    """;
                headerCommand.Parameters.AddWithValue("$commandBatchId", batch.CommandBatchId);
                headerCommand.Parameters.AddWithValue("$scenarioVersionId", batch.ScenarioVersionId);
                headerCommand.Parameters.AddWithValue("$userId", batch.UserId);
                headerCommand.Parameters.AddWithValue("$commandKind", batch.CommandKind);
                headerCommand.Parameters.AddWithValue("$commandScopeJson", (object?)batch.CommandScopeJson ?? DBNull.Value);
                headerCommand.Parameters.AddWithValue("$isUndone", batch.IsUndone ? 1 : 0);
                headerCommand.Parameters.AddWithValue("$supersededByBatchId", (object?)batch.SupersededByBatchId ?? DBNull.Value);
                headerCommand.Parameters.AddWithValue("$createdAt", batch.CreatedAt.ToString("O"));
                headerCommand.Parameters.AddWithValue("$undoneAt", batch.UndoneAt?.ToString("O") ?? (object)DBNull.Value);
                await headerCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var delta in batch.Deltas)
            {
                await InsertCommandDeltaAsync(connection, transaction, batch.CommandBatchId, delta, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<PlanningUndoRedoAvailability> GetUndoRedoAvailabilityAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            with retained_history as (
                select is_undone
                from planning_command_batches
                where scenario_version_id = $scenarioVersionId
                  and user_id = $userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit $limit
            )
            select
                coalesce(sum(case when is_undone = 0 then 1 else 0 end), 0),
                coalesce(sum(case when is_undone = 1 then 1 else 0 end), 0)
            from retained_history;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("$userId", userId);
        command.Parameters.AddWithValue("$limit", limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new PlanningUndoRedoAvailability(false, false, 0, 0, limit);
        }

        var undoDepth = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
        var redoDepth = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
        return new PlanningUndoRedoAvailability(undoDepth > 0, redoDepth > 0, undoDepth, redoDepth, limit);
    }

    public async Task<PlanningCommandBatch?> UndoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var batch = await LoadUndoCandidateAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken);
            if (batch is null)
            {
                await transaction.CommitAsync(cancellationToken);
                return null;
            }

            foreach (var delta in batch.Deltas)
            {
                await UpsertCellAsync(connection, transaction, delta.OldState.ToPlanningCell(delta.Coordinate), cancellationToken);
            }

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    update planning_command_batches
                    set is_undone = 1,
                        undone_at = $undoneAt
                    where command_batch_id = $commandBatchId;
                    """;
                command.Parameters.AddWithValue("$commandBatchId", batch.CommandBatchId);
                command.Parameters.AddWithValue("$undoneAt", DateTimeOffset.UtcNow.ToString("O"));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            return batch with { IsUndone = true, UndoneAt = DateTimeOffset.UtcNow };
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<PlanningCommandBatch?> RedoLatestCommandAsync(long scenarioVersionId, string userId, int limit, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            var batch = await LoadRedoCandidateAsync(connection, transaction, scenarioVersionId, userId, limit, cancellationToken);
            if (batch is null)
            {
                await transaction.CommitAsync(cancellationToken);
                return null;
            }

            foreach (var delta in batch.Deltas)
            {
                await UpsertCellAsync(connection, transaction, delta.NewState.ToPlanningCell(delta.Coordinate), cancellationToken);
            }

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    update planning_command_batches
                    set is_undone = 0,
                        undone_at = null
                    where command_batch_id = $commandBatchId;
                    """;
                command.Parameters.AddWithValue("$commandBatchId", batch.CommandBatchId);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            return batch with { IsUndone = false, UndoneAt = null };
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async Task InsertCommandDeltaAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long commandBatchId,
        PlanningCommandCellDelta delta,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
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
            values (
                $commandBatchId,
                $scenarioVersionId,
                $measureId,
                $storeId,
                $productNodeId,
                $timePeriodId,
                $oldInputValue,
                $newInputValue,
                $oldOverrideValue,
                $newOverrideValue,
                $oldIsSystemGeneratedOverride,
                $newIsSystemGeneratedOverride,
                $oldDerivedValue,
                $newDerivedValue,
                $oldEffectiveValue,
                $newEffectiveValue,
                $oldGrowthFactor,
                $newGrowthFactor,
                $oldIsLocked,
                $newIsLocked,
                $oldLockReason,
                $newLockReason,
                $oldLockedBy,
                $newLockedBy,
                $oldRowVersion,
                $newRowVersion,
                $oldCellKind,
                $newCellKind,
                $changeKind);
            """;
        command.Parameters.AddWithValue("$commandBatchId", commandBatchId);
        command.Parameters.AddWithValue("$scenarioVersionId", delta.Coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("$measureId", delta.Coordinate.MeasureId);
        command.Parameters.AddWithValue("$storeId", delta.Coordinate.StoreId);
        command.Parameters.AddWithValue("$productNodeId", delta.Coordinate.ProductNodeId);
        command.Parameters.AddWithValue("$timePeriodId", delta.Coordinate.TimePeriodId);
        command.Parameters.AddWithValue("$oldInputValue", (object?)delta.OldState.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$newInputValue", (object?)delta.NewState.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$oldOverrideValue", (object?)delta.OldState.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$newOverrideValue", (object?)delta.NewState.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$oldIsSystemGeneratedOverride", delta.OldState.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("$newIsSystemGeneratedOverride", delta.NewState.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("$oldDerivedValue", delta.OldState.DerivedValue);
        command.Parameters.AddWithValue("$newDerivedValue", delta.NewState.DerivedValue);
        command.Parameters.AddWithValue("$oldEffectiveValue", delta.OldState.EffectiveValue);
        command.Parameters.AddWithValue("$newEffectiveValue", delta.NewState.EffectiveValue);
        command.Parameters.AddWithValue("$oldGrowthFactor", delta.OldState.GrowthFactor);
        command.Parameters.AddWithValue("$newGrowthFactor", delta.NewState.GrowthFactor);
        command.Parameters.AddWithValue("$oldIsLocked", delta.OldState.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("$newIsLocked", delta.NewState.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("$oldLockReason", (object?)delta.OldState.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("$newLockReason", (object?)delta.NewState.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("$oldLockedBy", (object?)delta.OldState.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("$newLockedBy", (object?)delta.NewState.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("$oldRowVersion", delta.OldState.RowVersion);
        command.Parameters.AddWithValue("$newRowVersion", delta.NewState.RowVersion);
        command.Parameters.AddWithValue("$oldCellKind", delta.OldState.CellKind);
        command.Parameters.AddWithValue("$newCellKind", delta.NewState.CellKind);
        command.Parameters.AddWithValue("$changeKind", delta.ChangeKind);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InvalidateRedoStackAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        string userId,
        long supersedingBatchId,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            update planning_command_batches
            set superseded_by_batch_id = $supersedingBatchId
            where scenario_version_id = $scenarioVersionId
              and user_id = $userId
              and is_undone = 1
              and superseded_by_batch_id is null;
            """;
        command.Parameters.AddWithValue("$supersedingBatchId", supersedingBatchId);
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("$userId", userId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadUndoCandidateAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            with retained_history as (
                select command_batch_id
                from planning_command_batches
                where scenario_version_id = $scenarioVersionId
                  and user_id = $userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit $limit
            )
            select command_batch_id
            from planning_command_batches
            where command_batch_id in (select command_batch_id from retained_history)
              and is_undone = 0
            order by command_batch_id desc
            limit 1;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("$userId", userId);
        command.Parameters.AddWithValue("$limit", limit);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null || value is DBNull
            ? null
            : await LoadCommandBatchAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadRedoCandidateAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        string userId,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            with retained_history as (
                select command_batch_id
                from planning_command_batches
                where scenario_version_id = $scenarioVersionId
                  and user_id = $userId
                  and superseded_by_batch_id is null
                order by command_batch_id desc
                limit $limit
            )
            select command_batch_id
            from planning_command_batches
            where command_batch_id in (select command_batch_id from retained_history)
              and is_undone = 1
            order by undone_at desc, command_batch_id desc
            limit 1;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        command.Parameters.AddWithValue("$userId", userId);
        command.Parameters.AddWithValue("$limit", limit);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is null || value is DBNull
            ? null
            : await LoadCommandBatchAsync(connection, transaction, Convert.ToInt64(value), cancellationToken);
    }

    private static async Task<PlanningCommandBatch?> LoadCommandBatchAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long commandBatchId,
        CancellationToken cancellationToken)
    {
        await using var headerCommand = connection.CreateCommand();
        headerCommand.Transaction = transaction;
        headerCommand.CommandText = """
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
            where command_batch_id = $commandBatchId;
            """;
        headerCommand.Parameters.AddWithValue("$commandBatchId", commandBatchId);

        await using var headerReader = await headerCommand.ExecuteReaderAsync(cancellationToken);
        if (!await headerReader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var batchId = headerReader.GetInt64(0);
        var batchScenarioVersionId = headerReader.GetInt64(1);
        var batchUserId = headerReader.GetString(2);
        var commandKind = headerReader.GetString(3);
        var commandScopeJson = headerReader.IsDBNull(4) ? null : headerReader.GetString(4);
        var isUndone = headerReader.GetInt64(5) == 1;
        long? supersededByBatchId = headerReader.IsDBNull(6) ? null : headerReader.GetInt64(6);
        var createdAt = DateTimeOffset.Parse(headerReader.GetString(7));
        DateTimeOffset? undoneAt = headerReader.IsDBNull(8) ? null : DateTimeOffset.Parse(headerReader.GetString(8));
        await headerReader.DisposeAsync();

        var deltas = new List<PlanningCommandCellDelta>();
        await using var deltaCommand = connection.CreateCommand();
        deltaCommand.Transaction = transaction;
        deltaCommand.CommandText = """
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
            where command_batch_id = $commandBatchId
            order by command_delta_id asc;
            """;
        deltaCommand.Parameters.AddWithValue("$commandBatchId", commandBatchId);

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
                ReadNullableDecimal(deltaReader, 5),
                ReadNullableDecimal(deltaReader, 7),
                deltaReader.GetInt64(9) == 1,
                ReadDecimal(deltaReader, 11),
                ReadDecimal(deltaReader, 13),
                ReadDecimal(deltaReader, 15),
                deltaReader.GetInt64(17) == 1,
                deltaReader.IsDBNull(19) ? null : deltaReader.GetString(19),
                deltaReader.IsDBNull(21) ? null : deltaReader.GetString(21),
                deltaReader.GetInt64(23),
                deltaReader.GetString(25));
            var newState = new PlanningCellState(
                ReadNullableDecimal(deltaReader, 6),
                ReadNullableDecimal(deltaReader, 8),
                deltaReader.GetInt64(10) == 1,
                ReadDecimal(deltaReader, 12),
                ReadDecimal(deltaReader, 14),
                ReadDecimal(deltaReader, 16),
                deltaReader.GetInt64(18) == 1,
                deltaReader.IsDBNull(20) ? null : deltaReader.GetString(20),
                deltaReader.IsDBNull(22) ? null : deltaReader.GetString(22),
                deltaReader.GetInt64(24),
                deltaReader.GetString(26));
            deltas.Add(new PlanningCommandCellDelta(coordinate, oldState, newState, deltaReader.GetString(27)));
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

    private static decimal? ReadNullableDecimal(SqliteDataReader reader, int ordinal)
    {
        return reader.IsDBNull(ordinal) ? null : ReadDecimal(reader, ordinal);
    }

    private static async Task ClearPlanningCommandHistoryAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long? scenarioVersionId,
        CancellationToken cancellationToken)
    {
        if (scenarioVersionId is null)
        {
            foreach (var tableName in new[] { "planning_command_cell_deltas", "planning_command_batches" })
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = $"delete from {tableName};";
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return;
        }

        await using (var deleteDeltaCommand = connection.CreateCommand())
        {
            deleteDeltaCommand.Transaction = transaction;
            deleteDeltaCommand.CommandText = """
                delete from planning_command_cell_deltas
                where scenario_version_id = $scenarioVersionId;
                """;
            deleteDeltaCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId.Value);
            await deleteDeltaCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var deleteBatchCommand = connection.CreateCommand())
        {
            deleteBatchCommand.Transaction = transaction;
            deleteBatchCommand.CommandText = """
                delete from planning_command_batches
                where scenario_version_id = $scenarioVersionId;
                """;
            deleteBatchCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId.Value);
            await deleteBatchCommand.ExecuteNonQueryAsync(cancellationToken);
        }
    }
}
