using Npgsql;
using NpgsqlTypes;
using SalesPlanning.Api.Domain;
using SalesPlanning.Api.Security;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed partial class PostgresPlanningRepository
{
    private async Task<IReadOnlyList<PlanningCell>> GetDraftCellsDirectAsync(
        long scenarioVersionId,
        PlanningUserIdentity.PlanningUserContext userContext,
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
            var requestedKeys = coordinateList
                .Select(coordinate => coordinate.Key)
                .ToHashSet(StringComparer.Ordinal);
            var scenarioDraftCells = new List<PlanningCell>();
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
                where p.scenario_version_id = @scenarioVersionId
                  and p.user_id = any(@candidateUserIds)
                order by
                    p.scenario_version_id,
                    p.measure_id,
                    p.store_id,
                    p.product_node_id,
                    p.time_period_id,
                    case when p.user_id = @primaryUserId then 0 else 1 end,
                    p.updated_at desc,
                    p.row_version desc;
                """;
            await using (var command = new NpgsqlCommand(sql, connection, transaction))
            {
                command.CommandTimeout = 300;
                command.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                command.Parameters.AddWithValue("@primaryUserId", userContext.PrimaryUserId);
                command.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));

                await using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    scenarioDraftCells.Add(ReadPlanningCellDirect(reader));
                }
            }

            var distinctCells = scenarioDraftCells
                .DistinctBy(cell => cell.Coordinate.Key)
                .Where(cell => requestedKeys.Contains(cell.Coordinate.Key))
                .Select(cell => cell.Clone())
                .ToList();

            if (coordinateList.Count <= 500)
            {
                _logger.LogInformation(
                    "Loaded {DraftCellCount} draft cells for scenario {ScenarioVersionId} user {UserId} from {RequestedCoordinateCount} requested coordinates. Total draft rows for user: {TotalDraftRowsForUser}.",
                    distinctCells.Count,
                    scenarioVersionId,
                    userContext.PrimaryUserId,
                    coordinateList.Count,
                    scenarioDraftCells.Count);
            }

            return distinctCells;
        }, cancellationToken);
    }

    private async Task UpsertDraftPlanningCellsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        PlanningUserIdentity.PlanningUserContext userContext,
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
        const string deleteAliasSql = """
            delete from planning_draft_cells as target
            using unnest(
                @measureIds,
                @storeIds,
                @productNodeIds,
                @timePeriodIds)
                as source(
                    measure_id,
                    store_id,
                    product_node_id,
                    time_period_id)
            where target.scenario_version_id = @scenarioVersionId
              and target.user_id = any(@candidateUserIds)
              and target.user_id <> @primaryUserId
              and target.measure_id = source.measure_id
              and target.store_id = source.store_id
              and target.product_node_id = source.product_node_id
              and target.time_period_id = source.time_period_id;
            """;

        const string updatePrimarySql = """
            update planning_draft_cells as target
            set input_value = source.input_value,
                override_value = source.override_value,
                is_system_generated_override = source.is_system_generated_override,
                derived_value = source.derived_value,
                effective_value = source.effective_value,
                growth_factor = source.growth_factor,
                is_locked = source.is_locked,
                lock_reason = source.lock_reason,
                locked_by = source.locked_by,
                row_version = source.row_version,
                cell_kind = source.cell_kind,
                updated_at = now()
            from %STAGE_TABLE% as source
            where target.scenario_version_id = source.scenario_version_id
              and target.user_id = source.user_id
              and target.measure_id = source.measure_id
              and target.store_id = source.store_id
              and target.product_node_id = source.product_node_id
              and target.time_period_id = source.time_period_id;
            """;
        const string selectExistingPrimarySql = """
            select requested.measure_id,
                   requested.store_id,
                   requested.product_node_id,
                   requested.time_period_id
            from planning_draft_cells as target
            inner join unnest(
                @measureIds,
                @storeIds,
                @productNodeIds,
                @timePeriodIds)
                as requested(
                    measure_id,
                    store_id,
                    product_node_id,
                    time_period_id)
                on target.measure_id = requested.measure_id
               and target.store_id = requested.store_id
               and target.product_node_id = requested.product_node_id
               and target.time_period_id = requested.time_period_id
            where target.scenario_version_id = @scenarioVersionId
              and target.user_id = @primaryUserId;
            """;
        const string insertPrimarySql = """
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
            values (
                @scenarioVersionId,
                @primaryUserId,
                @measureId,
                @storeId,
                @productNodeId,
                @timePeriodId,
                @inputValue,
                @overrideValue,
                @isSystemGeneratedOverride,
                @derivedValue,
                @effectiveValue,
                @growthFactor,
                @isLocked,
                @lockReason,
                @lockedBy,
                @rowVersion,
                @cellKind,
                now());
            """;

        foreach (var cellChunk in orderedCells.Chunk(BulkWriteChunkSize))
        {
            var chunkStopwatch = Stopwatch.StartNew();
            var measureIds = cellChunk.Select(cell => cell.Coordinate.MeasureId).ToArray();
            var storeIds = cellChunk.Select(cell => cell.Coordinate.StoreId).ToArray();
            var productNodeIds = cellChunk.Select(cell => cell.Coordinate.ProductNodeId).ToArray();
            var timePeriodIds = cellChunk.Select(cell => cell.Coordinate.TimePeriodId).ToArray();
            var existingKeys = new HashSet<string>(StringComparer.Ordinal);

            await using (var selectExistingPrimaryCommand = new NpgsqlCommand(selectExistingPrimarySql, connection, transaction))
            {
                selectExistingPrimaryCommand.CommandTimeout = 300;
                selectExistingPrimaryCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                selectExistingPrimaryCommand.Parameters.AddWithValue("@primaryUserId", userContext.PrimaryUserId);
                selectExistingPrimaryCommand.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, measureIds));
                selectExistingPrimaryCommand.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, storeIds));
                selectExistingPrimaryCommand.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, productNodeIds));
                selectExistingPrimaryCommand.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, timePeriodIds));

                await using var existingReader = await selectExistingPrimaryCommand.ExecuteReaderAsync(cancellationToken);
                while (await existingReader.ReadAsync(cancellationToken))
                {
                    existingKeys.Add(new PlanningCellCoordinate(
                        scenarioVersionId,
                        existingReader.GetInt64(0),
                        existingReader.GetInt64(1),
                        existingReader.GetInt64(2),
                        existingReader.GetInt64(3)).Key);
                }
            }

            var existingCells = cellChunk
                .Where(cell => existingKeys.Contains(cell.Coordinate.Key))
                .ToArray();
            var missingCells = cellChunk
                .Where(cell => !existingKeys.Contains(cell.Coordinate.Key))
                .ToArray();
            var stageTableName = $"planning_draft_cells_stage_{Guid.NewGuid():N}";

            var stageCreateStopwatch = Stopwatch.StartNew();
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
            stageCreateStopwatch.Stop();

            var stageImportStopwatch = Stopwatch.StartNew();
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
                foreach (var cell in cellChunk)
                {
                    await importer.StartRowAsync(cancellationToken);
                    await importer.WriteAsync(scenarioVersionId, NpgsqlDbType.Bigint, cancellationToken);
                    await importer.WriteAsync(userContext.PrimaryUserId, NpgsqlDbType.Text, cancellationToken);
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
            stageImportStopwatch.Stop();

            var aliasDeleteStopwatch = Stopwatch.StartNew();
            await using (var deleteAliasCommand = new NpgsqlCommand(deleteAliasSql, connection, transaction))
            {
                deleteAliasCommand.CommandTimeout = 300;
                deleteAliasCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                deleteAliasCommand.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
                deleteAliasCommand.Parameters.AddWithValue("@primaryUserId", userContext.PrimaryUserId);
                deleteAliasCommand.Parameters.Add(CreateArrayParameter("@measureIds", NpgsqlDbType.Bigint, measureIds));
                deleteAliasCommand.Parameters.Add(CreateArrayParameter("@storeIds", NpgsqlDbType.Bigint, storeIds));
                deleteAliasCommand.Parameters.Add(CreateArrayParameter("@productNodeIds", NpgsqlDbType.Bigint, productNodeIds));
                deleteAliasCommand.Parameters.Add(CreateArrayParameter("@timePeriodIds", NpgsqlDbType.Bigint, timePeriodIds));
                await deleteAliasCommand.ExecuteNonQueryAsync(cancellationToken);
            }
            aliasDeleteStopwatch.Stop();

            var primaryUpdateStopwatch = Stopwatch.StartNew();
            var updatedPrimaryRowCount = 0;
            if (existingCells.Length > 0)
            {
                await using var updatePrimaryCommand = new NpgsqlCommand(
                    updatePrimarySql.Replace("%STAGE_TABLE%", stageTableName, StringComparison.Ordinal),
                    connection,
                    transaction);
                updatePrimaryCommand.CommandTimeout = 300;
                updatedPrimaryRowCount = await updatePrimaryCommand.ExecuteNonQueryAsync(cancellationToken);
            }
            primaryUpdateStopwatch.Stop();

            var insertedPrimaryRowCount = 0;
            var insertStopwatch = Stopwatch.StartNew();
            if (missingCells.Length > 0)
            {
                if (existingCells.Length == 0)
                {
                    await using var insertPrimaryCommand = new NpgsqlCommand(
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
                        from {stageTableName} as source;
                        """,
                        connection,
                        transaction);
                    insertPrimaryCommand.CommandTimeout = 300;
                    insertedPrimaryRowCount = await insertPrimaryCommand.ExecuteNonQueryAsync(cancellationToken);
                }
                else
                {
                    await using var insertPrimaryCommand = new NpgsqlCommand(insertPrimarySql, connection, transaction)
                    {
                        CommandTimeout = 300
                    };
                    insertPrimaryCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
                    insertPrimaryCommand.Parameters.AddWithValue("@primaryUserId", userContext.PrimaryUserId);
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@measureId", NpgsqlDbType.Bigint));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@storeId", NpgsqlDbType.Bigint));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@productNodeId", NpgsqlDbType.Bigint));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@timePeriodId", NpgsqlDbType.Bigint));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@inputValue", NpgsqlDbType.Numeric));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@overrideValue", NpgsqlDbType.Numeric));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@isSystemGeneratedOverride", NpgsqlDbType.Integer));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@derivedValue", NpgsqlDbType.Numeric));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@effectiveValue", NpgsqlDbType.Numeric));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@growthFactor", NpgsqlDbType.Numeric));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@isLocked", NpgsqlDbType.Integer));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@lockReason", NpgsqlDbType.Text));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@lockedBy", NpgsqlDbType.Text));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@rowVersion", NpgsqlDbType.Bigint));
                    insertPrimaryCommand.Parameters.Add(new NpgsqlParameter("@cellKind", NpgsqlDbType.Text));

                    foreach (var cell in missingCells)
                    {
                        insertPrimaryCommand.Parameters["@measureId"].Value = cell.Coordinate.MeasureId;
                        insertPrimaryCommand.Parameters["@storeId"].Value = cell.Coordinate.StoreId;
                        insertPrimaryCommand.Parameters["@productNodeId"].Value = cell.Coordinate.ProductNodeId;
                        insertPrimaryCommand.Parameters["@timePeriodId"].Value = cell.Coordinate.TimePeriodId;
                        insertPrimaryCommand.Parameters["@inputValue"].Value = cell.InputValue is { } inputValue ? inputValue : DBNull.Value;
                        insertPrimaryCommand.Parameters["@overrideValue"].Value = cell.OverrideValue is { } overrideValue ? overrideValue : DBNull.Value;
                        insertPrimaryCommand.Parameters["@isSystemGeneratedOverride"].Value = cell.IsSystemGeneratedOverride ? 1 : 0;
                        insertPrimaryCommand.Parameters["@derivedValue"].Value = cell.DerivedValue;
                        insertPrimaryCommand.Parameters["@effectiveValue"].Value = cell.EffectiveValue;
                        insertPrimaryCommand.Parameters["@growthFactor"].Value = cell.GrowthFactor;
                        insertPrimaryCommand.Parameters["@isLocked"].Value = cell.IsLocked ? 1 : 0;
                        insertPrimaryCommand.Parameters["@lockReason"].Value = cell.LockReason is { } lockReason ? lockReason : DBNull.Value;
                        insertPrimaryCommand.Parameters["@lockedBy"].Value = cell.LockedBy is { } lockedBy ? lockedBy : DBNull.Value;
                        insertPrimaryCommand.Parameters["@rowVersion"].Value = cell.RowVersion;
                        insertPrimaryCommand.Parameters["@cellKind"].Value = cell.CellKind;
                        insertedPrimaryRowCount += await insertPrimaryCommand.ExecuteNonQueryAsync(cancellationToken);
                    }
                }
            }
            insertStopwatch.Stop();
            chunkStopwatch.Stop();

            if (chunkStopwatch.ElapsedMilliseconds >= 500 || (existingCells.Length > 0 && missingCells.Length > 0))
            {
                _logger.LogInformation(
                    "Draft replace chunk for scenario {ScenarioVersionId} user {UserId} touched {DraftCellCount} cells ({ExistingCellCount} existing, {MissingCellCount} missing) in {ElapsedMs} ms (stage {StageMs} ms, alias delete {AliasDeleteMs} ms, primary update {PrimaryUpdateMs} ms/{UpdatedPrimaryRowCount} rows, insert {InsertMs} ms/{InsertedPrimaryRowCount} rows).",
                    scenarioVersionId,
                    userContext.PrimaryUserId,
                    cellChunk.Length,
                    existingCells.Length,
                    missingCells.Length,
                    chunkStopwatch.ElapsedMilliseconds,
                    stageCreateStopwatch.ElapsedMilliseconds + stageImportStopwatch.ElapsedMilliseconds,
                    aliasDeleteStopwatch.ElapsedMilliseconds,
                    primaryUpdateStopwatch.ElapsedMilliseconds,
                    updatedPrimaryRowCount,
                    insertStopwatch.ElapsedMilliseconds,
                    insertedPrimaryRowCount);
            }
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
                deltas_json,
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
                @deltasJson,
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
            headerCommand.Parameters.AddWithValue("@deltasJson", batch.Deltas.Count == 0 ? DBNull.Value : SerializeDraftBatchDeltas(batch.Deltas));
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
        await UpsertDraftPlanningCellsAsync(
            connection,
            transaction,
            scenarioVersionId,
            PlanningUserIdentity.CreatePlanningUserContext(userId, userId),
            cells,
            cancellationToken);

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
        string? deltasJson;
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
                   deltas_json,
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
            deltasJson = headerReader.IsDBNull(5) ? null : headerReader.GetString(5);
            isUndone = headerReader.GetInt32(6) == 1;
            supersededByBatchId = headerReader.IsDBNull(7) ? null : headerReader.GetInt64(7);
            createdAt = headerReader.GetFieldValue<DateTimeOffset>(8);
            undoneAt = headerReader.IsDBNull(9) ? null : headerReader.GetFieldValue<DateTimeOffset>(9);
        }

        List<PlanningCommandCellDelta> deltas;
        if (!string.IsNullOrWhiteSpace(deltasJson))
        {
            deltas = DeserializeDraftBatchDeltas(deltasJson);
        }
        else
        {
            deltas = [];
            await using var deltaCommand = new NpgsqlCommand(
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
                transaction);
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

    internal static string SerializeDraftBatchDeltas(IReadOnlyList<PlanningCommandCellDelta> deltas)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("v", 2);
            writer.WritePropertyName("d");
            writer.WriteStartArray();

            foreach (var delta in deltas)
            {
                writer.WriteStartArray();
                writer.WriteNumberValue(delta.Coordinate.ScenarioVersionId);
                writer.WriteNumberValue(delta.Coordinate.MeasureId);
                writer.WriteNumberValue(delta.Coordinate.StoreId);
                writer.WriteNumberValue(delta.Coordinate.ProductNodeId);
                writer.WriteNumberValue(delta.Coordinate.TimePeriodId);
                WriteCompactCellState(writer, delta.OldState);
                WriteCompactCellState(writer, delta.NewState);
                writer.WriteStringValue(delta.ChangeKind);
                writer.WriteEndArray();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    internal static List<PlanningCommandCellDelta> DeserializeDraftBatchDeltas(string deltasJson)
    {
        using var document = JsonDocument.Parse(deltasJson);
        var root = document.RootElement;

        if (root.ValueKind == JsonValueKind.Object
            && root.TryGetProperty("v", out var versionElement)
            && versionElement.ValueKind == JsonValueKind.Number
            && versionElement.GetInt32() == 2
            && root.TryGetProperty("d", out var deltasElement)
            && deltasElement.ValueKind == JsonValueKind.Array)
        {
            var deltas = new List<PlanningCommandCellDelta>(deltasElement.GetArrayLength());
            foreach (var element in deltasElement.EnumerateArray())
            {
                if (element.ValueKind != JsonValueKind.Array)
                {
                    continue;
                }

                var items = element.EnumerateArray().ToArray();
                if (items.Length != 8)
                {
                    continue;
                }

                var coordinate = new PlanningCellCoordinate(
                    items[0].GetInt64(),
                    items[1].GetInt64(),
                    items[2].GetInt64(),
                    items[3].GetInt64(),
                    items[4].GetInt64());
                deltas.Add(new PlanningCommandCellDelta(
                    coordinate,
                    ReadCompactCellState(items[5]),
                    ReadCompactCellState(items[6]),
                    items[7].GetString() ?? string.Empty));
            }

            return deltas;
        }

        return JsonSerializer.Deserialize<List<PlanningCommandCellDelta>>(deltasJson) ?? [];
    }

    private static void WriteCompactCellState(Utf8JsonWriter writer, PlanningCellState state)
    {
        writer.WriteStartArray();
        WriteNullableDecimal(writer, state.InputValue);
        WriteNullableDecimal(writer, state.OverrideValue);
        writer.WriteBooleanValue(state.IsSystemGeneratedOverride);
        writer.WriteNumberValue(state.DerivedValue);
        writer.WriteNumberValue(state.EffectiveValue);
        writer.WriteNumberValue(state.GrowthFactor);
        writer.WriteBooleanValue(state.IsLocked);
        WriteNullableString(writer, state.LockReason);
        WriteNullableString(writer, state.LockedBy);
        writer.WriteNumberValue(state.RowVersion);
        writer.WriteStringValue(state.CellKind);
        writer.WriteEndArray();
    }

    private static PlanningCellState ReadCompactCellState(JsonElement element)
    {
        var items = element.EnumerateArray().ToArray();
        return new PlanningCellState(
            ReadNullableDecimal(items[0]),
            ReadNullableDecimal(items[1]),
            items[2].GetBoolean(),
            items[3].GetDecimal(),
            items[4].GetDecimal(),
            items[5].GetDecimal(),
            items[6].GetBoolean(),
            items[7].ValueKind == JsonValueKind.Null ? null : items[7].GetString(),
            items[8].ValueKind == JsonValueKind.Null ? null : items[8].GetString(),
            items[9].GetInt64(),
            items[10].GetString() ?? "input");
    }

    private static void WriteNullableDecimal(Utf8JsonWriter writer, decimal? value)
    {
        if (value.HasValue)
        {
            writer.WriteNumberValue(value.Value);
            return;
        }

        writer.WriteNullValue();
    }

    private static decimal? ReadNullableDecimal(JsonElement element)
    {
        return element.ValueKind == JsonValueKind.Null
            ? null
            : element.GetDecimal();
    }

    private static void WriteNullableString(Utf8JsonWriter writer, string? value)
    {
        if (value is not null)
        {
            writer.WriteStringValue(value);
            return;
        }

        writer.WriteNullValue();
    }

    private static async Task CommitDraftDirectAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long scenarioVersionId,
        PlanningUserIdentity.PlanningUserContext userContext,
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
            select distinct on (
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id)
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
              and user_id = any(@candidateUserIds)
            order by
                scenario_version_id,
                measure_id,
                store_id,
                product_node_id,
                time_period_id,
                case when user_id = @primaryUserId then 0 else 1 end,
                updated_at desc,
                row_version desc
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
            mergeCommand.Parameters.AddWithValue("@primaryUserId", userContext.PrimaryUserId);
            mergeCommand.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
            await mergeCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var deleteCommand = new NpgsqlCommand(
            """
            delete from planning_draft_cells
            where scenario_version_id = @scenarioVersionId
              and user_id = any(@candidateUserIds);
            """,
            connection,
            transaction);
        deleteCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteCommand.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
        await deleteCommand.ExecuteNonQueryAsync(cancellationToken);

        await using var deleteDeltaCommand = new NpgsqlCommand(
            """
            delete from planning_draft_command_cell_deltas
            where command_batch_id in (
                select command_batch_id
                from planning_draft_command_batches
                where scenario_version_id = @scenarioVersionId
                  and user_id = any(@candidateUserIds)
            );
            """,
            connection,
            transaction);
        deleteDeltaCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteDeltaCommand.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
        await deleteDeltaCommand.ExecuteNonQueryAsync(cancellationToken);

        await using var deleteBatchCommand = new NpgsqlCommand(
            """
            delete from planning_draft_command_batches
            where scenario_version_id = @scenarioVersionId
              and user_id = any(@candidateUserIds);
            """,
            connection,
            transaction);
        deleteBatchCommand.Parameters.AddWithValue("@scenarioVersionId", scenarioVersionId);
        deleteBatchCommand.Parameters.Add(CreateArrayParameter("@candidateUserIds", NpgsqlDbType.Text, userContext.CandidateUserIds.ToArray()));
        await deleteBatchCommand.ExecuteNonQueryAsync(cancellationToken);
    }
}
