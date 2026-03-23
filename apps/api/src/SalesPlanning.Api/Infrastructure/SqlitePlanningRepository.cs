using System.Text.Json;
using Microsoft.Data.Sqlite;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Contracts;
using SalesPlanning.Api.Domain;
using SQLitePCL;

namespace SalesPlanning.Api.Infrastructure;

public sealed class SqlitePlanningRepository : IPlanningRepository
{
    private static readonly long[] SupportedMeasureIds = [1, 2];

    static SqlitePlanningRepository()
    {
        Batteries.Init();
    }

    private readonly string _connectionString;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _initialized;

    public SqlitePlanningRepository(string databasePath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(databasePath) ?? ".");
        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            Cache = SqliteCacheMode.Shared,
            Mode = SqliteOpenMode.ReadWriteCreate
        }.ToString();
    }

    public async Task<PlanningMetadataSnapshot> GetMetadataAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        var timePeriods = await LoadTimePeriodsAsync(connection, null, cancellationToken);
        return new PlanningMetadataSnapshot(productNodes, timePeriods);
    }

    public async Task<IReadOnlyList<PlanningCell>> GetCellsAsync(IEnumerable<PlanningCellCoordinate> coordinates, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        var coordinateSet = coordinates.Select(coordinate => coordinate.Key).ToHashSet(StringComparer.Ordinal);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var cells = await LoadCellsAsync(connection, null, cancellationToken);
        return cells
            .Where(cell => coordinateSet.Contains(cell.Coordinate.Key))
            .Select(cell => cell.Clone())
            .ToList();
    }

    public async Task<PlanningCell?> GetCellAsync(PlanningCellCoordinate coordinate, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = """
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   input_value,
                   override_value,
                   is_system_generated_override,
                   derived_value,
                   effective_value,
                   is_locked,
                   lock_reason,
                   locked_by,
                   row_version,
                   cell_kind
            from planning_cells
            where scenario_version_id = $scenarioVersionId
              and measure_id = $measureId
              and store_id = $storeId
              and product_node_id = $productNodeId
              and time_period_id = $timePeriodId;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("$measureId", coordinate.MeasureId);
        command.Parameters.AddWithValue("$storeId", coordinate.StoreId);
        command.Parameters.AddWithValue("$productNodeId", coordinate.ProductNodeId);
        command.Parameters.AddWithValue("$timePeriodId", coordinate.TimePeriodId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return ReadPlanningCell(reader);
    }

    public async Task<IReadOnlyList<PlanningCell>> GetScenarioCellsAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var cells = await LoadCellsAsync(connection, null, cancellationToken);
        return cells
            .Where(cell => cell.Coordinate.ScenarioVersionId == scenarioVersionId && cell.Coordinate.MeasureId == measureId)
            .Select(cell => cell.Clone())
            .ToList();
    }

    public async Task UpsertCellsAsync(IEnumerable<PlanningCell> cells, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var cell in cells)
            {
                await UpsertCellAsync(connection, transaction, cell, cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task AppendAuditAsync(PlanningActionAudit audit, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    insert into audits (action_id, action_type, method, user_id, comment, created_at)
                    values ($actionId, $actionType, $method, $userId, $comment, $createdAt);
                    """;
                command.Parameters.AddWithValue("$actionId", audit.ActionId);
                command.Parameters.AddWithValue("$actionType", audit.ActionType);
                command.Parameters.AddWithValue("$method", audit.Method);
                command.Parameters.AddWithValue("$userId", audit.UserId);
                command.Parameters.AddWithValue("$comment", (object?)audit.Comment ?? DBNull.Value);
                command.Parameters.AddWithValue("$createdAt", audit.CreatedAt.ToString("O"));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            foreach (var delta in audit.Deltas)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
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
                    values (
                        $actionId,
                        $scenarioVersionId,
                        $measureId,
                        $storeId,
                        $productNodeId,
                        $timePeriodId,
                        $oldValue,
                        $newValue,
                        $wasLocked,
                        $changeKind);
                    """;
                command.Parameters.AddWithValue("$actionId", audit.ActionId);
                command.Parameters.AddWithValue("$scenarioVersionId", delta.Coordinate.ScenarioVersionId);
                command.Parameters.AddWithValue("$measureId", delta.Coordinate.MeasureId);
                command.Parameters.AddWithValue("$storeId", delta.Coordinate.StoreId);
                command.Parameters.AddWithValue("$productNodeId", delta.Coordinate.ProductNodeId);
                command.Parameters.AddWithValue("$timePeriodId", delta.Coordinate.TimePeriodId);
                command.Parameters.AddWithValue("$oldValue", delta.OldValue);
                command.Parameters.AddWithValue("$newValue", delta.NewValue);
                command.Parameters.AddWithValue("$wasLocked", delta.WasLocked ? 1 : 0);
                command.Parameters.AddWithValue("$changeKind", delta.ChangeKind);
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<long> GetNextActionIdAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "select coalesce(max(action_id), 1000) + 1 from audits;";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return Convert.ToInt64(value);
    }

    public async Task<IReadOnlyList<PlanningActionAudit>> GetAuditAsync(long scenarioVersionId, long measureId, long storeId, long productNodeId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var audits = new List<PlanningActionAudit>();

        await using var auditCommand = connection.CreateCommand();
        auditCommand.CommandText = """
            select distinct a.action_id,
                            a.action_type,
                            a.method,
                            a.user_id,
                            a.comment,
                            a.created_at
            from audits a
            inner join audit_deltas d on d.action_id = a.action_id
            where d.scenario_version_id = $scenarioVersionId
              and d.measure_id = $measureId
              and d.store_id = $storeId
              and d.product_node_id = $productNodeId
            order by a.created_at desc;
            """;
        auditCommand.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
        auditCommand.Parameters.AddWithValue("$measureId", measureId);
        auditCommand.Parameters.AddWithValue("$storeId", storeId);
        auditCommand.Parameters.AddWithValue("$productNodeId", productNodeId);

        var headers = new List<(long ActionId, string ActionType, string Method, string UserId, string? Comment, DateTimeOffset CreatedAt)>();
        await using (var reader = await auditCommand.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
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
            await using var deltaCommand = connection.CreateCommand();
            deltaCommand.CommandText = """
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
                where action_id = $actionId
                order by audit_delta_id asc;
                """;
            deltaCommand.Parameters.AddWithValue("$actionId", header.ActionId);

            await using var reader = await deltaCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                deltas.Add(new PlanningCellDeltaAudit(
                    new PlanningCellCoordinate(
                        reader.GetInt64(0),
                        reader.GetInt64(1),
                        reader.GetInt64(2),
                        reader.GetInt64(3),
                        reader.GetInt64(4)),
                    ReadDecimal(reader, 5),
                    ReadDecimal(reader, 6),
                    reader.GetInt64(7) == 1,
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
    }

    public async Task<GridSliceResponse> GetGridSliceAsync(long scenarioVersionId, long measureId, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        var timePeriods = await LoadTimePeriodsAsync(connection, null, cancellationToken);
        var scenarioCells = (await LoadCellsAsync(connection, null, cancellationToken))
            .Where(cell => cell.Coordinate.ScenarioVersionId == scenarioVersionId && cell.Coordinate.MeasureId == measureId)
            .ToList();

        var rows = productNodes.Values
            .OrderBy(node => node.Path.Length)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .Select(node =>
            {
                var cells = scenarioCells
                    .Where(cell => cell.Coordinate.StoreId == node.StoreId && cell.Coordinate.ProductNodeId == node.ProductNodeId)
                    .ToDictionary(
                        cell => cell.Coordinate.TimePeriodId,
                        cell => new GridCellDto(
                            cell.EffectiveValue,
                            IsEffectivelyLocked(cell.Coordinate, scenarioCells, productNodes, timePeriods),
                            cell.CellKind == "calculated",
                            cell.OverrideValue is not null,
                            cell.RowVersion,
                            cell.CellKind));

                return new GridRowDto(
                    node.StoreId,
                    node.ProductNodeId,
                    node.Label,
                    node.Level,
                    node.Path,
                    node.IsLeaf,
                    cells);
            })
            .ToList();

        var periods = timePeriods.Values
            .OrderBy(node => node.SortOrder)
            .Select(node => new GridPeriodDto(node.TimePeriodId, node.Label, node.Grain, node.ParentTimePeriodId, node.SortOrder))
            .ToList();

        return new GridSliceResponse(scenarioVersionId, measureId, periods, rows);
    }

    public async Task<ProductNode> AddRowAsync(AddRowRequest request, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);

            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
            var normalizedLevel = request.Level.Trim().ToLowerInvariant();
            var nextProductNodeId = productNodes.Keys.DefaultIfEmpty(3000L).Max();
            var nextStoreId = productNodes.Values.Select(node => node.StoreId).DefaultIfEmpty(200L).Max();

            ProductNode node;
            switch (normalizedLevel)
            {
                case "store":
                {
                    var storeId = ++nextStoreId;
                    var productNodeId = ++nextProductNodeId;
                    node = new ProductNode(productNodeId, storeId, null, request.Label.Trim(), 0, new[] { request.Label.Trim() }, false);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    if (request.CopyFromStoreId is not null)
                    {
                        nextProductNodeId = await CloneStoreHierarchyAndDataAsync(
                            connection,
                            transaction,
                            request.ScenarioVersionId,
                            request.MeasureId,
                            request.CopyFromStoreId.Value,
                            node,
                            productNodes,
                            timePeriods,
                            cells,
                            nextProductNodeId,
                            cancellationToken);
                    }

                    break;
                }
                case "category":
                {
                    var parent = GetRequiredNode(productNodes, request.ParentProductNodeId, 0, "category");
                    node = new ProductNode(
                        ++nextProductNodeId,
                        parent.StoreId,
                        parent.ProductNodeId,
                        request.Label.Trim(),
                        1,
                        parent.Path.Append(request.Label.Trim()).ToArray(),
                        false);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    await UpsertHierarchyCategoryInternalAsync(connection, transaction, node.Label, cancellationToken);
                    break;
                }
                case "subcategory":
                {
                    var parent = GetRequiredNode(productNodes, request.ParentProductNodeId, 1, "subcategory");
                    if (parent.IsLeaf)
                    {
                        parent = parent with { IsLeaf = false };
                        await UpdateProductNodeAsync(connection, transaction, parent, cancellationToken);
                    }

                    node = new ProductNode(
                        ++nextProductNodeId,
                        parent.StoreId,
                        parent.ProductNodeId,
                        request.Label.Trim(),
                        2,
                        parent.Path.Append(request.Label.Trim()).ToArray(),
                        true);
                    await InsertProductNodeAsync(connection, transaction, node, cancellationToken);
                    await InitializeCellsForNodeAsync(connection, transaction, request.ScenarioVersionId, node, SupportedMeasureIds, timePeriods.Values, cancellationToken);
                    await UpsertHierarchySubcategoryInternalAsync(connection, transaction, parent.Label, node.Label, cancellationToken);
                    break;
                }
                default:
                    throw new InvalidOperationException($"Unsupported row level '{request.Level}'.");
            }

            await transaction.CommitAsync(cancellationToken);
            return node;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetHierarchyMappingsAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        return await LoadHierarchyMappingsAsync(connection, null, cancellationToken);
    }

    public async Task UpsertHierarchyCategoryAsync(string categoryLabel, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertHierarchyCategoryInternalAsync(connection, transaction, categoryLabel, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UpsertHierarchySubcategoryAsync(string categoryLabel, string subcategoryLabel, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await UpsertHierarchySubcategoryInternalAsync(connection, transaction, categoryLabel, subcategoryLabel, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task<ProductNode?> FindProductNodeByPathAsync(string[] path, CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await using var connection = await OpenConnectionAsync(cancellationToken);
        var productNodes = await LoadProductNodesAsync(connection, null, cancellationToken);
        return productNodes.Values.FirstOrDefault(candidate =>
            candidate.Path.Length == path.Length &&
            candidate.Path.Zip(path, (left, right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase)).All(match => match));
    }

    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken);
        await _gate.WaitAsync(cancellationToken);
        try
        {
            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            foreach (var tableName in new[]
                     {
                         "audit_deltas",
                         "audits",
                         "planning_cells",
                         "hierarchy_subcategories",
                         "hierarchy_categories",
                         "product_nodes",
                         "time_periods"
                     })
            {
                await using var deleteCommand = connection.CreateCommand();
                deleteCommand.Transaction = transaction;
                deleteCommand.CommandText = $"delete from {tableName};";
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await SeedAsync(connection, transaction, cancellationToken);
            await transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_initialized)
            {
                return;
            }

            await using var connection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    create table if not exists product_nodes (
                        product_node_id integer primary key,
                        store_id integer not null,
                        parent_product_node_id integer null,
                        label text not null,
                        level integer not null,
                        path_json text not null,
                        is_leaf integer not null
                    );
                    create table if not exists time_periods (
                        time_period_id integer primary key,
                        parent_time_period_id integer null,
                        label text not null,
                        grain text not null,
                        sort_order integer not null
                    );
                    create table if not exists planning_cells (
                        scenario_version_id integer not null,
                        measure_id integer not null,
                        store_id integer not null,
                        product_node_id integer not null,
                        time_period_id integer not null,
                        input_value real null,
                        override_value real null,
                        is_system_generated_override integer not null,
                        derived_value real not null,
                        effective_value real not null,
                        is_locked integer not null,
                        lock_reason text null,
                        locked_by text null,
                        row_version integer not null,
                        cell_kind text not null,
                        primary key (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
                    );
                    create table if not exists audits (
                        action_id integer primary key,
                        action_type text not null,
                        method text not null,
                        user_id text not null,
                        comment text null,
                        created_at text not null
                    );
                    create table if not exists audit_deltas (
                        audit_delta_id integer primary key autoincrement,
                        action_id integer not null,
                        scenario_version_id integer not null,
                        measure_id integer not null,
                        store_id integer not null,
                        product_node_id integer not null,
                        time_period_id integer not null,
                        old_value real not null,
                        new_value real not null,
                        was_locked integer not null,
                        change_kind text not null
                    );
                    create table if not exists hierarchy_categories (
                        category_label text primary key
                    );
                    create table if not exists hierarchy_subcategories (
                        category_label text not null,
                        subcategory_label text not null,
                        primary key (category_label, subcategory_label)
                    );
                    create index if not exists idx_planning_cells_scenario_measure on planning_cells (scenario_version_id, measure_id);
                    create index if not exists idx_audit_deltas_lookup on audit_deltas (scenario_version_id, measure_id, store_id, product_node_id);
                    """;
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var countCommand = connection.CreateCommand())
            {
                countCommand.Transaction = transaction;
                countCommand.CommandText = "select count(*) from product_nodes;";
                var existingCount = Convert.ToInt32(await countCommand.ExecuteScalarAsync(cancellationToken));
                if (existingCount == 0)
                {
                    await SeedAsync(connection, transaction, cancellationToken);
                }
            }

            var productNodes = await LoadProductNodesAsync(connection, transaction, cancellationToken);
            var timePeriods = await LoadTimePeriodsAsync(connection, transaction, cancellationToken);
            await EnsureSupportedMeasureCellsAsync(connection, transaction, productNodes.Values, timePeriods.Values, cancellationToken);
            await EnsureQuantitySeedAsync(connection, transaction, productNodes, timePeriods, cancellationToken);

            await transaction.CommitAsync(cancellationToken);
            _initialized = true;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        return connection;
    }

    private static async Task<Dictionary<long, ProductNode>> LoadProductNodesAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, ProductNode>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select product_node_id, store_id, parent_product_node_id, label, level, path_json, is_leaf
            from product_nodes;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var node = new ProductNode(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.GetString(3),
                reader.GetInt32(4),
                JsonSerializer.Deserialize<string[]>(reader.GetString(5)) ?? Array.Empty<string>(),
                reader.GetInt64(6) == 1);
            result[node.ProductNodeId] = node;
        }

        return result;
    }

    private static async Task<Dictionary<long, TimePeriodNode>> LoadTimePeriodsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new Dictionary<long, TimePeriodNode>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select time_period_id, parent_time_period_id, label, grain, sort_order
            from time_periods;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var node = new TimePeriodNode(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetInt64(1),
                reader.GetString(2),
                reader.GetString(3),
                reader.GetInt32(4));
            result[node.TimePeriodId] = node;
        }

        return result;
    }

    private static async Task<List<PlanningCell>> LoadCellsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var result = new List<PlanningCell>();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            select scenario_version_id,
                   measure_id,
                   store_id,
                   product_node_id,
                   time_period_id,
                   input_value,
                   override_value,
                   is_system_generated_override,
                   derived_value,
                   effective_value,
                   is_locked,
                   lock_reason,
                   locked_by,
                   row_version,
                   cell_kind
            from planning_cells;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(ReadPlanningCell(reader));
        }

        return result;
    }

    private static PlanningCell ReadPlanningCell(SqliteDataReader reader)
    {
        return new PlanningCell
        {
            Coordinate = new PlanningCellCoordinate(
                reader.GetInt64(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetInt64(3),
                reader.GetInt64(4)),
            InputValue = reader.IsDBNull(5) ? null : ReadDecimal(reader, 5),
            OverrideValue = reader.IsDBNull(6) ? null : ReadDecimal(reader, 6),
            IsSystemGeneratedOverride = reader.GetInt64(7) == 1,
            DerivedValue = ReadDecimal(reader, 8),
            EffectiveValue = ReadDecimal(reader, 9),
            IsLocked = reader.GetInt64(10) == 1,
            LockReason = reader.IsDBNull(11) ? null : reader.GetString(11),
            LockedBy = reader.IsDBNull(12) ? null : reader.GetString(12),
            RowVersion = reader.GetInt64(13),
            CellKind = reader.GetString(14)
        };
    }

    private static decimal ReadDecimal(SqliteDataReader reader, int ordinal)
    {
        return Convert.ToDecimal(reader.GetValue(ordinal));
    }

    private static async Task UpsertCellAsync(SqliteConnection connection, SqliteTransaction transaction, PlanningCell cell, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
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
                is_locked,
                lock_reason,
                locked_by,
                row_version,
                cell_kind)
            values (
                $scenarioVersionId,
                $measureId,
                $storeId,
                $productNodeId,
                $timePeriodId,
                $inputValue,
                $overrideValue,
                $isSystemGeneratedOverride,
                $derivedValue,
                $effectiveValue,
                $isLocked,
                $lockReason,
                $lockedBy,
                $rowVersion,
                $cellKind)
            on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id)
            do update set
                input_value = excluded.input_value,
                override_value = excluded.override_value,
                is_system_generated_override = excluded.is_system_generated_override,
                derived_value = excluded.derived_value,
                effective_value = excluded.effective_value,
                is_locked = excluded.is_locked,
                lock_reason = excluded.lock_reason,
                locked_by = excluded.locked_by,
                row_version = excluded.row_version,
                cell_kind = excluded.cell_kind;
            """;
        command.Parameters.AddWithValue("$scenarioVersionId", cell.Coordinate.ScenarioVersionId);
        command.Parameters.AddWithValue("$measureId", cell.Coordinate.MeasureId);
        command.Parameters.AddWithValue("$storeId", cell.Coordinate.StoreId);
        command.Parameters.AddWithValue("$productNodeId", cell.Coordinate.ProductNodeId);
        command.Parameters.AddWithValue("$timePeriodId", cell.Coordinate.TimePeriodId);
        command.Parameters.AddWithValue("$inputValue", (object?)cell.InputValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$overrideValue", (object?)cell.OverrideValue ?? DBNull.Value);
        command.Parameters.AddWithValue("$isSystemGeneratedOverride", cell.IsSystemGeneratedOverride ? 1 : 0);
        command.Parameters.AddWithValue("$derivedValue", cell.DerivedValue);
        command.Parameters.AddWithValue("$effectiveValue", cell.EffectiveValue);
        command.Parameters.AddWithValue("$isLocked", cell.IsLocked ? 1 : 0);
        command.Parameters.AddWithValue("$lockReason", (object?)cell.LockReason ?? DBNull.Value);
        command.Parameters.AddWithValue("$lockedBy", (object?)cell.LockedBy ?? DBNull.Value);
        command.Parameters.AddWithValue("$rowVersion", cell.RowVersion);
        command.Parameters.AddWithValue("$cellKind", cell.CellKind);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InsertProductNodeAsync(SqliteConnection connection, SqliteTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into product_nodes (
                product_node_id,
                store_id,
                parent_product_node_id,
                label,
                level,
                path_json,
                is_leaf)
            values (
                $productNodeId,
                $storeId,
                $parentProductNodeId,
                $label,
                $level,
                $pathJson,
                $isLeaf);
            """;
        command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("$storeId", node.StoreId);
        command.Parameters.AddWithValue("$parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("$label", node.Label);
        command.Parameters.AddWithValue("$level", node.Level);
        command.Parameters.AddWithValue("$pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("$isLeaf", node.IsLeaf ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpdateProductNodeAsync(SqliteConnection connection, SqliteTransaction transaction, ProductNode node, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            update product_nodes
            set store_id = $storeId,
                parent_product_node_id = $parentProductNodeId,
                label = $label,
                level = $level,
                path_json = $pathJson,
                is_leaf = $isLeaf
            where product_node_id = $productNodeId;
            """;
        command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
        command.Parameters.AddWithValue("$storeId", node.StoreId);
        command.Parameters.AddWithValue("$parentProductNodeId", (object?)node.ParentProductNodeId ?? DBNull.Value);
        command.Parameters.AddWithValue("$label", node.Label);
        command.Parameters.AddWithValue("$level", node.Level);
        command.Parameters.AddWithValue("$pathJson", JsonSerializer.Serialize(node.Path));
        command.Parameters.AddWithValue("$isLeaf", node.IsLeaf ? 1 : 0);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task InitializeCellsForNodeAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        ProductNode node,
        IEnumerable<long> measureIds,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        foreach (var measureId in measureIds)
        {
            foreach (var period in timePeriods)
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = """
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
                        is_locked,
                        lock_reason,
                        locked_by,
                        row_version,
                        cell_kind)
                    values (
                        $scenarioVersionId,
                        $measureId,
                        $storeId,
                        $productNodeId,
                        $timePeriodId,
                        null,
                        null,
                        0,
                        0,
                        0,
                        0,
                        null,
                        null,
                        1,
                        $cellKind)
                    on conflict (scenario_version_id, measure_id, store_id, product_node_id, time_period_id) do nothing;
                    """;
                command.Parameters.AddWithValue("$scenarioVersionId", scenarioVersionId);
                command.Parameters.AddWithValue("$measureId", measureId);
                command.Parameters.AddWithValue("$storeId", node.StoreId);
                command.Parameters.AddWithValue("$productNodeId", node.ProductNodeId);
                command.Parameters.AddWithValue("$timePeriodId", period.TimePeriodId);
                command.Parameters.AddWithValue("$cellKind", node.IsLeaf && period.Grain == "month" ? "leaf" : "calculated");
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }

    private static async Task EnsureSupportedMeasureCellsAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IEnumerable<ProductNode> productNodes,
        IEnumerable<TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        foreach (var productNode in productNodes)
        {
            await InitializeCellsForNodeAsync(connection, transaction, 1, productNode, SupportedMeasureIds, timePeriods, cancellationToken);
        }
    }

    private static async Task EnsureQuantitySeedAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "select count(*) from planning_cells where measure_id = 2 and input_value is not null;";
        var existingQuantityInputs = Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
        if (existingQuantityInputs > 0)
        {
            return;
        }

        var cells = await LoadCellsAsync(connection, transaction, cancellationToken);
        var quantityCells = cells
            .Where(cell => cell.Coordinate.ScenarioVersionId == 1 && cell.Coordinate.MeasureId == 2)
            .ToDictionary(cell => cell.Coordinate.Key, cell => cell.Clone());

        var quantityLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2110)] = new()
            {
                [202601] = 120m, [202602] = 150m, [202603] = 142m, [202604] = 144m,
                [202605] = 146m, [202606] = 149m, [202607] = 153m, [202608] = 155m,
                [202609] = 148m, [202610] = 140m, [202611] = 146m, [202612] = 158m
            },
            [(101, 2120)] = new()
            {
                [202601] = 55m, [202602] = 58m, [202603] = 57m, [202604] = 59m,
                [202605] = 60m, [202606] = 62m, [202607] = 64m, [202608] = 63m,
                [202609] = 61m, [202610] = 60m, [202611] = 61m, [202612] = 66m
            },
            [(101, 2210)] = new()
            {
                [202601] = 80m, [202602] = 84m, [202603] = 82m, [202604] = 85m,
                [202605] = 88m, [202606] = 90m, [202607] = 92m, [202608] = 94m,
                [202609] = 91m, [202610] = 88m, [202611] = 89m, [202612] = 96m
            }
        };

        foreach (var (key, monthValues) in quantityLeafValues)
        {
            foreach (var monthValue in monthValues)
            {
                var coordinate = new PlanningCellCoordinate(1, 2, key.StoreId, key.ProductNodeId, monthValue.Key);
                if (!quantityCells.TryGetValue(coordinate.Key, out var cell))
                {
                    continue;
                }

                cell.InputValue = monthValue.Value;
                cell.DerivedValue = monthValue.Value;
                cell.EffectiveValue = monthValue.Value;
                cell.RowVersion = Math.Max(cell.RowVersion, 2);
                cell.CellKind = "input";
            }
        }

        RecalculateSeedTotals(1, 2, quantityCells, productNodes, timePeriods);

        foreach (var cell in quantityCells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static ProductNode GetRequiredNode(
        IReadOnlyDictionary<long, ProductNode> productNodes,
        long? productNodeId,
        int expectedLevel,
        string childLevel)
    {
        if (productNodeId is null || !productNodes.TryGetValue(productNodeId.Value, out var parent))
        {
            throw new InvalidOperationException($"A parent row is required to add a {childLevel}.");
        }

        if (parent.Level != expectedLevel)
        {
            throw new InvalidOperationException($"A {childLevel} can only be added beneath a level {expectedLevel} row.");
        }

        return parent;
    }

    private static async Task<long> CloneStoreHierarchyAndDataAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        long scenarioVersionId,
        long measureId,
        long sourceStoreId,
        ProductNode targetStoreNode,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods,
        IReadOnlyList<PlanningCell> cells,
        long nextProductNodeId,
        CancellationToken cancellationToken)
    {
        var sourceRootNode = productNodes.Values.SingleOrDefault(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is null)
            ?? throw new InvalidOperationException($"Store {sourceStoreId} was not found for copy.");
        var sourceNodes = productNodes.Values
            .Where(node => node.StoreId == sourceStoreId && node.ParentProductNodeId is not null)
            .OrderBy(node => node.Level)
            .ThenBy(node => string.Join(">", node.Path), StringComparer.OrdinalIgnoreCase)
            .ToList();
        var cellLookup = cells.ToDictionary(cell => cell.Coordinate.Key, cell => cell);
        var nodeMap = new Dictionary<long, ProductNode>
        {
            [sourceRootNode.ProductNodeId] = targetStoreNode
        };

        foreach (var sourceNode in sourceNodes)
        {
            var parent = nodeMap[sourceNode.ParentProductNodeId!.Value];
            var clonedNode = new ProductNode(
                ++nextProductNodeId,
                targetStoreNode.StoreId,
                parent.ProductNodeId,
                sourceNode.Label,
                sourceNode.Level,
                parent.Path.Append(sourceNode.Label).ToArray(),
                sourceNode.IsLeaf);
            await InsertProductNodeAsync(connection, transaction, clonedNode, cancellationToken);
            await InitializeCellsForNodeAsync(connection, transaction, scenarioVersionId, clonedNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
            nodeMap[sourceNode.ProductNodeId] = clonedNode;
        }

        foreach (var sourceNode in sourceNodes.Prepend(sourceRootNode))
        {
            var targetNode = nodeMap[sourceNode.ProductNodeId];
            foreach (var supportedMeasureId in SupportedMeasureIds)
            {
                foreach (var period in timePeriods.Values)
                {
                    var sourceCoordinate = new PlanningCellCoordinate(scenarioVersionId, supportedMeasureId, sourceStoreId, sourceNode.ProductNodeId, period.TimePeriodId);
                    var targetCoordinate = new PlanningCellCoordinate(scenarioVersionId, supportedMeasureId, targetStoreNode.StoreId, targetNode.ProductNodeId, period.TimePeriodId);
                    if (!cellLookup.TryGetValue(sourceCoordinate.Key, out var sourceCell))
                    {
                        continue;
                    }

                    var clonedCell = sourceCell.Clone();
                    clonedCell.Coordinate = targetCoordinate;
                    clonedCell.IsLocked = false;
                    clonedCell.LockReason = null;
                    clonedCell.LockedBy = null;
                    await UpsertCellAsync(connection, transaction, clonedCell, cancellationToken);
                }
            }
        }

        return nextProductNodeId;
    }

    private static async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> LoadHierarchyMappingsAsync(SqliteConnection connection, SqliteTransaction? transaction, CancellationToken cancellationToken)
    {
        var categories = new SortedDictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        await using (var categoryCommand = connection.CreateCommand())
        {
            categoryCommand.Transaction = transaction;
            categoryCommand.CommandText = "select category_label from hierarchy_categories order by category_label;";
            await using var categoryReader = await categoryCommand.ExecuteReaderAsync(cancellationToken);
            while (await categoryReader.ReadAsync(cancellationToken))
            {
                categories[categoryReader.GetString(0)] = new List<string>();
            }
        }

        await using (var subcategoryCommand = connection.CreateCommand())
        {
            subcategoryCommand.Transaction = transaction;
            subcategoryCommand.CommandText = """
                select category_label, subcategory_label
                from hierarchy_subcategories
                order by category_label, subcategory_label;
                """;
            await using var subcategoryReader = await subcategoryCommand.ExecuteReaderAsync(cancellationToken);
            while (await subcategoryReader.ReadAsync(cancellationToken))
            {
                var categoryLabel = subcategoryReader.GetString(0);
                var subcategoryLabel = subcategoryReader.GetString(1);
                if (!categories.TryGetValue(categoryLabel, out var subcategories))
                {
                    subcategories = new List<string>();
                    categories[categoryLabel] = subcategories;
                }

                subcategories.Add(subcategoryLabel);
            }
        }

        return categories.ToDictionary(
            entry => entry.Key,
            entry => (IReadOnlyList<string>)entry.Value,
            StringComparer.OrdinalIgnoreCase);
    }

    private static async Task UpsertHierarchyCategoryInternalAsync(SqliteConnection connection, SqliteTransaction transaction, string categoryLabel, CancellationToken cancellationToken)
    {
        var normalizedLabel = categoryLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedLabel))
        {
            throw new InvalidOperationException("Category labels cannot be empty.");
        }

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into hierarchy_categories (category_label)
            values ($categoryLabel)
            on conflict (category_label) do nothing;
            """;
        command.Parameters.AddWithValue("$categoryLabel", normalizedLabel);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task UpsertHierarchySubcategoryInternalAsync(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string categoryLabel,
        string subcategoryLabel,
        CancellationToken cancellationToken)
    {
        var normalizedSubcategory = subcategoryLabel.Trim();
        if (string.IsNullOrWhiteSpace(normalizedSubcategory))
        {
            throw new InvalidOperationException("Subcategory labels cannot be empty.");
        }

        await UpsertHierarchyCategoryInternalAsync(connection, transaction, categoryLabel, cancellationToken);
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            insert into hierarchy_subcategories (category_label, subcategory_label)
            values ($categoryLabel, $subcategoryLabel)
            on conflict (category_label, subcategory_label) do nothing;
            """;
        command.Parameters.AddWithValue("$categoryLabel", categoryLabel.Trim());
        command.Parameters.AddWithValue("$subcategoryLabel", normalizedSubcategory);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static bool IsEffectivelyLocked(
        PlanningCellCoordinate coordinate,
        IReadOnlyCollection<PlanningCell> scenarioCells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        return scenarioCells.Any(cell =>
            cell.IsLocked &&
            cell.Coordinate.StoreId == coordinate.StoreId &&
            IsAncestorOrSelf(productNodes, cell.Coordinate.ProductNodeId, coordinate.ProductNodeId) &&
            IsAncestorOrSelf(timePeriods, cell.Coordinate.TimePeriodId, coordinate.TimePeriodId));
    }

    private static bool IsAncestorOrSelf(IReadOnlyDictionary<long, ProductNode> nodes, long ancestorId, long descendantId)
    {
        var current = descendantId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = nodes[current];
            if (node.ParentProductNodeId is null)
            {
                return false;
            }

            current = node.ParentProductNodeId.Value;
        }
    }

    private static bool IsAncestorOrSelf(IReadOnlyDictionary<long, TimePeriodNode> nodes, long ancestorId, long descendantId)
    {
        var current = descendantId;
        while (true)
        {
            if (current == ancestorId)
            {
                return true;
            }

            var node = nodes[current];
            if (node.ParentTimePeriodId is null)
            {
                return false;
            }

            current = node.ParentTimePeriodId.Value;
        }
    }

    private static async Task SeedAsync(SqliteConnection connection, SqliteTransaction transaction, CancellationToken cancellationToken)
    {
        var timePeriods = BuildTimePeriods();
        foreach (var timePeriod in timePeriods.Values)
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                insert into time_periods (time_period_id, parent_time_period_id, label, grain, sort_order)
                values ($timePeriodId, $parentTimePeriodId, $label, $grain, $sortOrder);
                """;
            command.Parameters.AddWithValue("$timePeriodId", timePeriod.TimePeriodId);
            command.Parameters.AddWithValue("$parentTimePeriodId", (object?)timePeriod.ParentTimePeriodId ?? DBNull.Value);
            command.Parameters.AddWithValue("$label", timePeriod.Label);
            command.Parameters.AddWithValue("$grain", timePeriod.Grain);
            command.Parameters.AddWithValue("$sortOrder", timePeriod.SortOrder);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        var productNodes = BuildProductNodes();
        foreach (var productNode in productNodes.Values)
        {
            await InsertProductNodeAsync(connection, transaction, productNode, cancellationToken);
            await InitializeCellsForNodeAsync(connection, transaction, 1, productNode, SupportedMeasureIds, timePeriods.Values, cancellationToken);
        }

        var cells = new Dictionary<string, PlanningCell>();
        foreach (var measureId in SupportedMeasureIds)
        {
            foreach (var productNode in productNodes.Values)
            {
                foreach (var timePeriod in timePeriods.Values)
                {
                    var cell = new PlanningCell
                    {
                        Coordinate = new PlanningCellCoordinate(1, measureId, productNode.StoreId, productNode.ProductNodeId, timePeriod.TimePeriodId),
                        DerivedValue = 0m,
                        EffectiveValue = 0m,
                        RowVersion = 1,
                        CellKind = productNode.IsLeaf && timePeriod.Grain == "month" ? "leaf" : "calculated"
                    };
                    cells[cell.Coordinate.Key] = cell;
                }
            }
        }

        var revenueLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2110)] = new()
            {
                [202601] = 600m, [202602] = 750m, [202603] = 700m, [202604] = 710m,
                [202605] = 720m, [202606] = 735m, [202607] = 760m, [202608] = 770m,
                [202609] = 730m, [202610] = 690m, [202611] = 720m, [202612] = 780m
            },
            [(101, 2120)] = new()
            {
                [202601] = 250m, [202602] = 260m, [202603] = 255m, [202604] = 265m,
                [202605] = 270m, [202606] = 280m, [202607] = 290m, [202608] = 285m,
                [202609] = 275m, [202610] = 268m, [202611] = 272m, [202612] = 295m
            },
            [(101, 2210)] = new()
            {
                [202601] = 420m, [202602] = 430m, [202603] = 410m, [202604] = 425m,
                [202605] = 440m, [202606] = 450m, [202607] = 460m, [202608] = 470m,
                [202609] = 455m, [202610] = 440m, [202611] = 448m, [202612] = 475m
            }
        };

        var quantityLeafValues = new Dictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>>
        {
            [(101, 2110)] = new()
            {
                [202601] = 120m, [202602] = 150m, [202603] = 142m, [202604] = 144m,
                [202605] = 146m, [202606] = 149m, [202607] = 153m, [202608] = 155m,
                [202609] = 148m, [202610] = 140m, [202611] = 146m, [202612] = 158m
            },
            [(101, 2120)] = new()
            {
                [202601] = 55m, [202602] = 58m, [202603] = 57m, [202604] = 59m,
                [202605] = 60m, [202606] = 62m, [202607] = 64m, [202608] = 63m,
                [202609] = 61m, [202610] = 60m, [202611] = 61m, [202612] = 66m
            },
            [(101, 2210)] = new()
            {
                [202601] = 80m, [202602] = 84m, [202603] = 82m, [202604] = 85m,
                [202605] = 88m, [202606] = 90m, [202607] = 92m, [202608] = 94m,
                [202609] = 91m, [202610] = 88m, [202611] = 89m, [202612] = 96m
            }
        };

        ApplyLeafSeedValues(cells, 1, revenueLeafValues);
        ApplyLeafSeedValues(cells, 2, quantityLeafValues);

        var lockedCoordinate = new PlanningCellCoordinate(1, 1, 101, 2110, 202602);
        cells[lockedCoordinate.Key].IsLocked = true;
        cells[lockedCoordinate.Key].LockReason = "Manager-held sample lock";
        cells[lockedCoordinate.Key].LockedBy = "demo.manager";

        RecalculateSeedTotals(
            1,
            1,
            cells.Where(entry => entry.Value.Coordinate.MeasureId == 1).ToDictionary(entry => entry.Key, entry => entry.Value),
            productNodes,
            timePeriods);
        RecalculateSeedTotals(
            1,
            2,
            cells.Where(entry => entry.Value.Coordinate.MeasureId == 2).ToDictionary(entry => entry.Key, entry => entry.Value),
            productNodes,
            timePeriods);

        foreach (var category in productNodes.Values.Where(node => node.Level == 1))
        {
            await UpsertHierarchyCategoryInternalAsync(connection, transaction, category.Label, cancellationToken);
        }

        foreach (var subcategory in productNodes.Values.Where(node => node.Level == 2))
        {
            var category = productNodes[subcategory.ParentProductNodeId!.Value];
            await UpsertHierarchySubcategoryInternalAsync(connection, transaction, category.Label, subcategory.Label, cancellationToken);
        }

        foreach (var cell in cells.Values)
        {
            await UpsertCellAsync(connection, transaction, cell, cancellationToken);
        }
    }

    private static void ApplyLeafSeedValues(
        IDictionary<string, PlanningCell> cells,
        long measureId,
        IReadOnlyDictionary<(long StoreId, long ProductNodeId), Dictionary<long, decimal>> leafValues)
    {
        foreach (var (key, monthValues) in leafValues)
        {
            foreach (var monthValue in monthValues)
            {
                var coordinate = new PlanningCellCoordinate(1, measureId, key.StoreId, key.ProductNodeId, monthValue.Key);
                var cell = cells[coordinate.Key];
                cell.InputValue = monthValue.Value;
                cell.DerivedValue = monthValue.Value;
                cell.EffectiveValue = monthValue.Value;
                cell.RowVersion = 2;
                cell.CellKind = "input";
            }
        }
    }

    private static void RecalculateSeedTotals(
        long scenarioVersionId,
        long measureId,
        IDictionary<string, PlanningCell> cells,
        IReadOnlyDictionary<long, ProductNode> productNodes,
        IReadOnlyDictionary<long, TimePeriodNode> timePeriods)
    {
        var aggregateTimes = timePeriods.Values
            .Where(period => timePeriods.Values.Any(child => child.ParentTimePeriodId == period.TimePeriodId))
            .OrderBy(period => period.SortOrder)
            .ToList();

        foreach (var leafNode in productNodes.Values.Where(node => node.IsLeaf))
        {
            foreach (var aggregateTime in aggregateTimes)
            {
                var childTimeIds = timePeriods.Values
                    .Where(period => period.ParentTimePeriodId == aggregateTime.TimePeriodId)
                    .Select(period => period.TimePeriodId)
                    .ToList();
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, leafNode.StoreId, leafNode.ProductNodeId, aggregateTime.TimePeriodId);
                var cell = cells[coordinate.Key];
                cell.DerivedValue = childTimeIds.Sum(childTimeId => cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    leafNode.StoreId,
                    leafNode.ProductNodeId,
                    childTimeId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
            }
        }

        foreach (var node in productNodes.Values.Where(node => !node.IsLeaf).OrderByDescending(node => node.Level))
        {
            var childIds = productNodes.Values
                .Where(child => child.ParentProductNodeId == node.ProductNodeId)
                .Select(child => child.ProductNodeId)
                .ToList();

            foreach (var period in timePeriods.Values)
            {
                var coordinate = new PlanningCellCoordinate(scenarioVersionId, measureId, node.StoreId, node.ProductNodeId, period.TimePeriodId);
                var cell = cells[coordinate.Key];
                cell.DerivedValue = childIds.Sum(childId => cells[new PlanningCellCoordinate(
                    scenarioVersionId,
                    measureId,
                    node.StoreId,
                    childId,
                    period.TimePeriodId).Key].EffectiveValue);
                cell.EffectiveValue = cell.DerivedValue;
            }
        }
    }

    private static Dictionary<long, ProductNode> BuildProductNodes()
    {
        return new List<ProductNode>
        {
            new(2000, 101, null, "Store A", 0, new[] { "Store A" }, false),
            new(2100, 101, 2000, "Beverages", 1, new[] { "Store A", "Beverages" }, false),
            new(2110, 101, 2100, "Soft Drinks", 2, new[] { "Store A", "Beverages", "Soft Drinks" }, true),
            new(2120, 101, 2100, "Tea", 2, new[] { "Store A", "Beverages", "Tea" }, true),
            new(2200, 101, 2000, "Snacks", 1, new[] { "Store A", "Snacks" }, false),
            new(2210, 101, 2200, "Chips", 2, new[] { "Store A", "Snacks", "Chips" }, true)
        }.ToDictionary(node => node.ProductNodeId);
    }

    private static Dictionary<long, TimePeriodNode> BuildTimePeriods()
    {
        return new List<TimePeriodNode>
        {
            new(202600, null, "FY26", "year", 1),
            new(202601, 202600, "Jan", "month", 2),
            new(202602, 202600, "Feb", "month", 3),
            new(202603, 202600, "Mar", "month", 4),
            new(202604, 202600, "Apr", "month", 5),
            new(202605, 202600, "May", "month", 6),
            new(202606, 202600, "Jun", "month", 7),
            new(202607, 202600, "Jul", "month", 8),
            new(202608, 202600, "Aug", "month", 9),
            new(202609, 202600, "Sep", "month", 10),
            new(202610, 202600, "Oct", "month", 11),
            new(202611, 202600, "Nov", "month", 12),
            new(202612, 202600, "Dec", "month", 13)
        }.ToDictionary(period => period.TimePeriodId);
    }
}
