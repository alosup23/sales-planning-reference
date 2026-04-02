using Microsoft.Extensions.Logging;
using SalesPlanning.Api.Infrastructure.Postgres;

if (args.Length < 2)
{
    Console.WriteLine("""
        Usage:
          SalesPlanning.PostgresAdmin migrate <connectionString> [migrationsDirectory]
          SalesPlanning.PostgresAdmin import-sqlite <connectionString> <sqliteDbPath> <seedKey> [sourceName]
          SalesPlanning.PostgresAdmin draft-stats <connectionString> [scenarioVersionId]
        """);
    return 1;
}

var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss ";
}).SetMinimumLevel(LogLevel.Information));

var command = args[0];
var connectionString = args[1];
var migrationsDirectory = Path.Combine(AppContext.BaseDirectory, "Infrastructure", "Postgres", "Migrations");
var repository = new PostgresBackedSqlitePlanningRepository(
    connectionString,
    loggerFactory.CreateLogger<PostgresBackedSqlitePlanningRepository>(),
    Path.Combine(Path.GetTempPath(), "sales-planning-postgres-admin", "planning.db"),
    applyMigrationsOnStartup: false,
    migrationsDirectory);

switch (command)
{
    case "migrate":
    {
        var directory = args.Length >= 3 ? args[2] : migrationsDirectory;
        var migrator = new PostgresMigrationRunner(connectionString, directory, loggerFactory.CreateLogger<PostgresMigrationRunner>());
        await migrator.ApplyMigrationsAsync(CancellationToken.None);
        Console.WriteLine("PostgreSQL migrations applied successfully.");
        return 0;
    }
    case "import-sqlite":
    {
        if (args.Length < 4)
        {
            Console.Error.WriteLine("Usage: SalesPlanning.PostgresAdmin import-sqlite <connectionString> <sqliteDbPath> <seedKey> [sourceName]");
            return 2;
        }

        var sqliteDbPath = args[2];
        var seedKey = args[3];
        var sourceName = args.Length >= 5 ? args[4] : Path.GetFileName(sqliteDbPath);
        var migrator = new PostgresMigrationRunner(connectionString, migrationsDirectory, loggerFactory.CreateLogger<PostgresMigrationRunner>());
        await migrator.ApplyMigrationsAsync(CancellationToken.None);
        await repository.ImportSqliteSnapshotAsync(sqliteDbPath, seedKey, sourceName, CancellationToken.None);
        Console.WriteLine($"Imported SQLite snapshot '{sourceName}' into PostgreSQL using seed key '{seedKey}'.");
        return 0;
    }
    case "draft-stats":
    {
        long? scenarioVersionId = null;
        if (args.Length >= 3 && long.TryParse(args[2], out var parsedScenarioVersionId))
        {
            scenarioVersionId = parsedScenarioVersionId;
        }

        await using var connection = new Npgsql.NpgsqlConnection(connectionString);
        await connection.OpenAsync(CancellationToken.None);

        static async Task<long> ExecuteLongScalarAsync(Npgsql.NpgsqlConnection connection, string sql, long? scenarioVersionId)
        {
            await using var command = new Npgsql.NpgsqlCommand(sql, connection);
            if (scenarioVersionId is { } value)
            {
                command.Parameters.AddWithValue("@scenarioVersionId", value);
            }

            var scalar = await command.ExecuteScalarAsync(CancellationToken.None);
            return scalar is null or DBNull ? 0L : Convert.ToInt64(scalar);
        }

        var totalDraftCells = await ExecuteLongScalarAsync(
            connection,
            "select count(*) from planning_draft_cells;",
            scenarioVersionId);
        var scopedDraftCells = scenarioVersionId is null
            ? totalDraftCells
            : await ExecuteLongScalarAsync(
                connection,
                "select count(*) from planning_draft_cells where scenario_version_id = @scenarioVersionId;",
                scenarioVersionId);

        await using var sizeCommand = new Npgsql.NpgsqlCommand(
            """
            select
                pg_total_relation_size('planning_draft_cells') as table_bytes,
                pg_relation_size('planning_draft_cells_pkey') as pkey_bytes,
                pg_relation_size('idx_planning_draft_cells_user_lookup') as lookup_index_bytes;
            """,
            connection);
        await using var sizeReader = await sizeCommand.ExecuteReaderAsync(CancellationToken.None);
        await sizeReader.ReadAsync(CancellationToken.None);
        var tableBytes = sizeReader.GetInt64(0);
        var pkeyBytes = sizeReader.GetInt64(1);
        var lookupIndexBytes = sizeReader.GetInt64(2);
        await sizeReader.CloseAsync();

        Console.WriteLine($"draft_cells_total={totalDraftCells}");
        Console.WriteLine($"draft_cells_scope={(scenarioVersionId is null ? "all" : scenarioVersionId)}={scopedDraftCells}");
        Console.WriteLine($"draft_cells_table_bytes={tableBytes}");
        Console.WriteLine($"draft_cells_pkey_bytes={pkeyBytes}");
        Console.WriteLine($"draft_cells_lookup_index_bytes={lookupIndexBytes}");

        await using var topUsersCommand = new Npgsql.NpgsqlCommand(
            scenarioVersionId is null
                ? """
                  select user_id, count(*) as row_count, max(updated_at) as last_updated
                  from planning_draft_cells
                  group by user_id
                  order by row_count desc, user_id
                  limit 10;
                  """
                : """
                  select user_id, count(*) as row_count, max(updated_at) as last_updated
                  from planning_draft_cells
                  where scenario_version_id = @scenarioVersionId
                  group by user_id
                  order by row_count desc, user_id
                  limit 10;
                  """,
            connection);
        if (scenarioVersionId is { } scopedScenarioVersionId)
        {
            topUsersCommand.Parameters.AddWithValue("@scenarioVersionId", scopedScenarioVersionId);
        }

        Console.WriteLine("top_users:");
        await using var topUsersReader = await topUsersCommand.ExecuteReaderAsync(CancellationToken.None);
        while (await topUsersReader.ReadAsync(CancellationToken.None))
        {
            Console.WriteLine($"{topUsersReader.GetString(0)}|{topUsersReader.GetInt64(1)}|{topUsersReader.GetFieldValue<DateTimeOffset>(2):O}");
        }

        return 0;
    }
    default:
        Console.Error.WriteLine($"Unsupported command '{command}'.");
        return 3;
}
