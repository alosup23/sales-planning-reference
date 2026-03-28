using Microsoft.Extensions.Logging;
using SalesPlanning.Api.Infrastructure.Postgres;

if (args.Length < 2)
{
    Console.WriteLine("""
        Usage:
          SalesPlanning.PostgresAdmin migrate <connectionString> [migrationsDirectory]
          SalesPlanning.PostgresAdmin import-sqlite <connectionString> <sqliteDbPath> <seedKey> [sourceName]
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
    default:
        Console.Error.WriteLine($"Unsupported command '{command}'.");
        return 3;
}
