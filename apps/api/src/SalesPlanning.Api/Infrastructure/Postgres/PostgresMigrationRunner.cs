using Npgsql;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public sealed class PostgresMigrationRunner
{
    private readonly string _connectionString;
    private readonly string _migrationsDirectory;
    private readonly ILogger<PostgresMigrationRunner> _logger;

    public PostgresMigrationRunner(string connectionString, string migrationsDirectory, ILogger<PostgresMigrationRunner> logger)
    {
        _connectionString = connectionString;
        _migrationsDirectory = migrationsDirectory;
        _logger = logger;
    }

    public async Task ApplyMigrationsAsync(CancellationToken cancellationToken)
    {
        if (!Directory.Exists(_migrationsDirectory))
        {
            throw new InvalidOperationException($"PostgreSQL migration directory was not found: {_migrationsDirectory}");
        }

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await EnsureSchemaMigrationsTableAsync(connection, cancellationToken);

        var appliedMigrations = await LoadAppliedMigrationsAsync(connection, cancellationToken);
        var migrationFiles = Directory.GetFiles(_migrationsDirectory, "*.sql", SearchOption.TopDirectoryOnly)
            .OrderBy(path => Path.GetFileName(path), StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var migrationFile in migrationFiles)
        {
            var migrationId = Path.GetFileName(migrationFile);
            if (appliedMigrations.Contains(migrationId))
            {
                continue;
            }

            var sql = await File.ReadAllTextAsync(migrationFile, cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using (var command = new NpgsqlCommand(sql, connection, transaction))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var recordCommand = new NpgsqlCommand(
                             "insert into schema_migrations (migration_id, applied_at) values (@migrationId, now());",
                             connection,
                             transaction))
            {
                recordCommand.Parameters.AddWithValue("@migrationId", migrationId);
                await recordCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            _logger.LogInformation("Applied PostgreSQL migration {MigrationId}", migrationId);
        }
    }

    private static async Task EnsureSchemaMigrationsTableAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            create table if not exists schema_migrations (
                migration_id text primary key,
                applied_at timestamptz not null default now()
            );
            """;
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<HashSet<string>> LoadAppliedMigrationsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var applied = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var command = new NpgsqlCommand("select migration_id from schema_migrations;", connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            applied.Add(reader.GetString(0));
        }

        return applied;
    }
}
