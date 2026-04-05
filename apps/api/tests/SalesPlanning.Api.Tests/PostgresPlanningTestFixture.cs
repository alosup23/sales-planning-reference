using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using SalesPlanning.Api.Application;
using SalesPlanning.Api.Infrastructure.Postgres;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PostgresPlanningTestFixture : IAsyncLifetime
{
    private string? _adminConnectionString;
    private string? _schemaName;

    public IPlanningRepository Repository { get; private set; } = null!;
    public PlanningService Service { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var configuration = new ConfigurationBuilder()
            .AddEnvironmentVariables()
            .Build();

        string baseConnectionString;
        try
        {
            baseConnectionString = await PostgresConnectionStringResolver.ResolveAsync(configuration, CancellationToken.None);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                "PostgreSQL-backed planning tests require PlanningPostgresConnectionString or PlanningPostgresSecretArn to be configured.",
                exception);
        }

        var adminBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Pooling = false,
            IncludeErrorDetail = true
        };

        _adminConnectionString = adminBuilder.ConnectionString;
        _schemaName = $"planning_test_{Guid.NewGuid():N}";

        await using (var adminConnection = new NpgsqlConnection(_adminConnectionString))
        {
            await adminConnection.OpenAsync();
            await using var command = new NpgsqlCommand($"""create schema "{_schemaName}" """, adminConnection);
            await command.ExecuteNonQueryAsync();
        }

        var repositoryBuilder = new NpgsqlConnectionStringBuilder(_adminConnectionString)
        {
            SearchPath = _schemaName
        };

        var migrationsDirectory = Path.GetFullPath(
            Path.Combine(
                AppContext.BaseDirectory,
                "../../../../src/SalesPlanning.Api/Infrastructure/Postgres/Migrations"));

        var repository = new PostgresPlanningRepository(
            repositoryBuilder.ConnectionString,
            NullLogger<PostgresPlanningRepository>.Instance,
            true,
            migrationsDirectory);

        Repository = repository;
        Service = new PlanningService(repository, new SplashAllocator());
        await Repository.ResetAsync(CancellationToken.None);
    }

    public async Task ResetAsync()
    {
        await Repository.ResetAsync(CancellationToken.None);
    }

    public async Task DisposeAsync()
    {
        if (string.IsNullOrWhiteSpace(_adminConnectionString) || string.IsNullOrWhiteSpace(_schemaName))
        {
            return;
        }

        await using var adminConnection = new NpgsqlConnection(_adminConnectionString);
        await adminConnection.OpenAsync();
        await using var command = new NpgsqlCommand($"""drop schema if exists "{_schemaName}" cascade""", adminConnection);
        await command.ExecuteNonQueryAsync();
    }
}
