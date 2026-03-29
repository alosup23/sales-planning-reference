using Microsoft.Extensions.Configuration;
using Npgsql;
using SalesPlanning.Api.Infrastructure.Postgres;
using Xunit;

namespace SalesPlanning.Api.Tests;

public sealed class PostgresConnectionStringResolverTests
{
    [Fact]
    public async Task ResolveAsync_UsesInjectedSecretPayloadWithoutCallingSecretsManager()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["PlanningPostgresSecretPayload"] = """{"host":"db.internal.example","port":5432,"dbname":"salesplanning","username":"planner","password":"super-secret"}"""
            })
            .Build();

        var connectionString = await PostgresConnectionStringResolver.ResolveAsync(configuration, CancellationToken.None);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        Assert.Equal("db.internal.example", builder.Host);
        Assert.Equal(5432, builder.Port);
        Assert.Equal("salesplanning", builder.Database);
        Assert.Equal("planner", builder.Username);
        Assert.Equal("super-secret", builder.Password);
        Assert.Equal(SslMode.Require, builder.SslMode);
    }
}
