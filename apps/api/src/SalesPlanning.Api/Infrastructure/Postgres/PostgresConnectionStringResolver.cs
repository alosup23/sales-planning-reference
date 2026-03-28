using System.Text.Json;
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using Npgsql;

namespace SalesPlanning.Api.Infrastructure.Postgres;

public static class PostgresConnectionStringResolver
{
    public static async Task<string> ResolveAsync(IConfiguration configuration, CancellationToken cancellationToken)
    {
        var directConnectionString = configuration["PlanningPostgresConnectionString"];
        if (!string.IsNullOrWhiteSpace(directConnectionString))
        {
            return directConnectionString.Trim();
        }

        var secretArn = configuration["PlanningPostgresSecretArn"];
        if (string.IsNullOrWhiteSpace(secretArn))
        {
            throw new InvalidOperationException("PlanningPostgresConnectionString or PlanningPostgresSecretArn must be configured when PlanningStorageMode is postgres.");
        }

        using var secretsManager = new AmazonSecretsManagerClient();
        var response = await secretsManager.GetSecretValueAsync(new GetSecretValueRequest
        {
            SecretId = secretArn.Trim()
        }, cancellationToken);

        var secretString = response.SecretString;
        if (string.IsNullOrWhiteSpace(secretString))
        {
            throw new InvalidOperationException($"Secrets Manager secret {secretArn} does not contain a string payload.");
        }

        if (!secretString.TrimStart().StartsWith("{", StringComparison.Ordinal))
        {
            return secretString.Trim();
        }

        using var document = JsonDocument.Parse(secretString);
        var root = document.RootElement;
        if (root.TryGetProperty("connectionString", out var connectionStringElement) && connectionStringElement.ValueKind == JsonValueKind.String)
        {
            return connectionStringElement.GetString() ?? throw new InvalidOperationException($"Secrets Manager secret {secretArn} has an empty connectionString value.");
        }

        var configuredHost = configuration["PlanningPostgresHost"];
        var configuredDatabase = configuration["PlanningPostgresDatabase"];
        var configuredPort = configuration["PlanningPostgresPort"];

        var host = root.TryGetProperty("host", out var hostElement)
            ? hostElement.GetString()
            : configuredHost;
        var database = root.TryGetProperty("dbname", out var dbNameElement)
            ? dbNameElement.GetString()
            : root.TryGetProperty("database", out var databaseElement)
                ? databaseElement.GetString()
                : configuredDatabase;
        var username = root.TryGetProperty("username", out var usernameElement)
            ? usernameElement.GetString()
            : root.TryGetProperty("user", out var userElement)
                ? userElement.GetString()
                : configuration["PlanningPostgresUsername"];
        var password = root.TryGetProperty("password", out var passwordElement)
            ? passwordElement.GetString()
            : configuration["PlanningPostgresPassword"];

        if (string.IsNullOrWhiteSpace(host) || string.IsNullOrWhiteSpace(database) || string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
        {
            throw new InvalidOperationException(
                $"Secrets Manager secret {secretArn} did not contain a full PostgreSQL connection payload. Configure PlanningPostgresHost, PlanningPostgresDatabase, and optional PlanningPostgresPort fallbacks.");
        }

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Port = root.TryGetProperty("port", out var portElement) && portElement.TryGetInt32(out var port)
                ? port
                : int.TryParse(configuredPort, out var configuredPortValue) ? configuredPortValue : 5432,
            Database = database,
            Username = username,
            Password = password,
            SslMode = SslMode.Require,
            Timeout = 30,
            CommandTimeout = 300
        };

        return builder.ConnectionString;
    }
}
