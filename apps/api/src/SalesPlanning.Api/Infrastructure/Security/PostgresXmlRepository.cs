using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;
using Npgsql;

namespace SalesPlanning.Api.Infrastructure.Security;

public sealed class PostgresXmlRepository(string connectionString) : IXmlRepository
{
    public IReadOnlyCollection<XElement> GetAllElements()
    {
        var results = new List<XElement>();
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var command = new NpgsqlCommand(
            """
            select xml
            from app_data_protection_keys
            order by created_at;
            """,
            connection);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            results.Add(XElement.Parse(reader.GetString(0)));
        }

        return results;
    }

    public void StoreElement(XElement element, string friendlyName)
    {
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();

        using var command = new NpgsqlCommand(
            """
            insert into app_data_protection_keys (friendly_name, xml, created_at)
            values (@friendlyName, @xml, now())
            on conflict (friendly_name) do update
            set xml = excluded.xml,
                created_at = now();
            """,
            connection);

        command.Parameters.AddWithValue("@friendlyName", friendlyName);
        command.Parameters.AddWithValue("@xml", element.ToString(SaveOptions.DisableFormatting));
        command.ExecuteNonQuery();
    }
}
