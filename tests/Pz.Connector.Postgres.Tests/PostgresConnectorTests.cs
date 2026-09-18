using Json.Schema;
using Npgsql;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres.Tests;

/// <summary><c>connect_timeout_seconds</c>/<c>command_timeout_seconds</c> -- schema shape and
/// that <see cref="PostgresConnector.BuildConnectionString"/> actually applies them. The docker-backed
/// proof that a configured command timeout really fires lives in
/// <see cref="PostgresConnectivityTests"/>'s sibling fixture-based suites (see
/// <c>Command_timeout_seconds_actually_fires_on_a_slow_query</c> below).</summary>
public sealed class PostgresConnectorTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    [Fact]
    public void Connection_schema_accepts_connect_and_command_timeout_seconds()
    {
        var schema = JsonSchema.FromText(new PostgresConnector().ConnectionConfigSchema);

        var valid = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(
            """{"host":"db","database":"analytics","connect_timeout_seconds":5,"command_timeout_seconds":90}""");

        Assert.True(schema.Evaluate(valid).IsValid);
    }

    [Fact]
    public void BuildConnectionString_applies_connect_and_command_timeout_seconds()
    {
        var cs = PostgresConnector.BuildConnectionString(Config(
            ("host", "srv"), ("database", "db"), ("connect_timeout_seconds", 5), ("command_timeout_seconds", 90)));
        var b = new NpgsqlConnectionStringBuilder(cs);

        Assert.Equal(5, b.Timeout);
        Assert.Equal(90, b.CommandTimeout);
    }

    [Fact]
    public void BuildConnectionString_omits_timeouts_when_not_configured_leaving_driver_defaults()
    {
        var cs = PostgresConnector.BuildConnectionString(Config(("host", "srv"), ("database", "db")));
        var b = new NpgsqlConnectionStringBuilder(cs);

        Assert.Equal(15, b.Timeout); // Npgsql's own default, unchanged
        Assert.Equal(30, b.CommandTimeout); // Npgsql's own default, unchanged
    }
}
