using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.Postgres.Tests;

/// <summary>#114: docker-backed proof that <c>command_timeout_seconds</c> actually bounds a real query
/// against a live Postgres -- not just that the connection string carries the value (see
/// <see cref="PostgresConnectorTests"/> for the offline proof of that).</summary>
[Collection("postgres")]
public sealed class PostgresTimeoutTests(PostgresContainerFixture fixture)
{
    [SkippableFact]
    public async Task Command_timeout_seconds_actually_fires_on_a_slow_query()
    {
        DockerFacts.SkipUnlessDocker();

        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = fixture.Host,
            ["port"] = fixture.Port,
            ["database"] = fixture.Database,
            ["user"] = fixture.User,
            ["password"] = fixture.Password,
            ["command_timeout_seconds"] = 1,
        });
        var cs = PostgresConnector.BuildConnectionString(config);

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("select pg_sleep(3)", connection);

        var ex = await Assert.ThrowsAsync<NpgsqlException>(() => command.ExecuteNonQueryAsync());

        // Confirms #114's classification decision: Npgsql already marks a command timeout transient by
        // its own driver signal (unlike SqlClient's SqlException.IsTransient, which needed #111's
        // MsTransient classifier because it reports false for a client-side timeout).
        Assert.True(ex.IsTransient);
    }
}
