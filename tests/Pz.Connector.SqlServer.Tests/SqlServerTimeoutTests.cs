using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Shared;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Docker-backed proof that <c>command_timeout_seconds</c> actually bounds a real query
/// against a live SQL Server -- not just that the connection string carries the value (see
/// <see cref="SqlServerConnectorTests"/> for the offline proof of that).</summary>
[Collection("sqlserver")]
public sealed class SqlServerTimeoutTests(MsSqlContainerFixture fixture)
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
            ["trust_server_certificate"] = true,
            ["command_timeout_seconds"] = 1,
        });
        var cs = SqlServerConnector.BuildConnectionString(config);

        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync();
        await using var command = new SqlCommand("waitfor delay '00:00:03'", connection);

        var ex = await Assert.ThrowsAsync<SqlException>(() => command.ExecuteNonQueryAsync());

        Assert.Equal(-2, ex.Number);
        Assert.False(ex.IsTransient); // the driver's own signal misses it -- MsTransient covers it
        Assert.True(MsTransient.IsTransient(ex));
    }
}
