using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class SqlServerConnectivityTests(MsSqlContainerFixture fixture)
{
    private ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
        ["user"] = fixture.User, ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    [SkippableFact]
    public async Task CheckConnection_reports_ok()
    {
        DockerFacts.SkipUnlessDocker();
        var check = await new SqlServerConnector().CheckConnectionAsync(Config, CancellationToken.None);
        Assert.True(check.Ok, check.Message);
    }

    [SkippableFact]
    public async Task CheckConnection_bad_password_reports_permanent()
    {
        DockerFacts.SkipUnlessDocker();
        var bad = new ConnectorConfig(new Dictionary<string, object?>(Config.Values) { ["password"] = "wrong-Pass1!" });
        var check = await new SqlServerConnector().CheckConnectionAsync(bad, CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }
}
