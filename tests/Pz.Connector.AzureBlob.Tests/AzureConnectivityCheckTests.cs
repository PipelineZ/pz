using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>`pz validate --connect`'s tier-5 probe (<see cref="AzureConnector.CheckConnectionAsync"/>)
/// against a live Azurite instance (docker+network gated -- see <see cref="AzuriteFixture"/>). The probe
/// must really touch the network: a sink-only Azure connection with bad credentials or an unreachable
/// account has to report a failure, not "ok".</summary>
[Collection("azurite")]
public sealed class AzureConnectivityCheckTests(AzuriteFixture fixture)
{
    private static ConnectorConfig Config(string connectionString) => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = connectionString,
    });

    [SkippableFact]
    public async Task Valid_credentials_report_ok()
    {
        var connector = new AzureConnector();
        var check = await connector.CheckConnectionAsync(Config(fixture.ConnectionString), CancellationToken.None);
        Assert.True(check.Ok);
    }

    [SkippableFact]
    public async Task Wrong_account_key_reports_a_permanent_failure()
    {
        // Same Azurite endpoint, a different (but well-formed) account key -- Azurite validates the
        // request signature exactly like real Azure Storage, so this exercises a real 403 over the wire
        // rather than a config-shape guard.
        var wrongKey = Convert.ToBase64String(new byte[64]);
        var badConnectionString = Regex.Replace(fixture.ConnectionString, "AccountKey=[^;]+", $"AccountKey={wrongKey}");

        var connector = new AzureConnector();
        var check = await connector.CheckConnectionAsync(Config(badConnectionString), CancellationToken.None);

        Assert.False(check.Ok);
        Assert.NotNull(check.Message);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }
}
