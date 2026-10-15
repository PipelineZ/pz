using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary><see cref="HttpsRoundTrips"/> against Azurite over HTTPS (see <see cref="AzuriteTlsFixture"/>; skipped
/// unless <c>PZ_TEST_AZURITE_TLS_DIR</c> is set). Without the curl transport on Linux, both fail with
/// "Problem with the SSL CA cert", as they do against a real storage account.</summary>
[Trait("Category", AzuriteTlsFixture.Category)]
public sealed class AzuriteTlsTests(AzuriteTlsFixture fixture) : IClassFixture<AzuriteTlsFixture>, IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-azurite-tls-").FullName;

    private ConnectorConfig Config() => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private static void SkipUnlessEnabled()
    {
        Skip.If(AzuriteTlsFixture.CertDir is null, "PZ_TEST_AZURITE_TLS_DIR is not set");
        DockerFacts.SkipUnlessDocker();
    }

    [SkippableFact]
    public async Task Native_copy_then_native_scan_round_trips_over_https()
    {
        SkipUnlessEnabled();
        await HttpsRoundTrips.NativeCopyThenNativeScanAsync(
            Config(), AzuriteTlsFixture.Container, $"native-{Guid.NewGuid():N}", Path.Combine(dir, "native.duckdb"));
    }

    [SkippableFact]
    public async Task Universal_write_lands_a_blob_the_native_scan_reads_over_https()
    {
        SkipUnlessEnabled();
        await HttpsRoundTrips.UniversalWriteThenNativeScanAsync(
            Config(), AzuriteTlsFixture.Container, $"universal-{Guid.NewGuid():N}", Path.Combine(dir, "universal.duckdb"));
    }
}
