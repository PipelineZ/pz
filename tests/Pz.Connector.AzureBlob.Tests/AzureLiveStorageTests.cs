using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary><see cref="HttpsRoundTrips"/> against a real storage account (see <see cref="LiveAzure"/>; skipped
/// unless <c>PZ_TEST_AZURE_ACCOUNT</c> is set). Run by hand before a release; CI covers TLS with
/// <see cref="AzuriteTlsTests"/>.</summary>
[Trait("Category", LiveAzure.Category)]
public sealed class AzureLiveStorageTests : IAsyncLifetime
{
    private readonly string prefix = LiveAzure.NewPrefix("azureblob");
    private readonly string dir = Directory.CreateTempSubdirectory("pz-azure-live-").FullName;

    private static ConnectorConfig Config() => new(LiveAzure.AuthKeys());

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (LiveAzure.Account is not null)
        {
            var container = AzureAuth.CreateBlobContainerClient(Config(), LiveAzure.Container);
            await foreach (var blob in container.GetBlobsAsync(prefix: prefix + "/"))
            {
                await container.DeleteBlobIfExistsAsync(blob.Name);
            }
        }

        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public async Task Native_copy_then_native_scan_round_trips_over_https()
    {
        LiveAzure.SkipUnlessConfigured();
        await HttpsRoundTrips.NativeCopyThenNativeScanAsync(Config(), LiveAzure.Container, prefix, Path.Combine(dir, "native.duckdb"));
    }

    [SkippableFact]
    public async Task Universal_write_lands_a_blob_the_native_scan_reads_over_https()
    {
        LiveAzure.SkipUnlessConfigured();
        await HttpsRoundTrips.UniversalWriteThenNativeScanAsync(Config(), LiveAzure.Container, prefix, Path.Combine(dir, "universal.duckdb"));
    }
}
