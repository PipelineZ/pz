using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.Iceberg.Tests;

/// <summary><see cref="IcebergAzureFilesTests"/> against a real storage account over HTTPS (see
/// <see cref="LiveAzure"/>; skipped unless <c>PZ_TEST_AZURE_ACCOUNT</c> is set). The table is written by the
/// REST+MinIO fixture and mirrored under a per-run prefix, which the files root then points at.</summary>
[Collection("iceberg-rest")]
[Trait("Category", LiveAzure.Category)]
public sealed class IcebergAzureLiveFilesTests(IcebergRestCatalogFixture catalog) : IAsyncLifetime
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-iceberg-azure-live-").FullName;
    private readonly string ns = "a" + Guid.NewGuid().ToString("N")[..8];
    private readonly string prefix = LiveAzure.NewPrefix("iceberg");

    private static BlobContainerClient Blobs() => LiveAzure.AccountKey is { } key
        ? new BlobContainerClient(LiveAzure.ContainerUri, new StorageSharedKeyCredential(LiveAzure.Account, key))
        : new BlobContainerClient(LiveAzure.ContainerUri, new AzureCliCredential());

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (LiveAzure.Account is not null)
        {
            var blobs = Blobs();
            await foreach (var blob in blobs.GetBlobsAsync(prefix: prefix + "/"))
            {
                await blobs.DeleteBlobIfExistsAsync(blob.Name);
            }
        }

        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private ConnectorConfig RestConfig() => new(new Dictionary<string, object?>
    {
        ["catalog"] = "rest", ["endpoint"] = catalog.Endpoint, ["warehouse"] = "wh", ["token"] = "unchecked-by-the-fixture",
        ["storage_key_id"] = catalog.AccessKey, ["storage_secret_key"] = catalog.SecretKey,
        ["storage_endpoint"] = catalog.StorageEndpoint, ["storage_url_style"] = "path", ["storage_use_ssl"] = false,
    });

    private ConnectorConfig LiveFilesConfig()
    {
        var values = LiveAzure.AuthKeys("storage_");
        values["catalog"] = "files";
        values["root"] = $"az://{LiveAzure.Container}/{prefix}/";
        return new(values);
    }

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    [SkippableFact]
    public async Task A_files_root_on_a_real_storage_account_reads_over_https()
    {
        LiveAzure.SkipUnlessConfigured();
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
        await using var duck = DuckSession.Open(Path.Combine(dir, "scratch.duckdb"));

        await using (var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(RestConfig(), CancellationToken.None))
        {
            var spec = new OutputSpec("wh", $"{ns}.live", "replace", "fail_on_change", new Dictionary<string, object?>()) { Keys = [] };
            Assert.True(sink.TryGetNativeCopy(spec, out var copy));
            await RunSetupAsync(duck, copy!.SetupStatements);
            await duck.ExecuteAsync("create table stage as select 1 as id union all select 2 union all select 3");
            await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "stage", StringComparison.Ordinal));
        }

        await catalog.MirrorTableAsync(ns, "live", Blobs(), keyPrefix: prefix + "/");
        var metadataVersion = await catalog.LatestMetadataVersionAsync(ns, "live");

        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(LiveFilesConfig(), CancellationToken.None);
        var read = new DatasetSpec("lake", $"{ns}.live", new Dictionary<string, object?> { ["metadata_version"] = metadataVersion });
        Assert.True(source.TryGetNativeScan(read, out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);

        Assert.Equal(6L, await duck.ScalarAsync<long>($"select sum(id)::bigint from {scan.SqlFragment}"));
    }
}
