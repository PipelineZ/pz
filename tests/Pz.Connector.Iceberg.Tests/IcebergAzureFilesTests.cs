using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>Proof of the Azure storage path that docker CAN host: a <c>files</c> read over an
/// <c>az://</c> root on Azurite, through the real ISource surface (<c>install azure</c>, the scoped
/// <c>type azure</c> secret, the moved-path scan). The table is written by the REST+MinIO fixture
/// and mirrored, because no REST catalog can keep its warehouse on Azurite (no DFS endpoint) —
/// REST writes on Azure are covered by the env-gated IcebergAzureRestTests.</summary>
[Collection("iceberg-rest")]
public sealed class IcebergAzureFilesTests(IcebergRestCatalogFixture catalog, IcebergAzuriteFixture azurite)
    : IClassFixture<IcebergAzuriteFixture>, IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-iceberg-azure-e2e-").FullName;
    private readonly string ns = "a" + Guid.NewGuid().ToString("N")[..8];

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private ConnectorConfig RestConfig() => new(new Dictionary<string, object?>
    {
        ["catalog"] = "rest", ["endpoint"] = catalog.Endpoint, ["warehouse"] = "wh", ["token"] = "unchecked-by-the-fixture",
        ["storage_key_id"] = catalog.AccessKey, ["storage_secret_key"] = catalog.SecretKey,
        ["storage_endpoint"] = catalog.StorageEndpoint, ["storage_url_style"] = "path", ["storage_use_ssl"] = false,
    });

    private ConnectorConfig AzureFilesConfig() => new(new Dictionary<string, object?>
    {
        ["catalog"] = "files",
        ["root"] = azurite.Root,
        ["storage_auth"] = "connection_string",
        ["storage_connection_string"] = azurite.ConnectionString,
    });

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    [SkippableFact]
    public async Task A_files_root_on_azure_blob_reads_a_mirrored_table_with_the_watermark_pushed_down()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = DuckSession.Open(Path.Combine(dir, "scratch.duckdb"));

        // Write through the REST catalog onto MinIO.
        await using (var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(RestConfig(), CancellationToken.None))
        {
            var spec = new OutputSpec("wh", $"{ns}.mirrored", "replace", "fail_on_change", new Dictionary<string, object?>()) { Keys = [] };
            Assert.True(sink.TryGetNativeCopy(spec, out var copy));
            await RunSetupAsync(duck, copy!.SetupStatements);
            await duck.ExecuteAsync("create table stage as select 1 as id union all select 2 union all select 3");
            await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "stage", StringComparison.Ordinal));
        }

        await catalog.MirrorTableAsync(ns, "mirrored", azurite.Blobs);
        var metadataVersion = await catalog.LatestMetadataVersionAsync(ns, "mirrored");

        // Read it back from Azurite through the files catalog.
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(AzureFilesConfig(), CancellationToken.None);
        var read = new DatasetSpec("lake", $"{ns}.mirrored", new Dictionary<string, object?> { ["metadata_version"] = metadataVersion })
            { WatermarkCursor = "id", WatermarkValue = "1" };
        Assert.True(source.TryGetNativeScan(read, out var scan));
        Assert.Contains("install azure", scan!.SetupStatements);
        Assert.Contains(scan.SetupStatements, s => s.StartsWith("create or replace secret", StringComparison.Ordinal) && s.Contains("type azure", StringComparison.Ordinal) && s.EndsWith($"scope 'az://{IcebergAzuriteFixture.Container}/')", StringComparison.Ordinal));
        await RunSetupAsync(duck, scan.SetupStatements);

        await duck.ExecuteAsync($"create table landed as select * from {scan.SqlFragment}");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from landed"));
        Assert.Equal(5L, await duck.ScalarAsync<long>("select sum(id)::bigint from landed"));
    }
}
