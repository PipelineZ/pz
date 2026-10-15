using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>The native scan, the native copy and the universal write against a real storage account over
/// HTTPS (see <see cref="LiveAzure"/>; skipped unless <c>PZ_TEST_AZURE_ACCOUNT</c> is set). The Azurite suites
/// prove statement shape and protocol over plain http; only this lane goes through TLS, which is where the
/// azure extension's transport choice matters.</summary>
[Trait("Category", LiveAzure.Category)]
public sealed class AzureLiveStorageTests : IAsyncLifetime
{
    private readonly string prefix = LiveAzure.NewPrefix("azureblob");
    private readonly string dir = Directory.CreateTempSubdirectory("pz-azure-live-").FullName;

    private static ConnectorConfig Config() => new(LiveAzure.AuthKeys());

    private Dictionary<string, object?> Location(string path, string format = "parquet") => new()
    {
        ["container"] = LiveAzure.Container,
        ["path"] = $"{prefix}/{path}",
        ["format"] = format,
    };

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

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    private static async Task<long> NativeCountAsync(DuckSession duck, DatasetSpec spec)
    {
        Assert.True(new AzureSource(Config()).TryGetNativeScan(spec, out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);
        return await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
    }

    [SkippableFact]
    public async Task Native_copy_then_native_scan_round_trips_over_https()
    {
        LiveAzure.SkipUnlessConfigured();
        await using var duck = DuckSession.Open(Path.Combine(dir, "native.duckdb"));

        var output = new OutputSpec("lake", "data", "replace", "fail_on_change", Location("native"));
        Assert.True(new AzureSink(Config()).TryGetNativeCopy(output, out var copy));
        await RunSetupAsync(duck, copy!.SetupStatements);
        await duck.ExecuteAsync(copy.CopySql.Replace(
            "{{source}}", "(select range as id from range(3))", StringComparison.Ordinal));

        Assert.Equal(3, await NativeCountAsync(duck, new DatasetSpec("lake", "data", Location("native/data.parquet"))));
    }

    [SkippableFact]
    public async Task Universal_write_lands_a_blob_the_native_scan_reads_over_https()
    {
        LiveAzure.SkipUnlessConfigured();
        var schema = new Schema([new Field("id", Int64Type.Default, nullable: true)], null);

        await using (var sink = new AzureSink(Config()))
        {
            var session = await sink.BeginWriteAsync(
                new OutputSpec("sink", "orders", "replace", "fail_on_change", Location("universal")), schema, CancellationToken.None);
            using var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendRange([1L, 2L, 3L, 4L]).Build()], 4);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            Assert.Equal(4, (await session.CommitAsync(CancellationToken.None)).RowsWritten);
            await session.DisposeAsync();
        }

        await using var duck = DuckSession.Open(Path.Combine(dir, "universal.duckdb"));
        Assert.Equal(4, await NativeCountAsync(duck, new DatasetSpec("lake", "orders", Location("universal/orders.parquet"))));
    }
}
