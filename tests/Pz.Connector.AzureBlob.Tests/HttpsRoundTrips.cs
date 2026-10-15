using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>The native scan, the native copy and the universal write over HTTPS, shared by the TLS Azurite lane
/// (<see cref="AzuriteTlsTests"/>) and the real-account lane (<see cref="AzureLiveStorageTests"/>). Both exist for
/// the azure extension's transport choice, which only TLS exercises.</summary>
internal static class HttpsRoundTrips
{
    private static Dictionary<string, object?> Location(string container, string path, string format = "parquet") => new()
    {
        ["container"] = container,
        ["path"] = path,
        ["format"] = format,
    };

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    private static async Task<long> NativeCountAsync(DuckSession duck, ConnectorConfig config, DatasetSpec spec)
    {
        Assert.True(new AzureSource(config).TryGetNativeScan(spec, out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);
        return await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
    }

    public static async Task NativeCopyThenNativeScanAsync(ConnectorConfig config, string container, string prefix, string dbPath)
    {
        await using var duck = DuckSession.Open(dbPath);

        var output = new OutputSpec("lake", "data", "replace", "fail_on_change", Location(container, $"{prefix}/native"));
        Assert.True(new AzureSink(config).TryGetNativeCopy(output, out var copy));
        await RunSetupAsync(duck, copy!.SetupStatements);
        await duck.ExecuteAsync(copy.CopySql.Replace(
            "{{source}}", "(select range as id from range(3))", StringComparison.Ordinal));

        Assert.Equal(3, await NativeCountAsync(duck, config,
            new DatasetSpec("lake", "data", Location(container, $"{prefix}/native/data.parquet"))));
    }

    public static async Task UniversalWriteThenNativeScanAsync(ConnectorConfig config, string container, string prefix, string dbPath)
    {
        var schema = new Schema([new Field("id", Int64Type.Default, nullable: true)], null);

        await using (var sink = new AzureSink(config))
        {
            var session = await sink.BeginWriteAsync(
                new OutputSpec("sink", "orders", "replace", "fail_on_change", Location(container, $"{prefix}/universal")),
                schema, CancellationToken.None);
            using var batch = new RecordBatch(schema, [new Int64Array.Builder().AppendRange([1L, 2L, 3L, 4L]).Build()], 4);
            await session.WriteBatchAsync(batch, CancellationToken.None);
            Assert.Equal(4, (await session.CommitAsync(CancellationToken.None)).RowsWritten);
            await session.DisposeAsync();
        }

        await using var duck = DuckSession.Open(dbPath);
        Assert.Equal(4, await NativeCountAsync(duck, config,
            new DatasetSpec("lake", "orders", Location(container, $"{prefix}/universal/orders.parquet"))));
    }
}
