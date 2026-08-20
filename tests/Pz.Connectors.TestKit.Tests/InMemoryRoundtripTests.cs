using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;

public class InMemoryRoundtripTests
{
    private static DatasetSpec Dataset(long rows, int partitions = 1) => new("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = rows, ["partitions"] = partitions });

    private static OutputSpec Output() => new("memsink", "out", "replace", "fail_on_change",
        new Dictionary<string, object?>());

    [Fact]
    public async Task Roundtrip_source_to_sink_preserves_rows_and_types()
    {
        var connector = new InMemoryConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var schema = await source.GetSchemaAsync(Dataset(500), CancellationToken.None);
        await using var sink = await ((ISinkConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(Output(), schema.Schema, CancellationToken.None);

        long rowsSeen = 0;
        var partitions = await source.PlanReadAsync(Dataset(500), ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 8192), CancellationToken.None))
            {
                rowsSeen += batch.Length;
                await session.WriteBatchAsync(batch, CancellationToken.None);
                batch.Dispose();
            }
        }
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(500, rowsSeen);
        Assert.Equal(500, result.RowsWritten);
        var committed = Assert.Single(connector.Committed);
        Assert.Equal(500, committed.Batches.Sum(b => (long)b.Length));
        var firstIds = (Int64Array)committed.Batches[0].Column(0);
        Assert.Equal(0L, firstIds.GetValue(0));
    }

    [Fact]
    public async Task Partitioned_read_unions_to_full_dataset()
    {
        var connector = new InMemoryConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var partitions = await source.PlanReadAsync(Dataset(100, partitions: 3), ReadHints.None, CancellationToken.None);
        Assert.Equal(3, partitions.Count);
        var ids = new List<long>();
        foreach (var partition in partitions)
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var col = (Int64Array)batch.Column(0);
                for (var i = 0; i < col.Length; i++) ids.Add(col.GetValue(i)!.Value);
                batch.Dispose();
            }
        Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), ids.OrderBy(x => x));
    }

    [Fact]
    public async Task Abort_discards_and_counts()
    {
        var connector = new InMemoryConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        await using var source = await ((ISourceConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var schema = await source.GetSchemaAsync(Dataset(10), CancellationToken.None);
        await using (var session = await sink.BeginWriteAsync(Output(), schema.Schema, CancellationToken.None))
        {
            await session.AbortAsync(CancellationToken.None);
        }
        Assert.Empty(connector.Committed);
        Assert.Equal(1, connector.AbortedSessions);
    }

    [Fact]
    public async Task Fault_injection_throws_typed_transient_exception()
    {
        var connector = new InMemoryConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var spec = new DatasetSpec("mem", "numbers", new Dictionary<string, object?>
        {
            ["rows"] = 1000L, ["fail_read_at_batch"] = 1, ["fail_transient"] = true,
        });
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(new BatchOptions(TargetBatchBytes: 4096), CancellationToken.None))
                batch.Dispose();
        });
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new InMemoryConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }
}
