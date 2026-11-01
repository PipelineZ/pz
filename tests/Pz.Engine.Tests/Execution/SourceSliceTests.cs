using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="RunContext.SourceSliceRows"/> (fresh previews): the connector is told the limit, and the
/// engine stops reading at it whether or not the connector honours it -- a source that never ends still
/// lands exactly the slice and the node succeeds.</summary>
public sealed class SourceSliceTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-slice-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode Node()
    {
        var source = new ConnectionDef("mem", "slicestub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "connections.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private async Task<NodeResult> Run(ISource source, ConnectorCapabilities caps, long? slice)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("slicestub", new StreamingStubConnector(source, caps));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance)
        {
            SourceSliceRows = slice,
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        return await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, timeout.Token);
    }

    private Task<long> Staged() => _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers");

    [Fact]
    public async Task A_source_that_ignores_the_limit_is_stopped_at_the_slice()
    {
        var partition = new CountingPartition(400, batches: null);
        var source = new SliceStubSource([partition]);

        var result = await Run(source, ConnectorCapabilities.None, 1000);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1000, result.RowsMoved);
        Assert.Equal(1000, await Staged());
        Assert.Equal(1000, source.ObservedLimit);
        Assert.True(partition.Stopped);
    }

    [Fact]
    public async Task A_mid_batch_cut_lands_exactly_the_slice()
    {
        var result = await Run(new SliceStubSource([new CountingPartition(700, batches: 3)]), ConnectorCapabilities.None, 1000);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1000, await Staged());
    }

    [Fact]
    public async Task A_streaming_source_is_capped_too()
    {
        var source = new SliceStubSource([new CountingPartition(400, batches: null)], streaming: true);

        var result = await Run(source, ConnectorCapabilities.StreamingPartitions, 1000);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1000, await Staged());
    }

    [Fact]
    public async Task A_stable_partition_source_skips_partition_mode_under_a_slice()
    {
        var result = await Run(new SliceStubSource([new CountingPartition(400, batches: null)]),
            ConnectorCapabilities.StablePartitionIds, 1000);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1000, await Staged());
        Assert.Equal(0, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where schema_name = 'pz_meta' and table_name = 'partitions_done'"));
    }

    // Review of batch 2: a sliced read cancelled the partition, and nothing keeps a sync token in a fresh
    // preview, so the post-read poll (an RPC for a process connector) is not made -- it could only fail.
    [Fact]
    public async Task A_sliced_feed_read_does_not_poll_the_sync_state()
    {
        var partition = new ThrowingSyncPartition();
        var source = new FeedSliceStubSource([partition]);

        var result = await Run(source, ConnectorCapabilities.None, 1000);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1000, await Staged());
        Assert.False(partition.Polled);
    }

    [Fact]
    public async Task No_slice_reads_everything()
    {
        var result = await Run(new SliceStubSource([new CountingPartition(700, batches: 3)]), ConnectorCapabilities.None, null);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(2100, await Staged());
    }
}

/// <summary>Yields <paramref name="batchRows"/>-row batches of consecutive ids, <paramref name="batches"/> of
/// them or forever (null); records when its read was ended (the enumerator disposed, however the engine
/// stopped it).</summary>
internal sealed class CountingPartition(int batchRows, int? batches) : IDatasetPartition
{
    public bool Stopped { get; private set; }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var next = 0L;
        try
        {
            for (var i = 0; batches is null || i < batches; i++)
            {
                await Task.Yield();
                ct.ThrowIfCancellationRequested();
                var builder = new Int64Array.Builder();
                for (var r = 0; r < batchRows; r++)
                {
                    builder.Append(next++);
                }

                yield return new RecordBatch(StubSchema.IdSchema, [builder.Build()], batchRows);
            }
        }
        finally
        {
            Stopped = true;
        }
    }
}

/// <summary>A source that ignores <see cref="ReadHints.Limit"/> but records it.</summary>
internal sealed class SliceStubSource(IReadOnlyList<IDatasetPartition> partitions, bool streaming = false)
    : ISource, IStreamingSource
{
    public long? ObservedLimit { get; private set; }

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        ObservedLimit = hints.Limit;
        return new(partitions);
    }

    public async IAsyncEnumerable<IDatasetPartition> PlanReadStreamingAsync(DatasetSpec spec, ReadHints hints,
        [EnumeratorCancellation] CancellationToken ct)
    {
        if (!streaming)
        {
            throw new InvalidOperationException("not a streaming source in this test");
        }

        ObservedLimit = hints.Limit;
        foreach (var partition in partitions)
        {
            await Task.Yield();
            yield return partition;
        }
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>A feed partition whose sync-state poll fails, as a process connector's RPC can after a cancel.</summary>
internal sealed class ThrowingSyncPartition() : IDatasetPartition, ISyncStatePartition
{
    private readonly CountingPartition _rows = new(400, batches: null);

    public bool Polled { get; private set; }

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) => _rows.ReadAsync(options, ct);

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        Polled = true;
        throw new PzConnectorException("read state unavailable after cancel", isTransient: false);
    }
}

/// <summary>A source whose natural read shape is a feed.</summary>
internal sealed class FeedSliceStubSource(IReadOnlyList<IDatasetPartition> partitions) : ISource, INaturalReadShapeSource
{
    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        new(partitions);

    public ValueTask DisposeAsync() => default;
}
