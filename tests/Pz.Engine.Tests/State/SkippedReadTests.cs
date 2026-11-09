using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>A read a rule of the dataset's config skipped (<see cref="ISkippedReadPartition"/>) fails an incremental
/// node with PZ0540, on every read path, before anything it read is committed; a non-incremental node succeeds as
/// before.</summary>
public sealed class SkippedReadTests : IAsyncLifetime
{
    private const string Rule = "on_status[0] (skip) matched HTTP 400";

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-skipped-read-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private WatermarkStore _store = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    public enum Path_ { Channel, Streaming, PartitionMode, StreamingPartitionMode, Checkpointing, Feed, FeedPartitionMode }

    private RunContext Ctx(SkippingConnector connector, bool fullRefresh = false)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", connector);
        return new RunContext(_duck, registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh);
    }

    private static DagNode Node(string dataset, IncrementalDef? incremental, long rows = 25)
    {
        var def = new DatasetDef(dataset, new Dictionary<string, object?> { ["rows"] = rows },
            new Dictionary<string, string> { ["id"] = "bigint" },
            incremental is null ? null : new SyncModeDef(SyncMode.Incremental, incremental));
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(), [def], "connections.yml");
        return new DagNode(new NodeId("c1c1c1c1c1c1c1c1"), NodeKind.SourceLoad, $"src_mem__{dataset}", [], null,
            new SourceDatasetDef(source, def));
    }

    private Task<NodeResult> RunAsync(DagNode node, RunContext ctx) =>
        new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

    [Theory]
    [InlineData(Path_.Channel)]
    [InlineData(Path_.Streaming)]
    [InlineData(Path_.PartitionMode)]
    [InlineData(Path_.StreamingPartitionMode)]
    [InlineData(Path_.Checkpointing)]
    public async Task A_skipped_read_fails_an_incremental_node_with_PZ0540_naming_the_rule(Path_ path)
    {
        _store.Set(WatermarkStore.Key("mem", "w"), new Watermark("id", "bigint", "5", "prior-run"));
        var connector = new SkippingConnector(new InMemoryConnector(), path);

        var result = await RunAsync(Node("w", new IncrementalDef("id")), Ctx(connector));

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(PzErrorCode.SkippedReadOnIncremental, result.Error!.Code);
        Assert.Contains("source 'mem.w'", result.Error.Message);
        Assert.Contains(Rule, result.Error.Message);
        Assert.Null(result.WatermarkCandidate);
        Assert.Equal(1, connector.Attempts); // permanent: never retried
        if (path is Path_.PartitionMode or Path_.StreamingPartitionMode or Path_.Checkpointing)
        {
            // Nothing the skipped read produced is recorded as done or resumable, so pz retry reads it again.
            Assert.Equal(0L, await _duck.ScalarAsync<long>(
                "select count(*) from pz_meta.partitions_done where partition_id = 'p0'", default));
            Assert.Equal(0L, await _duck.ScalarAsync<long>(
                "select count(*) from pz_meta.partition_checkpoints where partition_id = 'p0'", default));
        }
    }

    [Theory]
    [InlineData(Path_.Feed)]
    [InlineData(Path_.FeedPartitionMode)]
    public async Task A_skipped_read_fails_a_feed_node_that_would_commit_a_sync_token(Path_ path)
    {
        // An auto dataset over a feed connector: the partition hands back a next token, and committing it after a
        // skip would move the feed past rows never read.
        var connector = new SkippingConnector(new InMemoryConnector(), path);

        var result = await RunAsync(Node("feed", incremental: null), Ctx(connector));

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(PzErrorCode.SkippedReadOnIncremental, result.Error!.Code);
        Assert.Null(result.SyncStateCandidate);
    }

    [Fact]
    public async Task A_skipped_read_fails_a_windowed_first_run_and_a_full_refresh_too()
    {
        var windowed = new IncrementalDef("id", "10", "0", null);

        var first = await RunAsync(Node("f", windowed), Ctx(new SkippingConnector(new InMemoryConnector(), Path_.Channel)));
        _store.Set(WatermarkStore.Key("mem", "r"), new Watermark("id", "bigint", "5", "prior-run"));
        var refresh = await RunAsync(Node("r", windowed),
            Ctx(new SkippingConnector(new InMemoryConnector(), Path_.Channel), fullRefresh: true));

        Assert.Equal(PzErrorCode.SkippedReadOnIncremental, first.Error!.Code);
        Assert.Equal(PzErrorCode.SkippedReadOnIncremental, refresh.Error!.Code);
    }

    [Theory]
    [InlineData(Path_.Channel)]
    [InlineData(Path_.PartitionMode)]
    public async Task A_skipped_read_on_a_non_incremental_node_succeeds(Path_ path)
    {
        var result = await RunAsync(Node("n", incremental: null),
            Ctx(new SkippingConnector(new InMemoryConnector(), path)));

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Error);
    }

    /// <summary>The in-memory reference connector over one partition that reads its rows, then reports
    /// <see cref="Rule"/> as having skipped part of the read. <see cref="Path_"/> picks the read path.</summary>
    public sealed class SkippingConnector(InMemoryConnector inner, Path_ path) : ISourceConnector
    {
        public int Attempts;

        public ConnectorInfo Info => inner.Info;

        public ConnectorCapabilities Capabilities => inner.Capabilities | path switch
        {
            Path_.Streaming => ConnectorCapabilities.StreamingPartitions,
            Path_.PartitionMode => ConnectorCapabilities.StablePartitionIds,
            Path_.StreamingPartitionMode => ConnectorCapabilities.StablePartitionIds | ConnectorCapabilities.StreamingPartitions,
            Path_.Checkpointing => ConnectorCapabilities.StablePartitionIds | ConnectorCapabilities.CheckpointableReads,
            Path_.Feed => ConnectorCapabilities.SyncState,
            Path_.FeedPartitionMode => ConnectorCapabilities.SyncState | ConnectorCapabilities.StablePartitionIds,
            _ => ConnectorCapabilities.None,
        };

        public string ConnectionConfigSchema => inner.ConnectionConfigSchema;
        public string DatasetConfigSchema => ((ISourceConnector)inner).DatasetConfigSchema;

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            inner.ValidateAsync(config, ct);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            inner.CheckConnectionAsync(config, ct);

        public async ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct)
        {
            Interlocked.Increment(ref Attempts);
            return new Source(await ((ISourceConnector)inner).OpenAsync(config, ct), path);
        }

        private sealed class Source(ISource inner, Path_ path) : ISource, IStreamingSource, INaturalReadShapeSource
        {
            public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) =>
                path is Path_.Feed or Path_.FeedPartitionMode ? NaturalReadShape.Feed : NaturalReadShape.Full;

            public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
                inner.GetSchemaAsync(spec, ct);

            public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
            {
                scan = null;
                return false;
            }

            public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints,
                CancellationToken ct)
            {
                var planned = await inner.PlanReadAsync(spec, hints, ct);
                return [.. planned.Select((p, i) => Wrap(p, i))];
            }

            public async IAsyncEnumerable<IDatasetPartition> PlanReadStreamingAsync(DatasetSpec spec, ReadHints hints,
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
            {
                foreach (var partition in await PlanReadAsync(spec, hints, ct))
                {
                    yield return partition;
                }
            }

            private IDatasetPartition Wrap(IDatasetPartition p, int i) => path switch
            {
                Path_.Checkpointing => new CheckpointingPartition(p, $"p{i}"),
                Path_.Feed or Path_.FeedPartitionMode => new FeedPartition(p, $"p{i}"),
                _ => new Partition(p, $"p{i}"),
            };

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }

        private class Partition(IDatasetPartition inner, string id) : IIdentifiedPartition, ISkippedReadPartition
        {
            public string PartitionId => id;

            public string? SkippedBy { get; private set; }

            public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
            {
                SkippedBy = Rule;
                await foreach (var batch in inner.ReadAsync(options, ct))
                {
                    yield return batch;
                }
            }
        }

        /// <summary>A feed partition that skipped part of its read and still offers the next sync token.</summary>
        private sealed class FeedPartition(IDatasetPartition inner, string id) : Partition(inner, id), ISyncStatePartition
        {
            public bool TryGetSyncStateCandidate(out string? candidate)
            {
                candidate = "next-token";
                return true;
            }
        }

        /// <summary>Offers a checkpoint after every batch: the skip must stop the first one being stored.</summary>
        private sealed class CheckpointingPartition(IDatasetPartition inner, string id)
            : Partition(inner, id), ICheckpointingPartition
        {
            public bool TryResumeFrom(string checkpoint) => false;

            public bool TryGetCheckpoint(out string? checkpoint)
            {
                checkpoint = "after-batch";
                return true;
            }
        }
    }
}
