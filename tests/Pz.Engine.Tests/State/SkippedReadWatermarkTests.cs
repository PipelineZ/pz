using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>A read a connector rule skipped (<see cref="ISkippedReadPartition"/>) is not an empty window: the
/// engine captures no watermark candidate for it, on the channel path and in partition mode, and says so.</summary>
public sealed class SkippedReadWatermarkTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-skipped-read-tests", Guid.NewGuid().ToString("N"));
    private readonly List<string> _notices = [];
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

    private RunContext Ctx(bool skipped, bool stableIds)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new SkippingConnector(new InMemoryConnector(), skipped, stableIds));
        return new RunContext(_duck, registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, Notice: _notices.Add);
    }

    private static DagNode Windowed(NodeId id, string dataset, long rows) => Node(id, dataset, rows,
        new IncrementalDef("id", "10", "0", null));

    private static DagNode Node(NodeId id, string dataset, long rows, IncrementalDef incremental)
    {
        var def = new DatasetDef(dataset, new Dictionary<string, object?> { ["rows"] = rows },
            new Dictionary<string, string> { ["id"] = "bigint" }, new SyncModeDef(SyncMode.Incremental, incremental));
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(), [def], "connections.yml");
        return new DagNode(id, NodeKind.SourceLoad, $"src_mem__{dataset}", [], null, new SourceDatasetDef(source, def));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_skipped_read_of_an_empty_window_does_not_advance_to_the_ceiling(bool stableIds)
    {
        var key = WatermarkStore.Key("mem", "w");
        _store.Set(key, new Watermark("id", "bigint", "30", "prior-run"));

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx(skipped: true, stableIds))
            .ExecuteAsync(new CompiledDag([Windowed(new NodeId("c1c1c1c1c1c1c1c1"), "w", 25)]), new RunOptions(), default);

        var node = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Success, node.Status);
        Assert.Equal(0, node.RowsMoved);
        Assert.Null(node.WatermarkCandidate);
        Assert.Contains(_notices, n => n.StartsWith("source 'mem.w': a connector rule skipped part or all of this read",
            StringComparison.Ordinal) && n.Contains("watermark is not advanced", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_skipped_read_that_still_landed_rows_captures_no_candidate_either()
    {
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx(skipped: true, stableIds: false))
            .ExecuteAsync(new CompiledDag([Node(new NodeId("c2c2c2c2c2c2c2c2"), "p", 5, new IncrementalDef("id"))]),
                new RunOptions(), default);

        var node = Assert.Single(result.Nodes);
        Assert.Equal(5, node.RowsMoved);
        Assert.Null(node.WatermarkCandidate);
    }

    [Fact]
    public async Task An_empty_window_nobody_skipped_still_advances_as_before()
    {
        var key = WatermarkStore.Key("mem", "e");
        _store.Set(key, new Watermark("id", "bigint", "30", "prior-run"));

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx(skipped: false, stableIds: false))
            .ExecuteAsync(new CompiledDag([Windowed(new NodeId("c3c3c3c3c3c3c3c3"), "e", 25)]), new RunOptions(), default);

        Assert.Equal("40", Assert.Single(result.Nodes).WatermarkCandidate!.Value);
        Assert.Empty(_notices);
    }

    /// <summary>The in-memory reference connector, with partitions that report <paramref name="skipped"/> and,
    /// when <paramref name="stableIds"/>, carry ids so the engine reads them in partition mode.</summary>
    private sealed class SkippingConnector(InMemoryConnector inner, bool skipped, bool stableIds) : ISourceConnector
    {
        public ConnectorInfo Info => inner.Info;

        public ConnectorCapabilities Capabilities => inner.Capabilities | (stableIds
            ? ConnectorCapabilities.StablePartitionIds
            : ConnectorCapabilities.None);

        public string ConnectionConfigSchema => inner.ConnectionConfigSchema;
        public string DatasetConfigSchema => ((ISourceConnector)inner).DatasetConfigSchema;

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            inner.ValidateAsync(config, ct);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            inner.CheckConnectionAsync(config, ct);

        public async ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            new Source(await ((ISourceConnector)inner).OpenAsync(config, ct), skipped);

        private sealed class Source(ISource inner, bool skipped) : ISource
        {
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
                return [.. planned.Select((p, i) => (IDatasetPartition)new Partition(p, $"p{i}", skipped))];
            }

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }

        private sealed class Partition(IDatasetPartition inner, string id, bool skipped)
            : IIdentifiedPartition, ISkippedReadPartition
        {
            private bool _done;

            public string PartitionId => id;

            public bool ReadSkipped => skipped && _done;

            public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
                [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
            {
                await foreach (var batch in inner.ReadAsync(options, ct))
                {
                    yield return batch;
                }

                _done = true;
            }
        }
    }
}
