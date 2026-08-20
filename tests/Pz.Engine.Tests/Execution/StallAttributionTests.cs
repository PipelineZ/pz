using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

/// <summary>Per-node channel stall attribution.
/// <see cref="SourceLoadExecutor"/> times both sides of its internal bounded(4) channel via
/// <see cref="StallAccumulator"/> — this proves the two fault seams (a slow ingest consumer, a slow
/// source producer) land on the correct side of that breakdown, and that Pipeline nodes (no channel)
/// never get a non-null <see cref="NodeResult.Timings"/>.</summary>
public sealed class StallAttributionTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private InMemoryConnector _mem = null!;
    private RunContext _ctx = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _mem = new InMemoryConnector();
        var reg = new ConnectorRegistry();
        reg.AddSource("inmemory", _mem);
        reg.AddSink("inmemory", _mem);
        _ctx = new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceLoadNode(long rows, Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["rows"] = rows };
        foreach (var (k, v) in extra ?? []) options[k] = v;
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", options, null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Slow_consumer_reports_producer_stall()
    {
        // Fake-clock deterministic: 32 tiny single-batch partitions; the consumer
        // parks in OnBatchConsumedForTests after appending its FIRST batch, until the test releases it.
        // The injected clock only moves at the single Advance below, so every bracket measures exactly
        // the advance if it spans it and exactly zero otherwise. Call census before the advance —
        // consumer: bracket-1 start + end (2 calls; it parks in the hook before bracket 2 can start);
        // producers: 32 write-bracket starts + exactly 5 ends (4 slots in the bounded(4) channel + the
        // 1 batch the consumer removed before parking) = 39 total, and no 40th call is possible until
        // release (the 27 remaining writers are blocked mid-bracket, the consumer is parked outside its
        // bracket). Advancing then gives ProducerStall == 27 × advance and ConsumerStall == 0 exactly,
        // independent of machine load. Census arithmetic depends on: one batch per partition
        // (rowsPerPartition << MaxRowsPerBatch), channel capacity 4, and stall brackets being the only
        // EffectiveTime.GetTimestamp callers — a violation surfaces loudly: as the WaitAsync timeout
        // below when the census can no longer be reached (capacity decrease, lost bracket, new
        // EffectiveTime caller), or as an exact-equality mismatch when it is exceeded (capacity increase).
        const int partitionCount = 32;
        const long rowsPerPartition = 10;
        const int censusBeforeAdvance = 2 + partitionCount + 5;
        var advance = TimeSpan.FromSeconds(10);

        var time = new CountingTimeProvider();
        var ctx = _ctx with { Time = time };
        var release = new TaskCompletionSource();
        var node = SourceLoadNode(0, new Dictionary<string, object?>
        {
            ["partition_sizes"] = Enumerable.Repeat(rowsPerPartition, partitionCount).ToArray(),
        });

        _duck.OnBatchConsumedForTests = () => release.Task.Wait();
        try
        {
            var execute = new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

            await time.WhenCallCountAtLeast(censusBeforeAdvance).WaitAsync(TimeSpan.FromSeconds(30));
            time.Advance(advance);
            release.SetResult();

            var result = await execute.WaitAsync(TimeSpan.FromSeconds(30));

            Assert.Equal(NodeStatus.Success, result.Status);
            Assert.NotNull(result.Timings);
            Assert.Equal(TimeSpan.FromSeconds(10 * 27), result.Timings!.ProducerStall);
            Assert.Equal(TimeSpan.Zero, result.Timings!.ConsumerStall);
        }
        finally
        {
            _duck.OnBatchConsumedForTests = null;
        }
    }

    [Fact]
    public async Task Slow_producer_reports_consumer_stall()
    {
        // Fake-clock deterministic: one partition, one batch (rows = 500 stays well
        // under ArrowBatchBuilder's MaxRowsPerBatch — the same census dependency as the 32-partition
        // test above; more than one batch would let the advance land between consumer brackets). The
        // producer parks in
        // rows_read_hook after reading its last row — BEFORE its write bracket — while the consumer's
        // first MoveNextAsync bracket is open against an empty channel. Call #1 on the injected clock is
        // necessarily that consumer bracket's start (the held producer takes no timestamp until its
        // write, and nothing else on this path consults EffectiveTime — see CountingTimeProvider doc),
        // so once it has happened AND the producer is at its gate, the single Advance lands inside the
        // consumer bracket and outside every producer bracket: ConsumerStall == advance and
        // ProducerStall == 0 exactly, independent of machine load.
        const long rows = 500;
        var advance = TimeSpan.FromSeconds(10);

        var time = new CountingTimeProvider();
        var ctx = _ctx with { Time = time };
        var atGate = new TaskCompletionSource();
        var release = new TaskCompletionSource();
        var node = SourceLoadNode(rows, new Dictionary<string, object?>
        {
            ["rows_read_hook"] = (Action<long>)(n =>
            {
                if (n == rows)
                {
                    atGate.SetResult();
                    release.Task.Wait();
                }
            }),
        });

        var execute = new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);
        await time.WhenCallCountAtLeast(1).WaitAsync(TimeSpan.FromSeconds(15));
        await atGate.Task.WaitAsync(TimeSpan.FromSeconds(15));
        time.Advance(advance);
        release.SetResult();

        var result = await execute.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Timings);
        Assert.Equal(advance, result.Timings!.ConsumerStall);
        Assert.Equal(TimeSpan.Zero, result.Timings!.ProducerStall);
    }

    [Fact]
    public async Task Pipeline_nodes_have_null_timings()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(100), _ctx, default);
        var pipeline = new PipelineDef("evens", "select * from staging.src_mem__numbers where flag",
            "table", [], [], "pipelines/evens.sql");
        var node = new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "evens",
            [new NodeId("aaaaaaaaaaaaaaaa")], pipeline.RawSql, pipeline);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Timings);
    }

    [Fact]
    public async Task Sink_write_reports_timings_too()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(300), _ctx, default);
        var sink = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [],
            "sinks/cap.yml") { Outputs = [new OutputDef("out", "src_mem__numbers", "replace", "fail_on_change", new Dictionary<string, object?>())] };
        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SinkWrite, "cap.out",
            [new NodeId("aaaaaaaaaaaaaaaa")], null, new SinkOutputDef(sink, sink.Outputs[0]));

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Timings);
    }

    /// <summary>Settable fake clock (same 1-tick == TimeSpan-tick convention as
    /// <c>Resilience.ManualTimeProvider</c>, which is sealed and census-free, hence a local fake) that
    /// counts <see cref="GetTimestamp"/> calls and lets the test await the Nth call. This is the seam
    /// that makes the two attribution tests load-independent: the clock only moves on
    /// <see cref="Advance"/>, and the test only advances once the call census proves which stall
    /// brackets are open. The census is sound because on a breaker-less success-path node the ONLY
    /// <c>RunContext.EffectiveTime.GetTimestamp()</c> callers are StallAccumulator's brackets
    /// (KindDispatchingExecutor times durations with raw Stopwatch; BreakerRegistry is null here) — if a
    /// new EffectiveTime consumer ever joins the node path, the census count stops being reached and the
    /// WaitAsync below times out loudly.</summary>
    private sealed class CountingTimeProvider : TimeProvider
    {
        private readonly object _lock = new();
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private long _ticks;
        private int _calls;
        private int _threshold;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp()
        {
            lock (_lock)
            {
                _calls++;
                if (_threshold > 0 && _calls >= _threshold)
                {
                    _reached.TrySetResult();
                }

                return _ticks;
            }
        }

        public void Advance(TimeSpan by)
        {
            lock (_lock)
            {
                _ticks += by.Ticks;
            }
        }

        /// <summary>Single-use threshold: completes once <see cref="GetTimestamp"/> has been called at
        /// least <paramref name="n"/> times (immediately if it already has).</summary>
        public Task WhenCallCountAtLeast(int n)
        {
            lock (_lock)
            {
                _threshold = n;
                return _calls >= n ? Task.CompletedTask : _reached.Task;
            }
        }
    }
}
