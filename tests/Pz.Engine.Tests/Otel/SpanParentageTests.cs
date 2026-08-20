using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Diagnostics.Otel;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Otel;

/// <summary>With a BCL <see cref="ActivityListener"/>
/// (AllDataAndRecorded) and a <see cref="MeterListener"/> registered, a small InMemory
/// source-&gt;pipeline-&gt;sink dag proves the promised run -&gt; node -&gt; stage span tree and that
/// <c>pz.rows_moved</c> sums to the actual rows moved. Both listeners are process-global BCL state, so
/// every assertion here identifies "my" spans by the unique <see cref="RunPaths.RunId"/> tagged onto the
/// root span (and then walks down via <see cref="Activity.ParentSpanId"/>, which is a cryptographically
/// random 8-byte value — collision-proof against whatever unrelated tests happen to run concurrently in
/// other xunit collections) rather than assuming this listener sees nothing else. This is the ONLY place
/// in the suite that registers such a listener — <c>tests/Pz.Diagnostics.Tests/Otel/OtelPrimitivesTests.cs</c>
/// (a separate test process) asserts the no-listener zero-cost case and must never observe one.</summary>
public sealed class SpanParentageTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private RunContext _ctx = null!;
    private string _runId = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        var mem = new InMemoryConnector();
        var reg = new ConnectorRegistry();
        reg.AddSource("inmemory", mem);
        reg.AddSink("inmemory", mem);
        _runId = Guid.NewGuid().ToString("N");
        _ctx = new RunContext(_duck, reg, new RunPaths(_dir, _runId), NullRunEvents.Instance);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static CompiledDag ThreeNodeDag(long rows)
    {
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?> { ["rows"] = rows }, null)],
            "sources/mem.yml");
        var sourceNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));

        var pipeline = new PipelineDef("evens", "select * from staging.src_mem__numbers", "view", [], [],
            "pipelines/evens.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "evens",
            [sourceNode.Id], pipeline.RawSql, pipeline);

        var sink = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [],
            "sinks/cap.yml") { Outputs = [new OutputDef("out", "evens", "replace", "fail_on_change", new Dictionary<string, object?>())] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "cap.out",
            [pipelineNode.Id], null, new SinkOutputDef(sink, sink.Outputs[0]));

        return new CompiledDag([sourceNode, pipelineNode, sinkNode]);
    }

    [Fact]
    public async Task Run_node_stage_span_tree_parents_correctly_and_rows_metric_sums()
    {
        var activities = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PzActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activities.Add,
        };
        ActivitySource.AddActivityListener(listener);

        // PzMeters is process-global static state (same reason PzActivitySource's listener needs care
        // above): other xunit collections running concurrently exercise SourceLoadExecutor/
        // SinkWriteExecutor too, and their pz.rows_moved measurements would land in this same callback.
        // Disambiguate the same way the span assertions do — at measurement time, Activity.Current is
        // exactly the node span PzMeters.RowsMoved.Add's call site is nested under (the Add call always
        // happens synchronously on that same node's ambient context), so only count a measurement whose
        // ambient node-id tag is one of this test's own three known node ids.
        var myNodeIds = new HashSet<string> { "1111111111111111", "3333333333333333" };
        long rowsMovedSum = 0;
        using var meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, ml) =>
            {
                if (instrument.Meter.Name == PzMeters.Name && instrument.Name == "pz.rows_moved")
                {
                    ml.EnableMeasurementEvents(instrument);
                }
            },
        };
        meterListener.SetMeasurementEventCallback<long>((_, measurement, _, _) =>
        {
            if (Activity.Current?.GetTagItem("pz.node.id") is string nodeId && myNodeIds.Contains(nodeId))
            {
                Interlocked.Add(ref rowsMovedSum, measurement);
            }
        });
        meterListener.Start();

        var dag = ThreeNodeDag(rows: 25);
        var orchestrator = new RunOrchestrator(new KindDispatchingExecutor(), _ctx);
        var result = await orchestrator.ExecuteAsync(dag, new RunOptions(MaxConcurrency: 4), default);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(3, result.Nodes.Count);

        var runSpan = Assert.Single(activities,
            a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == _runId);
        Assert.Null(runSpan.ParentId);

        var nodeSpans = activities
            .Where(a => a.OperationName.StartsWith("node.") && a.ParentSpanId == runSpan.SpanId)
            .ToList();
        Assert.Equal(3, nodeSpans.Count);

        var sourceNodeSpan = Assert.Single(nodeSpans, n => n.OperationName == "node.SourceLoad");
        Assert.Equal("success", sourceNodeSpan.GetTagItem("pz.node.status"));
        Assert.Equal("src_mem__numbers", sourceNodeSpan.GetTagItem("pz.node.name"));

        var pipelineNodeSpan = Assert.Single(nodeSpans, n => n.OperationName == "node.Pipeline");
        Assert.Equal("success", pipelineNodeSpan.GetTagItem("pz.node.status"));

        var sinkNodeSpan = Assert.Single(nodeSpans, n => n.OperationName == "node.SinkWrite");
        Assert.Equal("success", sinkNodeSpan.GetTagItem("pz.node.status"));

        var extractSpan = Assert.Single(activities,
            a => a.OperationName == "extract" && a.ParentSpanId == sourceNodeSpan.SpanId);
        var ingestSpan = Assert.Single(activities,
            a => a.OperationName == "ingest" && a.ParentSpanId == sourceNodeSpan.SpanId);
        Assert.NotNull(extractSpan);
        Assert.NotNull(ingestSpan);

        var egressSpan = Assert.Single(activities,
            a => a.OperationName == "egress" && a.ParentSpanId == sinkNodeSpan.SpanId);
        var writeSpan = Assert.Single(activities,
            a => a.OperationName == "write" && a.ParentSpanId == sinkNodeSpan.SpanId);
        Assert.NotNull(egressSpan);
        Assert.NotNull(writeSpan);

        // Pipeline nodes have no channel, so no stage-span children.
        Assert.DoesNotContain(activities, a => a.ParentSpanId == pipelineNodeSpan.SpanId);

        var expectedRowsMoved = result.Nodes
            .Where(n => n.Kind is NodeKind.SourceLoad or NodeKind.SinkWrite)
            .Sum(n => n.RowsMoved);
        Assert.Equal(expectedRowsMoved, Interlocked.Read(ref rowsMovedSum));
    }

    [Fact]
    public async Task Concurrent_sibling_nodes_each_parent_to_the_run_span()
    {
        // Two independent SourceLoad nodes (no edges between them) become ready and dispatch
        // concurrently under RunOrchestrator's semaphore-gated dispatch. RunOrchestrator spawns each
        // via a plain fire-and-forget `_ = RunNodeAsync(node)` call (no Task.Run) -- this proves the
        // ambient Activity.Current (and thus correct span parentage) still flows through that dispatch
        // shape for two nodes running genuinely concurrently, not just sequentially.
        var activities = new ConcurrentBag<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PzActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activities.Add,
        };
        ActivitySource.AddActivityListener(listener);

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [
                new DatasetDef("a", new Dictionary<string, object?> { ["rows"] = 10L }, null),
                new DatasetDef("b", new Dictionary<string, object?> { ["rows"] = 10L }, null),
            ], "sources/mem.yml");
        var nodeA = new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__a",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
        var nodeB = new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.SourceLoad, "src_mem__b",
            [], null, new SourceDatasetDef(source, source.Datasets[1]));

        var dag = new CompiledDag([nodeA, nodeB]);
        var orchestrator = new RunOrchestrator(new KindDispatchingExecutor(), _ctx);
        var result = await orchestrator.ExecuteAsync(dag, new RunOptions(MaxConcurrency: 2), default);

        Assert.Equal(RunStatus.Success, result.Status);

        var runSpan = Assert.Single(activities,
            a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == _runId);
        var nodeSpans = activities
            .Where(a => a.OperationName.StartsWith("node.") && a.ParentSpanId == runSpan.SpanId)
            .ToList();
        Assert.Equal(2, nodeSpans.Count);
    }
}
