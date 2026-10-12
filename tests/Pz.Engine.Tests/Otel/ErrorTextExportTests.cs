using System.Collections.Concurrent;
using System.Diagnostics;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Diagnostics.Otel;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Tests.Resilience;

namespace Pz.Engine.Tests.Otel;

/// <summary>A connector's error text can echo data values (a duplicate-key message quotes the key), so an
/// exported span carries the error code and type only. The local artifacts keep the full message.</summary>
public sealed class ErrorTextExportTests : IAsyncLifetime
{
    private const string Marker = "pzsecret-7f3a";
    private static readonly string Message = "E11000 duplicate key error: dup key: { _id: \"" + Marker + "\" }";

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

    private static ConcurrentBag<Activity> Listen(out ActivityListener listener)
    {
        var stopped = new ConcurrentBag<Activity>();
        listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == PzActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = stopped.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return stopped;
    }

    private static DagNode FailingSource(bool transient)
    {
        var options = new Dictionary<string, object?>
        {
            ["rows"] = 10L, ["fail_read_at_batch"] = 0, ["fail_message"] = Message,
        };
        if (transient)
        {
            options["fail_transient"] = true;
            options["fail_read_retry_limit"] = new RetryCounter(1);
        }

        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("boom", options, null)], "sources/mem.yml");
        return new DagNode(new NodeId("4444444444444444"), NodeKind.SourceLoad, "src_mem__boom",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static void AssertNoMarker(Activity span) => NodeSpanAttributeTests.AssertNoText(span, Marker);

    [Fact]
    public async Task A_failed_node_and_its_run_export_the_code_only_while_run_results_keep_the_message()
    {
        var stopped = Listen(out var listener);
        using var _ = listener;
        var orchestrator = new RunOrchestrator(new KindDispatchingExecutor(), _ctx);

        var result = await orchestrator.ExecuteAsync(new CompiledDag([FailingSource(transient: false)]),
            new RunOptions(MaxConcurrency: 1), default);

        var node = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Failed, node.Status);
        Assert.Contains(Marker, node.Error!.Message);

        var runSpan = Assert.Single(stopped, a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == _runId);
        var nodeSpan = Assert.Single(stopped, a => a.OperationName == "node.SourceLoad" && a.ParentSpanId == runSpan.SpanId);
        Assert.Equal(ActivityStatusCode.Error, nodeSpan.Status);
        Assert.Equal("PZ0501", nodeSpan.StatusDescription);
        var ex = Assert.Single(nodeSpan.Events, e => e.Name == "exception");
        Assert.Equal("PZ0501", ex.Tags.Single(t => t.Key == "exception.type").Value);
        Assert.Equal("PZ0501", ex.Tags.Single(t => t.Key == "pz.error.code").Value);
        Assert.DoesNotContain(ex.Tags, t => t.Key is "exception.message" or "exception.stacktrace");
        AssertNoMarker(nodeSpan);
        AssertNoMarker(runSpan);
        foreach (var span in stopped.Where(a => a.TraceId == runSpan.TraceId)) AssertNoMarker(span);

        new RunResultsWriter(_ctx.Paths, "2026-01-01T00:00:00.000Z").WriteSnapshot(result.Nodes, "failed");
        Assert.Contains(Marker, File.ReadAllText(_ctx.Paths.RunResultsPath));
    }

    [Fact]
    public async Task A_retried_node_exports_retry_events_without_message_text()
    {
        var stopped = Listen(out var listener);
        using var _ = listener;
        var executor = new KindDispatchingExecutor(delay: (_, _) => Task.CompletedTask);
        var orchestrator = new RunOrchestrator(executor, _ctx);

        var result = await orchestrator.ExecuteAsync(new CompiledDag([FailingSource(transient: true)]),
            new RunOptions(MaxConcurrency: 1), default);

        Assert.Equal(NodeStatus.Success, Assert.Single(result.Nodes).Status);
        var runSpan = Assert.Single(stopped, a => a.OperationName == "run" && (string?)a.GetTagItem("pz.run.id") == _runId);
        var nodeSpan = Assert.Single(stopped, a => a.OperationName == "node.SourceLoad" && a.ParentSpanId == runSpan.SpanId);
        var retry = Assert.Single(nodeSpan.Events, e => e.Name == "retry_scheduled");
        Assert.Equal("PZ0501", retry.Tags.Single(t => t.Key == "pz.error.code").Value);
        Assert.DoesNotContain(retry.Tags, t => t.Key is "reason" or "exception.message");
        AssertNoMarker(nodeSpan);
        AssertNoMarker(runSpan);
    }
}
