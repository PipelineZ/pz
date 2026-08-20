using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>SQL-declared incremental: flow-level coverage of the SQL route through the
/// real dispatcher/executor stack (<see cref="RunOrchestrator"/>/<see cref="KindDispatchingExecutor"/>) plus
/// a real <see cref="DuckSession"/>, exactly as `pz run` would but without the CLI/project-loading layer.
/// The sibling <see cref="WatermarkFlowTests"/> proves the same commit-gated advancement rules for the
/// YAML-declared route; this class proves the SQL route inherits them, with the SQL-declared shapes
/// DagCompiler actually produces:
/// <list type="bullet">
/// <item>the SourceLoad's dataset carries <c>IncrementalDef(cursor, DeclaredInSql: true, SqlBounds: [...])</c>
/// (crib: <c>SqlBoundEvaluationTests</c>), whose bounds the executor evaluates in DuckDB against the stored
/// watermark and stamps on the <see cref="DatasetSpec"/> — the InMemory source's decision-10 delta lever
/// (<c>id &lt;= WatermarkValue</c> skip) then makes "extract only the delta" observable as a row count;</item>
/// <item>the Pipeline node carries the NULL-guarded <see cref="DagNode.RenderedSql"/> +
/// <see cref="DagNode.WatermarkSubstitutions"/> (crib: <c>PipelineSubstitutionTests</c>), so
/// <see cref="PipelineExecutor"/> rewrites the quoted sentinel to a typed literal (or NULL) at execute time —
/// this is where a lookback's inclusive overlap cut actually lands when the connector cannot push it.</item>
/// </list>
/// Topology is the real three-kind chain a compiled `INSERT INTO {{ sink }} select … from {{ source }}
/// where cursor &gt; {{ watermark }}` produces: SourceLoad → Pipeline → SinkWrite.</summary>
public sealed class SqlWatermarkFlowTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sqlwatermark-flow-tests", Guid.NewGuid().ToString("N"));
    private InMemoryConnector _mem = null!;
    private ConnectorRegistry _registry = null!;
    private WatermarkStore _store = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _mem = new InMemoryConnector();
        _registry = new ConnectorRegistry();
        _registry.AddSource("inmemory", _mem);
        _registry.AddSink("inmemory", _mem);
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
        return Task.CompletedTask;
    }

    /// <summary>The quoted watermark() sentinel WatermarkInference records for `mem.&lt;dataset&gt;` — the
    /// exact token PipelineExecutor/SourceLoadExecutor rewrite out (<c>__pz_watermark__&lt;source&gt;__&lt;dataset&gt;__</c>).</summary>
    private static string Sentinel(string dataset) => $"__pz_watermark__mem__{dataset}__";

    /// <summary>A fresh, uniquely-named staging DuckDB session + matching <see cref="RunContext"/> — exactly
    /// what a real second `pz run` opens (a brand-new <c>staging.duckdb</c> per run id). Reusing one session
    /// across two runs would hit "table already exists" on the second CREATE, a fixture artifact, not the
    /// behavior under test. <paramref name="notices"/>, when supplied, captures the run's ctx.Notice stream
    /// (the inclusive-bound-dropped / caught-up notices).</summary>
    private async Task<(DuckSession Duck, RunContext Ctx)> NewRunAsync(
        string runId, bool fullRefresh = false, List<string>? notices = null)
    {
        var duck = DuckSession.Open(Path.Combine(_dir, $"{runId}.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        var ctx = new RunContext(duck, _registry, new RunPaths(_dir, runId), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh, Notice: notices is null ? null : notices.Add);
        return (duck, ctx);
    }

    /// <summary>Builds the real SourceLoad → Pipeline → SinkWrite chain for a SQL-declared incremental
    /// dataset. <paramref name="bounds"/> are the executor-side pushdown traces (evaluated in DuckDB);
    /// <paramref name="guard"/> is the pipeline's NULL-guarded predicate over the source's staging table,
    /// with every <c>'sentinel'</c> occurrence rewritten by PipelineExecutor at execute time. Cursor is the
    /// InMemory source's own int64 <c>id</c> column (bigint), so the decision-10 delta lever and the
    /// post-land MAX(id) capture both key off the same column the bounds cut on.</summary>
    private (CompiledDag Dag, NodeId SourceId, NodeId PipelineId, NodeId SinkId) BuildDag(
        string dataset, long rows, IReadOnlyList<SqlWatermarkBound> bounds, string guard,
        string sinkMode = "replace", IReadOnlyList<string>? keys = null,
        Dictionary<string, object?>? sinkOptions = null)
    {
        var pipelineName = $"{dataset}_out";
        var outputName = $"{dataset}_synced";

        var incremental = new IncrementalDef("id", DeclaredInSql: true, SqlBounds: bounds);
        var columns = new Dictionary<string, string> { ["id"] = "bigint" };
        var datasetDef = new DatasetDef(dataset, new Dictionary<string, object?> { ["rows"] = rows }, columns,
            new SyncModeDef(SyncMode.Incremental, incremental));
        var sourceDef = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(), [datasetDef], "sources/mem.yml");
        var sourceId = NodeId.Compute($"src_mem__{dataset}");
        var sourceNode = new DagNode(sourceId, NodeKind.SourceLoad, $"src_mem__{dataset}", [], null,
            new SourceDatasetDef(sourceDef, datasetDef));

        var renderedSql = $"select id, name, amount, flag, ts from staging.src_mem__{dataset} where {guard}";
        var pipelineDef = new PipelineDef(pipelineName, renderedSql, "table", [], [], $"pipelines/{pipelineName}.sql");
        var pipelineId = NodeId.Compute(pipelineName);
        var pipelineNode = new DagNode(pipelineId, NodeKind.Pipeline, pipelineName, [sourceId], renderedSql, pipelineDef)
        {
            WatermarkSubstitutions = [new WatermarkSubstitution(Sentinel(dataset), "mem", dataset, "bigint")],
        };

        var output = new OutputDef(outputName, pipelineName, sinkMode, "fail_on_change",
            sinkOptions ?? [], keys ?? []);
        var sinkDef = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [], "sinks/cap.yml") { Outputs = [output] };
        var sinkId = NodeId.Compute($"cap.{outputName}");
        var sinkNode = new DagNode(sinkId, NodeKind.SinkWrite, $"cap.{outputName}", [pipelineId], null,
            new SinkOutputDef(sinkDef, output));

        return (new CompiledDag([sourceNode, pipelineNode, sinkNode]), sourceId, pipelineId, sinkId);
    }

    /// <summary>Exclusive `cursor &gt; watermark` — the ordinary SQL-declared shape. Bound pushes to the
    /// source (delta lever); the pipeline guard re-applies the same cut on the staging table.</summary>
    private static (SqlWatermarkBound[] Bounds, string Guard) Exclusive(string dataset, string pipelineName)
    {
        var s = Sentinel(dataset);
        return ([new SqlWatermarkBound(pipelineName, Inclusive: false, $"'{s}'", s)],
            $"('{s}' is null or id > '{s}')");
    }

    private static Watermark Wm(string value) => new("id", "bigint", value, "prior-run");

    private static NodeResult NodeOf(IReadOnlyList<NodeResult> nodes, NodeId id) => nodes.Single(n => n.Id == id);

    private long CommittedRows(string outputName) =>
        _mem.Committed.Where(c => c.Spec.Output == outputName).SelectMany(c => c.Batches).Sum(b => (long)b.Length);

    private long CommittedDistinctIds(string outputName)
    {
        var ids = new HashSet<long>();
        foreach (var write in _mem.Committed.Where(c => c.Spec.Output == outputName))
        {
            foreach (var batch in write.Batches)
            {
                var idCol = (Apache.Arrow.Int64Array)batch.Column("id");
                for (var i = 0; i < idCol.Length; i++) { ids.Add(idCol.GetValue(i)!.Value); }
            }
        }

        return ids.Count;
    }

    [Fact]
    public async Task First_run_lands_everything_and_watermark_persists_only_after_sinks_commit()
    {
        const string ds = "s1";
        var (bounds, guard) = Exclusive(ds, $"{ds}_out");
        var (dag, sourceId, pipelineId, sinkId) = BuildDag(ds, rows: 10, bounds, guard);
        var key = WatermarkStore.Key("mem", ds);

        var (duck, ctx) = await NewRunAsync("run-1");
        RunResult result;
        await using (duck)
        {
            result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, result.Status);

            // No stored watermark -> nothing pushed to the source, NULL substituted into the pipeline guard:
            // every seeded row (ids 0..9) flows source -> pipeline -> sink.
            Assert.Equal(10, NodeOf(result.Nodes, sourceId).RowsMoved);
            Assert.Equal(10, NodeOf(result.Nodes, pipelineId).RowsMoved);
            Assert.Equal(10, NodeOf(result.Nodes, sinkId).RowsMoved);

            // The candidate is captured on the SourceLoad result, but NOT yet persisted: advancement (the
            // commit gate) has not run, so the store is still empty even though the run has finished.
            Assert.NotNull(NodeOf(result.Nodes, sourceId).WatermarkCandidate);
            Assert.Null(_store.Get(key));

            WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        }

        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("9", stored!.Value); // MAX(id) over ids 0..9
        Assert.Equal("id", stored.Cursor);
    }

    [Fact]
    public async Task Second_run_extracts_only_the_delta()
    {
        const string ds = "s2";
        _store.Set(WatermarkStore.Key("mem", ds), Wm("9")); // as if a prior run committed ids 0..9
        var (bounds, guard) = Exclusive(ds, $"{ds}_out");
        var (dag, sourceId, pipelineId, sinkId) = BuildDag(ds, rows: 15, bounds, guard);

        var (duck, ctx) = await NewRunAsync("run-2");
        await using (duck)
        {
            var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, result.Status);

            // The SQL bound evaluated to the stored value 9 (exclusive) and was stamped on the DatasetSpec;
            // the InMemory source honored it (id <= 9 skipped), so ONLY the 5-row delta (ids 10..14) is
            // extracted -- never the full 15. This is the SQL route's equivalent of proving the connector
            // received DatasetSpec.WatermarkValue.
            Assert.Equal(5, NodeOf(result.Nodes, sourceId).RowsMoved);
            Assert.Equal(5, NodeOf(result.Nodes, pipelineId).RowsMoved); // guard id > 9 agrees with the pushdown
            Assert.Equal(5, NodeOf(result.Nodes, sinkId).RowsMoved);

            WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        }

        Assert.Equal("14", _store.Get(WatermarkStore.Key("mem", ds))!.Value);
    }

    [Fact]
    public async Task Lookback_relands_the_overlap_and_the_merge_sink_dedupes()
    {
        const string ds = "s3";
        var s = Sentinel(ds);
        var outputName = $"{ds}_synced";

        // Run 1: an ordinary exclusive first run establishes ids 0..9 in a MERGE sink (keys=[id]) and a
        // stored watermark of 9.
        var (b1, g1) = Exclusive(ds, $"{ds}_out");
        var (dag1, src1, _, _) = BuildDag(ds, rows: 10, b1, g1, sinkMode: "merge", keys: ["id"]);
        var (duck1, ctx1) = await NewRunAsync("run-1");
        await using (duck1)
        {
            var r1 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx1).ExecuteAsync(dag1, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, r1.Status);
            Assert.Equal(10, NodeOf(r1.Nodes, src1).RowsMoved);
            WatermarkAdvancement.Advance(dag1, r1.Nodes, _store);
        }

        Assert.Equal("9", _store.Get(WatermarkStore.Key("mem", ds))!.Value);
        Assert.Equal(10, CommittedRows(outputName));

        // Run 2: a lookback bound `id >= wm - 2` (inclusive). The InMemory connector does NOT advertise
        // InclusiveWatermarkBound, so the executor drops the inclusive pushdown (with a notice) and the
        // source extracts unbounded -- the pipeline guard `id >= 7` is what actually cuts the slice,
        // re-landing the 7,8,9 overlap alongside the 10..14 delta (8 rows). The merge sink dedupes on id.
        var lookbackBounds = new[] { new SqlWatermarkBound($"{ds}_out", Inclusive: true, $"'{s}' - 2", s) };
        var lookbackGuard = $"('{s}' is null or id >= '{s}' - 2)";
        var (dag2, src2, pipe2, _) = BuildDag(ds, rows: 15, lookbackBounds, lookbackGuard, sinkMode: "merge", keys: ["id"]);
        var notices = new List<string>();
        var (duck2, ctx2) = await NewRunAsync("run-2", notices: notices);
        await using (duck2)
        {
            var r2 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx2).ExecuteAsync(dag2, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, r2.Status);

            Assert.Contains(notices, n => n.Contains("cannot honor an inclusive watermark bound", StringComparison.Ordinal));
            Assert.Equal(15, NodeOf(r2.Nodes, src2).RowsMoved);  // inclusive dropped -> source unbounded
            Assert.Equal(8, NodeOf(r2.Nodes, pipe2).RowsMoved);  // guard id >= 7 -> ids 7..14, overlap re-landed
            WatermarkAdvancement.Advance(dag2, r2.Nodes, _store);
        }

        // The load-bearing assertion: the merge deduped the re-landed overlap -- exactly 15 DISTINCT ids
        // (0..14), never 18 (10 + 8 with 7,8,9 double-counted).
        Assert.Equal(15, CommittedRows(outputName));
        Assert.Equal(15, CommittedDistinctIds(outputName));
        Assert.Equal("14", _store.Get(WatermarkStore.Key("mem", ds))!.Value);
    }

    [Fact]
    public async Task Full_refresh_lands_everything_while_capture_and_advancement_still_run()
    {
        const string ds = "s4";
        var key = WatermarkStore.Key("mem", ds);
        // A stale stored watermark that WOULD filter every row (ids 0..14) if honored on read -- proving
        // --full-refresh truly ignores it rather than coincidentally matching.
        _store.Set(key, Wm("99"));
        var (bounds, guard) = Exclusive(ds, $"{ds}_out");
        var (dag, sourceId, pipelineId, sinkId) = BuildDag(ds, rows: 15, bounds, guard);

        var (duck, ctx) = await NewRunAsync("run-fr", fullRefresh: true);
        await using (duck)
        {
            var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, result.Status);

            // Read side skipped on both the source (no bound pushed) and the pipeline (NULL substituted):
            // the full 15 rows are re-extracted, not just what's newer than the stale 99.
            Assert.Equal(15, NodeOf(result.Nodes, sourceId).RowsMoved);
            Assert.Equal(15, NodeOf(result.Nodes, pipelineId).RowsMoved);
            Assert.Equal(15, NodeOf(result.Nodes, sinkId).RowsMoved);

            // Capture + advancement STILL run under --full-refresh: the candidate is captured
            // and the watermark is re-established from the full extract's own max.
            Assert.NotNull(NodeOf(result.Nodes, sourceId).WatermarkCandidate);
            WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        }

        Assert.Equal("14", _store.Get(key)!.Value); // re-established at 14, not left at the stale 99
    }

    [Fact]
    public async Task Failed_sink_does_not_advance_the_watermark()
    {
        const string ds = "s5";
        var key = WatermarkStore.Key("mem", ds);
        _store.Set(key, Wm("9")); // an established watermark that must survive the failed run untouched
        var (bounds, guard) = Exclusive(ds, $"{ds}_out");
        var (dag, sourceId, pipelineId, sinkId) = BuildDag(ds, rows: 15, bounds, guard,
            sinkOptions: new Dictionary<string, object?> { ["fail_commit"] = true });

        var (duck, ctx) = await NewRunAsync("run-fail");
        await using (duck)
        {
            var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);
            Assert.Equal(RunStatus.CompletedWithFailures, result.Status);

            // Extraction + transform genuinely succeeded (the delta was staged and a candidate captured) --
            // only the sink's commit failed, a post-extract/pre-commit fault.
            Assert.Equal(NodeStatus.Success, NodeOf(result.Nodes, sourceId).Status);
            Assert.Equal(5, NodeOf(result.Nodes, sourceId).RowsMoved);
            Assert.Equal(NodeStatus.Success, NodeOf(result.Nodes, pipelineId).Status);
            Assert.NotNull(NodeOf(result.Nodes, sourceId).WatermarkCandidate);
            Assert.Equal(NodeStatus.Failed, NodeOf(result.Nodes, sinkId).Status);

            WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        }

        // The commit gate held: the previously stored watermark stands, neither advanced to 14 nor regressed.
        Assert.Equal("9", _store.Get(key)!.Value);
    }
}
