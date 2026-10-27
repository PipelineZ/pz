using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Diagnostics.Events;
using Pz.DuckDb;
using Pz.Engine.Checks;
using Pz.Engine.Dispatch;
using Pz.Engine.Events;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary><c>--log-sql</c>: Pipeline nodes and <c>custom_sql</c> checks publish the statement they hand DuckDB,
/// once per attempt, only when <see cref="RunContext.LogSql"/> is set. Driven through the real executors and a
/// real DuckDB session.</summary>
public sealed class NodeSqlEventTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-nodesql-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private WatermarkStore _store = null!;
    private readonly SqlRecorder _events = new();

    private const string Sentinel = "__pz_watermark__src_a__x__";

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync(
            "create table staging.src_a__x as select * from (values " +
            "(TIMESTAMP '2026-07-15T06:00:00.000000'), (TIMESTAMP '2026-07-15T10:00:00.000000')) as t(ts)");
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static string GuardedSql => $"select * from staging.src_a__x where ('{Sentinel}' is null or ts > '{Sentinel}')";

    private static DagNode PipelineNode(string sql, string materialization = "table", bool watermark = true)
    {
        var def = new PipelineDef("p", sql, materialization, [], [], "pipelines/p.sql");
        return new DagNode(NodeId.Compute("p"), NodeKind.Pipeline, "p", [], sql, def)
        {
            WatermarkSubstitutions = watermark
                ? [new WatermarkSubstitution(Sentinel, "src_a", "x", "timestamp")]
                : [],
        };
    }

    private static DagNode CheckNode(CheckDef check) =>
        new(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_p", [], null, new CheckNodeDef("p", check));

    private RunContext Context(bool logSql = true, bool fullRefresh = false, IDuckSession? duck = null) =>
        new(duck ?? _duck, new ConnectorRegistry(), new RunPaths(_dir, "test-run"), _events,
            Watermarks: _store, FullRefresh: fullRefresh)
        {
            LogSql = logSql,
        };

    [Fact]
    public async Task Nothing_is_published_without_the_flag()
    {
        await new PipelineExecutor().ExecuteAsync(PipelineNode(GuardedSql), Context(logSql: false), default);

        Assert.Empty(_events.Calls);
    }

    [Fact]
    public async Task A_pipeline_publishes_its_statement_with_the_watermark_literal()
    {
        _store.Set(WatermarkStore.Key("src_a", "x"),
            new Watermark("ts", "timestamp", "2026-07-15T08:00:00.000000", "prior-run"));

        var result = await new PipelineExecutor().ExecuteAsync(PipelineNode(GuardedSql), Context(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var call = Assert.Single(_events.Calls);
        Assert.Equal(1, call.Attempt);
        Assert.StartsWith("CREATE OR REPLACE TABLE staging.p AS (", call.Sql, StringComparison.Ordinal);
        Assert.Contains("TIMESTAMP '2026-07-15T08:00:00.000000'", call.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain(Sentinel, call.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_full_refresh_run_publishes_null_in_place_of_the_watermark()
    {
        _store.Set(WatermarkStore.Key("src_a", "x"),
            new Watermark("ts", "timestamp", "2026-07-15T08:00:00.000000", "prior-run"));

        await new PipelineExecutor().ExecuteAsync(PipelineNode(GuardedSql), Context(fullRefresh: true), default);

        var call = Assert.Single(_events.Calls);
        Assert.Contains("(NULL is null or ts > NULL)", call.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain(Sentinel, call.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_view_materialized_pipeline_publishes_create_or_replace_view()
    {
        await new PipelineExecutor().ExecuteAsync(
            PipelineNode("select 1 as a", materialization: "view", watermark: false), Context(), default);

        var call = Assert.Single(_events.Calls);
        Assert.Equal("CREATE OR REPLACE VIEW staging.p AS (select 1 as a)", call.Sql);
    }

    [Fact]
    public async Task The_published_statement_is_the_one_executed()
    {
        var counting = new CountingDuckSession(_duck);

        await new PipelineExecutor().ExecuteAsync(
            PipelineNode("select 1 as a", watermark: false), Context(duck: counting), default);

        Assert.Equal(_events.Calls.Single().Sql, counting.Statements[0]);
    }

    [Fact]
    public async Task A_custom_sql_check_publishes_its_count_query()
    {
        await _duck.ExecuteAsync("create table staging.p as select 1 as id");
        var check = new CheckDef("custom_sql", [],
            new Dictionary<string, object?> { ["name"] = "t", ["sql"] = "select * from staging.p where id < 0;" });

        var result = await new CheckExecutor().ExecuteAsync(CheckNode(check), Context(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var call = Assert.Single(_events.Calls);
        Assert.Equal("select count(*) from (select * from staging.p where id < 0) q", call.Sql);
        Assert.Equal(NodeKind.Check, call.Node.Kind);
    }

    [Fact]
    public async Task A_failing_custom_sql_check_publishes_only_the_count_query()
    {
        await _duck.ExecuteAsync("create table staging.p as select 1 as id");
        var check = new CheckDef("custom_sql", [],
            new Dictionary<string, object?> { ["name"] = "t", ["sql"] = "select * from staging.p" });

        var result = await new CheckExecutor().ExecuteAsync(CheckNode(check), Context(), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Single(_events.Calls);
    }

    [Fact]
    public async Task A_built_in_check_publishes_nothing()
    {
        await _duck.ExecuteAsync("create table staging.p as select 1 as id");
        var check = new CheckDef("not_null", ["id"], new Dictionary<string, object?>());

        await new CheckExecutor().ExecuteAsync(CheckNode(check), Context(), default);

        Assert.Empty(_events.Calls);
    }

    [Fact]
    public async Task A_retried_pipeline_publishes_one_event_per_attempt()
    {
        var flaky = new FailFirstExecuteSession(_duck);
        var executor = new KindDispatchingExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero),
            delay: (_, _) => Task.CompletedTask);

        var result = await executor.ExecuteAsync(
            PipelineNode("select 1 as a", watermark: false), Context(duck: flaky), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal([1, 2], _events.Calls.Select(c => c.Attempt));
    }

    [Fact]
    public async Task The_publisher_caps_the_statement_and_reports_its_full_length()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var sql = new string('a', NodeSqlEvent.MaxChars + 10);

        publisher.NodeSql(PipelineNode("select 1", watermark: false), 2, sql);
        bus.Complete();

        var all = new List<RunEvent>();
        await foreach (var evt in bus.ReadAllAsync()) all.Add(evt);
        var e = Assert.IsType<NodeSqlEvent>(Assert.Single(all));
        Assert.Equal(NodeSqlEvent.MaxChars, e.Sql.Length);
        Assert.True(e.Truncated);
        Assert.Equal(NodeSqlEvent.MaxChars + 10, e.Chars);
        Assert.Equal("pipeline", e.Kind);
        Assert.Equal(2, e.Attempt);
        Assert.Equal("p", e.Name);
    }

    [Fact]
    public void A_short_statement_is_not_cut()
    {
        var text = NodeSqlEvent.Cap("select 1", out var truncated);

        Assert.Equal("select 1", text);
        Assert.False(truncated);
    }

    [Fact]
    public void The_cap_never_splits_a_surrogate_pair()
    {
        // A pair straddling the cap: its high half is the last character the cap would keep.
        var sql = new string('a', NodeSqlEvent.MaxChars - 1) + "\U0001F600" + "tail";

        var text = NodeSqlEvent.Cap(sql, out var truncated);

        Assert.True(truncated);
        Assert.Equal(NodeSqlEvent.MaxChars - 1, text.Length);
        Assert.False(char.IsHighSurrogate(text[^1]));
    }

    [Fact]
    public void A_throwing_target_does_not_escape_the_publish_helper()
    {
        var ctx = new RunContext(_duck, new ConnectorRegistry(), new RunPaths(_dir, "test-run"), new ThrowingEvents())
        {
            LogSql = true,
        };

        ctx.PublishNodeSql(PipelineNode("select 1", watermark: false), "select 1");
    }

    private sealed record SqlCall(DagNode Node, int Attempt, string Sql);

    private sealed class SqlRecorder : IRunEvents
    {
        public List<SqlCall> Calls { get; } = [];

        public void NodeSql(DagNode node, int attempt, string sql) => Calls.Add(new SqlCall(node, attempt, sql));

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed,
            string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }
        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped,
            TimeSpan duration) { }
        public void ConnectorLog(string connection, string level, string message) { }
    }

    private sealed class ThrowingEvents : IRunEvents
    {
        public void NodeSql(DagNode node, int attempt, string sql) => throw new InvalidOperationException("boom");

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed,
            string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }
        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped,
            TimeSpan duration) { }
        public void ConnectorLog(string connection, string level, string message) { }
    }

    /// <summary>Fails the first <c>CREATE OR REPLACE</c> with a transient connector error, so the attempt loop
    /// retries the pipeline node once.</summary>
    private sealed class FailFirstExecuteSession(IDuckSession inner) : IDuckSession
    {
        private bool _failed;

        public Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            if (!_failed && sql.StartsWith("CREATE OR REPLACE", StringComparison.Ordinal))
            {
                _failed = true;
                throw new PzConnectorException("transient", isTransient: true);
            }

            return inner.ExecuteAsync(sql, ct);
        }

        public Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default) => inner.ScalarAsync<T>(sql, ct);

        public Task<long> IngestArrowAsync(string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches,
            CancellationToken ct = default) => inner.IngestArrowAsync(targetTable, schema, batches, ct);

        public IAsyncEnumerable<RecordBatch> QueryArrowAsync(string sql, int targetBatchBytes = 32 * 1024 * 1024,
            CancellationToken ct = default) => inner.QueryArrowAsync(sql, targetBatchBytes, ct);

        public Task<Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default) =>
            inner.GetResultSchemaAsync(sql, ct);

        public Task CreateEmptyTableAsync(string targetTable, Schema schema, CancellationToken ct = default) =>
            inner.CreateEmptyTableAsync(targetTable, schema, ct);

        public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, Schema? schema = null,
            CancellationToken ct = default) => inner.AppendArrowBatchAsync(targetTable, batch, schema, ct);

        public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default) =>
            inner.ExecuteTransactionAsync(statements, ct);

        public ValueTask DisposeAsync() => default;
    }
}
