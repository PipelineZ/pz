using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>Real DuckDB, temp dir per test (the "once_t"/"a_t"/... tables are ordinary CREATE TABLE
/// statements, so a real session is the only way to prove the not-REPEATABLE failure mode the
/// ledger exists to paper over).</summary>
public sealed class NativeSetupLedgerTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-native-setup-ledger-tests", Guid.NewGuid().ToString("N"));
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

    [Fact]
    public async Task A_statement_runs_once_per_run()
    {
        const string statement = "create table once_t(x int)";

        // Baseline, without a ledger: setup statements are idempotent by CONTRACT, but a bare CREATE
        // TABLE (no IF NOT EXISTS) is not REPEATABLE -- calling NativeSetup.ExecuteSetupAsync twice
        // re-issues it verbatim and DuckDB refuses the duplicate.
        await NativeSetup.ExecuteSetupAsync(_duck, statement, CancellationToken.None);
        var direct = await Assert.ThrowsAsync<PzConnectorException>(
            () => NativeSetup.ExecuteSetupAsync(_duck, statement, CancellationToken.None));
        Assert.Contains("PZ0311", direct.Message, StringComparison.Ordinal);
        await _duck.ExecuteAsync("drop table once_t");

        // Through one ledger: the same statement text, called twice, succeeds both times and the
        // table is created exactly once (a second raw CREATE TABLE would have failed above).
        var ledger = new NativeSetupLedger();
        await ledger.ExecuteOnceAsync(_duck, statement, CancellationToken.None);
        await ledger.ExecuteOnceAsync(_duck, statement, CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'once_t'"));
    }

    [Fact]
    public async Task Distinct_statement_texts_each_run()
    {
        var ledger = new NativeSetupLedger();
        await ledger.ExecuteOnceAsync(_duck, "create table a_t(x int)", CancellationToken.None);
        await ledger.ExecuteOnceAsync(_duck, "create table b_t(x int)", CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name = 'a_t'"));
        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name = 'b_t'"));
    }

    [Fact]
    public async Task A_failed_statement_is_forgotten_so_a_retry_reissues_it()
    {
        const string statement = "insert into later_t values (1)";
        var ledger = new NativeSetupLedger();

        var failure = await Assert.ThrowsAsync<PzConnectorException>(
            () => ledger.ExecuteOnceAsync(_duck, statement, CancellationToken.None));
        Assert.Contains("PZ0311", failure.Message, StringComparison.Ordinal);

        await _duck.ExecuteAsync("create table later_t(x int)");

        // Same ledger, same statement text: the failed attempt above must have been forgotten, not
        // memoized as "already ran" -- a node retry re-issues it.
        await ledger.ExecuteOnceAsync(_duck, statement, CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from later_t"));
    }

    [Fact]
    public async Task Concurrent_callers_share_one_execution()
    {
        var gate = new TaskCompletionSource();
        var blocking = new GatedFirstCallDuckSession(_duck, gate.Task);
        var ledger = new NativeSetupLedger();
        const string statement = "create table concurrent_t(x int)";

        var first = ledger.ExecuteOnceAsync(blocking, statement, CancellationToken.None);
        var second = ledger.ExecuteOnceAsync(blocking, statement, CancellationToken.None);

        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        gate.SetResult();
        await first;
        await second;

        Assert.Equal(1, blocking.ExecuteCallCount);
        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'concurrent_t'"));
    }

    /// <summary>Proves the wiring, not just the ledger in isolation: two nodes of the same run
    /// (a SourceLoad and a SinkWrite, both native-only) reach <see cref="NativeSetup.ExecuteSetupAsync"/>
    /// through the same <see cref="RunContext.SetupLedger"/> rather than each running its own copy of
    /// the loop that used to live in the executors.</summary>
    [Fact]
    public async Task Executors_reach_setup_through_the_ledger()
    {
        const string statement = "create table ledger_reach_marker(x int)";
        await _duck.ExecuteAsync("create table staging.p5 as select 1 as x");

        var sourceNode = SourceLoadNode("src5", "t5");
        var sinkNode = SinkWriteNode("p5");

        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new SharedSetupSource(statement));
        registry.AddSink("stub", new SharedSetupSink(statement, Path.Combine(_dir, "out5.tmp"), Path.Combine(_dir, "out5.csv")));

        var plan = new ExecutionPlan(
            [
                new PlannedNode(sourceNode.Id, sourceNode.Kind, sourceNode.Name, EdgeStrategy.NativeScan, 1, "test"),
                new PlannedNode(sinkNode.Id, sinkNode.Kind, sinkNode.Name, EdgeStrategy.NativeCopy, 1, "test"),
            ],
            MemoryBudget.Compute(new EngineConfig()));

        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var sourceResult = await new KindDispatchingExecutor().ExecuteAsync(sourceNode, ctx, CancellationToken.None);
        var sinkResult = await new KindDispatchingExecutor().ExecuteAsync(sinkNode, ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, sourceResult.Status);
        Assert.Equal(NodeStatus.Success, sinkResult.Status);
        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'ledger_reach_marker'"));
    }

    private static DagNode SourceLoadNode(string sourceName, string datasetName)
    {
        var source = new ConnectionDef(sourceName, "stub", new Dictionary<string, object?>(),
            [new DatasetDef(datasetName, new Dictionary<string, object?>(), null)], $"sources/{sourceName}.yml");
        return new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SourceLoad, $"src_{sourceName}__{datasetName}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static DagNode SinkWriteNode(string input)
    {
        var sink = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml")
        { Outputs = [new OutputDef("out", input, "replace", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("dddddddddddddddd"), NodeKind.SinkWrite, "stub.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    /// <summary>Wraps a real session, blocking the FIRST <see cref="ExecuteAsync"/> call on
    /// <paramref name="gate"/> until the test releases it -- proves two concurrent
    /// <see cref="NativeSetupLedger.ExecuteOnceAsync"/> callers for the same statement text share the
    /// one in-flight DuckDB execution rather than racing it.</summary>
    private sealed class GatedFirstCallDuckSession(IDuckSession inner, Task gate) : IDuckSession
    {
        private int callCount;

        public int ExecuteCallCount => callCount;

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            if (Interlocked.Increment(ref callCount) == 1)
            {
                await gate.ConfigureAwait(false);
            }

            await inner.ExecuteAsync(sql, ct).ConfigureAwait(false);
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

        public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, CancellationToken ct = default) =>
            inner.AppendArrowBatchAsync(targetTable, batch, ct);

        public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default) =>
            inner.ExecuteTransactionAsync(statements, ct);

        public ValueTask DisposeAsync() => default; // the real session is owned/disposed by the test fixture
    }

    /// <summary>Source connector whose native-scan setup statement is fixed by the test. The fragment
    /// is a real value expression (not the planner-only stubs' unexecuted placeholder text in
    /// Planning/PlannerStubs.cs), so the executor's `create or replace table ... as select * from
    /// &lt;fragment&gt;` actually runs.</summary>
    private sealed class SharedSetupSource(string setupStatement) : ISourceConnector, ISource
    {
        public ConnectorInfo Info => new("stub-shared-setup", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = new NativeScan("(values (1)) t(x)", [setupStatement]) { Mechanism = "stub_scan" };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Sink connector whose native-copy setup statement is fixed by the test. CopySql and
    /// Finalizations are real (unlike the planner-only stubs), so the executor's COPY actually runs.</summary>
    private sealed class SharedSetupSink(string setupStatement, string tempPath, string finalPath) : ISinkConnector, ISink
    {
        public ConnectorInfo Info => new("stub-shared-setup-sink", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeCopy | ConnectorCapabilities.ReplaceWrites;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = new NativeCopy($"copy (select 1) to '{tempPath}'", [setupStatement])
            {
                Mechanism = "stub_copy",
                Finalizations = [new FileMove(tempPath, finalPath)],
            };
            return true;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }
}
