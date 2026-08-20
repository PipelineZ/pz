using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="IntegerInferenceLint"/> unit
/// tests. Fixture mirrors <see cref="SchemaDriftGateTests"/> (temp dir, <see cref="DuckSession.Open"/>,
/// hand-built staging table) — the lint is called directly with the staged table both SourceLoad
/// native-scan epilogues hand it. The lint fires <see cref="IRunEvents.LossyIntegerInferenceDetected"/>
/// when a DOUBLE column's non-null values are all finite integers and at least one exceeds 2^53 —
/// the shape DuckDB's csv/json auto-detect produces from a &gt;int64 integer column, silently losing
/// digits. Genuinely fractional, small-integral, non-finite, or contract-governed columns stay quiet.</summary>
public sealed class IntegerInferenceLintTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-integer-inference-lint-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var paths = new RunPaths(_dir, "current");
        Directory.CreateDirectory(paths.RunDir);
        _duck = DuckSession.Open(paths.StagingDbPath);
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static SourceDatasetDef Def(IReadOnlyDictionary<string, string>? columns = null)
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), columns);
        var source = new ConnectionDef("crm", "inmemory", new Dictionary<string, object?>(), [dataset],
            "sources/crm.yml");
        return new SourceDatasetDef(source, dataset);
    }

    private static DagNode Node(SourceDatasetDef def) =>
        new(new NodeId("1111111111111111"), NodeKind.SourceLoad, $"src_{def.Source.Name}__{def.Dataset.Name}", [], null, def);

    private RunContext Ctx(IRunEvents events) =>
        new(_duck, new ConnectorRegistry(), new RunPaths(_dir, "current"), events);

    private Task CreateStagingTableAsync(string tableName, string selectList, bool zeroRows = false) =>
        _duck.ExecuteAsync($"create or replace table {tableName} as select {selectList}" +
            (zeroRows ? " where false" : ""));

    /// <summary>Records every <see cref="IRunEvents.LossyIntegerInferenceDetected"/> call — every other
    /// member is a no-op, mirroring <c>SchemaDriftGateTests.RecordingEvents</c>'s shape.</summary>
    private sealed class RecordingEvents : IRunEvents
    {
        public readonly List<(DagNode Node, string Connection, string Entity, IReadOnlyList<string> Columns)> Lints = [];

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }

        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) =>
            Lints.Add((node, connection, entity, columns));

        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }

        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }

    private async Task<IReadOnlyList<(string Connection, string Entity, IReadOnlyList<string> Columns)>> RunLintAsync(
        string selectList, IReadOnlyDictionary<string, string>? columns = null, bool zeroRows = false)
    {
        var def = Def(columns);
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, selectList, zeroRows);

        var events = new RecordingEvents();
        await IntegerInferenceLint.ApplyAsync(node, def, tableName, Ctx(events), CancellationToken.None);
        return [.. events.Lints.Select(l => (l.Connection, l.Entity, l.Columns))];
    }

    [Fact]
    public async Task Double_column_of_integers_beyond_exact_precision_fires_one_event_naming_it()
    {
        // The fuzz finding's exact shape: 12345678901234567890 auto-detected as DOUBLE.
        var lints = await RunLintAsync("12345678901234567890::double as id, 'a'::varchar as name");

        var lint = Assert.Single(lints);
        Assert.Equal("crm", lint.Connection);
        Assert.Equal("orders", lint.Entity);
        Assert.Equal(["id"], lint.Columns);
    }

    [Fact]
    public async Task Offending_columns_are_collected_into_one_event_in_schema_order()
    {
        var lints = await RunLintAsync(
            "unnest([12345678901234567890::double, 98765432109876543210::double]) as a, " +
            "unnest([1.5::double, 2.5::double]) as b, " +
            "unnest([98765432109876543210::double, 12345678901234567890::double]) as c");

        var lint = Assert.Single(lints);
        Assert.Equal(["a", "c"], lint.Columns);
    }

    [Fact]
    public async Task Fractional_doubles_stay_quiet()
    {
        Assert.Empty(await RunLintAsync("unnest([19.99::double, 12345678901234567890::double]) as price"));
    }

    [Fact]
    public async Task Integral_doubles_beyond_hugeint_range_stay_quiet()
    {
        // 1e300 is integral as a double, but no declarable integer type could hold it — this is a
        // scientific-notation float column, not a corrupted business key, so the nudge would mislead.
        Assert.Empty(await RunLintAsync("1e300::double as x"));
    }

    [Fact]
    public async Task Integral_doubles_within_exact_precision_stay_quiet()
    {
        // 2^53 itself is still exactly representable; only beyond it do gaps appear.
        Assert.Empty(await RunLintAsync("unnest([9007199254740992::double, 42::double]) as id"));
    }

    [Fact]
    public async Task Non_finite_values_stay_quiet()
    {
        Assert.Empty(await RunLintAsync("unnest(['inf'::double, 'nan'::double, 1e300::double]) as x"));
    }

    [Fact]
    public async Task All_null_double_column_stays_quiet()
    {
        Assert.Empty(await RunLintAsync("null::double as id"));
    }

    [Fact]
    public async Task Zero_row_table_stays_quiet()
    {
        Assert.Empty(await RunLintAsync("12345678901234567890::double as id", zeroRows: true));
    }

    [Fact]
    public async Task Big_integers_in_a_bigint_column_stay_quiet()
    {
        Assert.Empty(await RunLintAsync("9223372036854775807::bigint as id"));
    }

    [Fact]
    public async Task Declared_contract_skips_the_lint_entirely()
    {
        Assert.Empty(await RunLintAsync("12345678901234567890::double as id",
            columns: new Dictionary<string, string> { ["id"] = "double" }));
    }
}
