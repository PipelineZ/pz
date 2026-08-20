using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="AmbiguousDateLint"/> unit
/// tests, same fixture shape as <see cref="IntegerInferenceLintTests"/> but staged from REAL csv
/// files through DuckDB's own <c>read_csv</c>/<c>sniff_csv</c>, because the lint's whole subject is
/// what the sniffer picked. A csv whose date column has only day-and-month-&le;12 values is parsed
/// with an assumed day/month order (empirically <c>%d/%m/%Y</c>) — a month-first source is misread
/// on every row. The lint probes the sniffed format via the scan's sniff fragment and warns per
/// DATE/TIMESTAMP column when the format is a day/month-ambiguous family AND no staged value's day
/// exceeds 12 (i.e. no row ever disambiguated the order). A file with one day-&gt;12 value forced
/// the sniffer's hand; an ISO file's format is not ambiguous — both stay quiet.</summary>
public sealed class AmbiguousDateLintTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-ambiguous-date-lint-tests", Guid.NewGuid().ToString("N"));
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

    private RunContext Ctx(IRunEvents events, Action<string>? notice = null) =>
        new(_duck, new ConnectorRegistry(), new RunPaths(_dir, "current"), events, Notice: notice);

    /// <summary>Records every <see cref="IRunEvents.AmbiguousDateInferenceDetected"/> call — every
    /// other member is a no-op, mirroring <c>IntegerInferenceLintTests.RecordingEvents</c>.</summary>
    private sealed class RecordingEvents : IRunEvents
    {
        public readonly List<(string Connection, string Entity, IReadOnlyList<string> Columns, string Format)> Lints = [];

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
            IReadOnlyList<string> columns) { }

        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) =>
            Lints.Add((connection, entity, columns, format));

        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }

    private async Task<string> WriteCsvAsync(string name, string content)
    {
        var path = Path.Combine(_dir, name);
        await File.WriteAllTextAsync(path, content);
        return path;
    }

    private async Task<RecordingEvents> RunLintAsync(string csvPath, Action<string>? notice = null,
        string? sniffFragmentOverride = null)
    {
        var def = Def();
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        var escaped = csvPath.Replace("'", "''");
        await _duck.ExecuteAsync(
            $"create or replace table {tableName} as select * from read_csv('{escaped}', header = true, auto_detect = true)");

        var events = new RecordingEvents();
        var sniffFragment = sniffFragmentOverride ?? $"sniff_csv('{escaped}')";
        await AmbiguousDateLint.ApplyAsync(node, def, tableName, sniffFragment, Ctx(events, notice),
            CancellationToken.None);
        return events;
    }

    [Fact]
    public async Task All_ambiguous_date_column_fires_one_event_naming_column_and_format()
    {
        // The fuzz finding's exact shape: every value has day AND month <= 12, so nothing ever
        // disambiguated the sniffer's day-first assumption — a US-format file is misread on every row.
        var path = await WriteCsvAsync("amb.csv", "when,amount\n01/02/2024,10\n03/04/2024,20\n");

        var events = await RunLintAsync(path);

        var lint = Assert.Single(events.Lints);
        Assert.Equal("crm", lint.Connection);
        Assert.Equal("orders", lint.Entity);
        Assert.Equal(["when"], lint.Columns);
        Assert.StartsWith("%d/%m", lint.Format, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_disambiguating_row_forces_the_order_and_stays_quiet()
    {
        // 13 cannot be a month: the sniffer's %d/%m pick was forced by the data, not assumed.
        var path = await WriteCsvAsync("forced.csv", "when,amount\n13/02/2024,10\n03/04/2024,20\n");

        Assert.Empty((await RunLintAsync(path)).Lints);
    }

    [Fact]
    public async Task Iso_dates_are_not_ambiguous_and_stay_quiet()
    {
        var path = await WriteCsvAsync("iso.csv", "when,amount\n2024-02-01,10\n2024-04-03,20\n");

        Assert.Empty((await RunLintAsync(path)).Lints);
    }

    [Fact]
    public async Task Ambiguous_timestamps_fire_too()
    {
        var path = await WriteCsvAsync("ts.csv", "at,amount\n01/02/2024 10:30:00,10\n03/04/2024 11:00:00,20\n");

        var lint = Assert.Single((await RunLintAsync(path)).Lints);
        Assert.Equal(["at"], lint.Columns);
    }

    [Fact]
    public async Task File_without_date_columns_stays_quiet()
    {
        var path = await WriteCsvAsync("nodates.csv", "id,amount\n1,10\n2,20\n");

        Assert.Empty((await RunLintAsync(path)).Lints);
    }

    [Fact]
    public async Task Declared_contract_skips_the_lint_entirely()
    {
        var path = await WriteCsvAsync("amb2.csv", "when,amount\n01/02/2024,10\n");
        var def = Def(new Dictionary<string, string> { ["when"] = "date", ["amount"] = "bigint" });
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        var escaped = path.Replace("'", "''");
        await _duck.ExecuteAsync(
            $"create or replace table {tableName} as select * from read_csv('{escaped}', header = true, auto_detect = true)");

        var events = new RecordingEvents();
        await AmbiguousDateLint.ApplyAsync(node, def, tableName, $"sniff_csv('{escaped}')", Ctx(events),
            CancellationToken.None);

        Assert.Empty(events.Lints);
    }

    [Fact]
    public async Task A_failing_probe_never_throws_and_reports_via_notice()
    {
        var path = await WriteCsvAsync("amb3.csv", "when,amount\n01/02/2024,10\n");
        var notices = new List<string>();

        var events = await RunLintAsync(path, notices.Add,
            sniffFragmentOverride: "sniff_csv('/nonexistent/definitely-missing.csv')");

        Assert.Empty(events.Lints);
        var notice = Assert.Single(notices);
        Assert.Contains("date-format probe", notice, StringComparison.Ordinal);
    }
}
