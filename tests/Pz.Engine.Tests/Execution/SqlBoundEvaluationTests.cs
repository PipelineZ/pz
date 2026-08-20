using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Incremental;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>SourceLoadExecutor evaluates a SQL-declared dataset's recorded bounds against
/// the stored watermark inside DuckDB, reduces them to one (value, inclusive) union bound, and stamps it
/// on the DatasetSpec handed to the connector — gated by the connector's InclusiveWatermarkBound
/// capability when the union is inclusive. Driven through the real executor + a real in-memory DuckDB
/// session, with a fake source connector that records the exact DatasetSpec it received.</summary>
public sealed class SqlBoundEvaluationTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sqlbound-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private WatermarkStore _store = null!;

    private const string Cursor = "ts";
    private const string StoredValue = "2026-07-15T08:00:00.000000";
    private const string Sentinel = "__pz_watermark__mem__events__";

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

    private static SqlWatermarkBound Bound(bool inclusive, string valueExpr) =>
        new("p", inclusive, valueExpr, Sentinel);

    /// <summary>A ceiling — how `max_window` and `until` are spelled in SQL.</summary>
    private static SqlWatermarkBound UpperBound(bool inclusive, string valueExpr) =>
        new("p", inclusive, valueExpr, Sentinel, IsUpper: true);

    /// <summary>The bare sentinel expression: <c>watermark('mem','events')</c> rewritten to its quoted
    /// sentinel, exactly what WatermarkInference records in <see cref="SqlWatermarkBound.ValueExprSql"/>.</summary>
    private static string SentinelExpr => $"'{Sentinel}'";

    /// <summary>Runs a DeclaredInSql SourceLoad through the executor and returns the DatasetSpec the fake
    /// connector observed. <paramref name="stored"/> seeds the watermark store when non-null; a distinct
    /// <paramref name="dataset"/> name per call keeps staging tables from colliding on the shared session.</summary>
    private async Task<(DatasetSpec Received, IReadOnlyList<string> Notices)> RunAsync(
        string dataset, IReadOnlyList<SqlWatermarkBound> bounds, ConnectorCapabilities capabilities,
        string? stored, bool fullRefresh = false, bool declareColumns = true)
    {
        if (stored is not null)
        {
            _store.Set(WatermarkStore.Key("mem", dataset), new Watermark(Cursor, "timestamp", stored, "prior-run"));
        }

        var incremental = new IncrementalDef(Cursor, DeclaredInSql: true, SqlBounds: bounds);
        IReadOnlyDictionary<string, string>? columns = declareColumns
            ? new Dictionary<string, string> { [Cursor] = "timestamp" }
            : null;
        var datasetDef = new DatasetDef(dataset, new Dictionary<string, object?>(), columns,
            new SyncModeDef(SyncMode.Incremental, incremental));
        var sourceDef = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(), [datasetDef], "sources/mem.yml");
        var node = new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, $"src_mem__{dataset}",
            [], null, new SourceDatasetDef(sourceDef, datasetDef));

        var source = new RecordingSource(capabilities);
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", source);

        var notices = new List<string>();
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh, Notice: notices.Add);

        var result = await new SourceLoadExecutor().ExecuteAsync(node, ctx, default);
        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(source.Received);
        return (source.Received!, notices);
    }

    [Fact]
    public async Task A_sql_declared_cursor_needs_no_columns_contract_to_evaluate_its_bounds()
    {
        // `columns:` is optional for a SQL-declared cursor, so bound evaluation must NOT read the
        // cursor's type out of the contract: such a project compiles and runs clean on its FIRST run
        // (no stored watermark, bounds skipped) and only fails on the second. The stored watermark
        // carries its own type; use it.
        var (spec, _) = await RunAsync("events_nocontract", [Bound(inclusive: false, SentinelExpr)],
            ConnectorCapabilities.None, StoredValue, declareColumns: false);

        Assert.Equal(Cursor, spec.WatermarkCursor);
        Assert.Equal(StoredValue, spec.WatermarkValue);
    }

    [Fact]
    public async Task A_lookback_expression_evaluates_without_a_columns_contract_too()
    {
        // The type drives DuckDB's cast/strftime probe, so a fallback that typed it wrongly would
        // surface here as a mis-canonicalized value rather than a crash.
        var (spec, _) = await RunAsync("events_nocontract2",
            [Bound(inclusive: false, $"{SentinelExpr} - interval 2 hour")],
            ConnectorCapabilities.None, StoredValue, declareColumns: false);

        Assert.Equal("2026-07-15T06:00:00.000000", spec.WatermarkValue);
    }

    [Fact]
    public async Task Single_gt_bound_stamps_stored_watermark_exclusive()
    {
        var (spec, _) = await RunAsync("events1", [Bound(inclusive: false, SentinelExpr)],
            ConnectorCapabilities.None, StoredValue);

        Assert.Equal(Cursor, spec.WatermarkCursor);
        Assert.Equal(StoredValue, spec.WatermarkValue);
        Assert.False(spec.WatermarkLowerInclusive);
    }

    [Fact]
    public async Task Lookback_bound_is_evaluated_in_duckdb()
    {
        var (spec, _) = await RunAsync("events2",
            [Bound(inclusive: false, $"{SentinelExpr} - interval 2 hour")], ConnectorCapabilities.None, StoredValue);

        Assert.Equal("2026-07-15T06:00:00.000000", spec.WatermarkValue);
        Assert.False(spec.WatermarkLowerInclusive);
    }

    [Fact]
    public async Task Two_bounds_union_takes_lowest_and_ors_inclusivity()
    {
        // `> wm` (exclusive at 08:00) and `>= wm - 2h` (inclusive at 06:00): the union is the lowest
        // value (06:00), and inclusive because the winning bound is inclusive.
        var (spec, _) = await RunAsync("events3",
            [Bound(inclusive: false, SentinelExpr), Bound(inclusive: true, $"{SentinelExpr} - interval 2 hour")],
            ConnectorCapabilities.InclusiveWatermarkBound, StoredValue);

        Assert.Equal("2026-07-15T06:00:00.000000", spec.WatermarkValue);
        Assert.True(spec.WatermarkLowerInclusive);
    }

    [Fact]
    public async Task Inclusive_union_without_capability_pushes_nothing_and_notices()
    {
        var (spec, notices) = await RunAsync("events4",
            [Bound(inclusive: false, SentinelExpr), Bound(inclusive: true, $"{SentinelExpr} - interval 2 hour")],
            ConnectorCapabilities.None, StoredValue);

        Assert.Null(spec.WatermarkCursor);
        Assert.Null(spec.WatermarkValue);
        Assert.Contains(notices, n => n.Contains("cannot honor an inclusive watermark bound", StringComparison.Ordinal));
    }

    [Fact]
    public async Task No_stored_watermark_or_full_refresh_stamps_no_bound()
    {
        // No stored watermark at all: nothing to evaluate the bound against, so no bound is pushed.
        var (noStored, _) = await RunAsync("events5a", [Bound(inclusive: false, SentinelExpr)],
            ConnectorCapabilities.None, stored: null);
        Assert.Null(noStored.WatermarkCursor);
        Assert.Null(noStored.WatermarkValue);

        // A stored watermark exists but the run is --full-refresh: the read side is skipped, so again no
        // bound is pushed (extraction is unbounded).
        var (fullRefresh, _) = await RunAsync("events5b", [Bound(inclusive: false, SentinelExpr)],
            ConnectorCapabilities.None, StoredValue, fullRefresh: true);
        Assert.Null(fullRefresh.WatermarkCursor);
        Assert.Null(fullRefresh.WatermarkValue);
    }

    // -- The first run, and ceilings --------------------------------------------------------------

    [Fact]
    public async Task An_initial_floor_is_pushed_on_the_very_first_run()
    {
        // No stored watermark. Before this, bound evaluation returned early and the whole table was read,
        // with the floor applied only by the pipeline filter in DuckDB.
        var (spec, _) = await RunAsync("events_initial",
            [Bound(inclusive: false, $"coalesce({SentinelExpr}, TIMESTAMP '2026-01-01')")],
            ConnectorCapabilities.None, stored: null);

        Assert.Equal(Cursor, spec.WatermarkCursor);
        Assert.Equal("2026-01-01T00:00:00.000000", spec.WatermarkValue);
    }

    [Fact]
    public async Task A_bare_watermark_on_a_first_run_pushes_no_bound_and_is_not_an_error()
    {
        var (spec, _) = await RunAsync("events_bare", [Bound(inclusive: false, SentinelExpr)],
            ConnectorCapabilities.None, stored: null);

        Assert.Null(spec.WatermarkValue);
    }

    [Fact]
    public async Task A_ceiling_is_stamped_on_the_spec_as_an_upper_bound()
    {
        var (spec, _) = await RunAsync("events_ceiling",
            [
                Bound(inclusive: false, SentinelExpr),
                UpperBound(inclusive: true, $"{SentinelExpr} + interval 7 day"),
            ],
            ConnectorCapabilities.BoundedWindow, StoredValue);

        Assert.Equal(StoredValue, spec.WatermarkValue);
        Assert.Equal("2026-07-22T08:00:00.000000", spec.WatermarkUpperBound);
    }

    [Fact]
    public async Task Two_ceilings_reduce_to_the_tightest()
    {
        // The OPPOSITE of the floor rule. A floor may safely be too low -- the pipeline filter cuts. A
        // ceiling may not be too high: advancement is MAX(cursor) over STAGING, which the pipeline's WHERE
        // never filters, so rows past the intended window would advance the watermark past themselves.
        var (spec, _) = await RunAsync("events_two_ceilings",
            [
                Bound(inclusive: false, SentinelExpr),
                UpperBound(inclusive: true, $"{SentinelExpr} + interval 7 day"),
                UpperBound(inclusive: true, "TIMESTAMP '2026-07-17T00:00:00'"),
            ],
            ConnectorCapabilities.BoundedWindow, StoredValue);

        Assert.Equal("2026-07-17T00:00:00.000000", spec.WatermarkUpperBound);
    }

    [Fact]
    public async Task A_ceiling_needs_no_columns_contract_on_a_first_run()
    {
        // The type comes from `select typeof(...)` when there is neither a contract nor a stored
        // watermark to read one off -- this is what keeps columns: optional for the whole trio.
        var (spec, _) = await RunAsync("events_typed_probe",
            [
                Bound(inclusive: false, $"coalesce({SentinelExpr}, TIMESTAMP '2026-01-01')"),
                UpperBound(inclusive: true, $"coalesce({SentinelExpr}, TIMESTAMP '2026-01-01') + interval 7 day"),
            ],
            ConnectorCapabilities.BoundedWindow, stored: null, declareColumns: false);

        Assert.Equal("2026-01-01T00:00:00.000000", spec.WatermarkValue);
        Assert.Equal("2026-01-08T00:00:00.000000", spec.WatermarkUpperBound);
    }

    [Fact]
    public async Task Full_refresh_still_pushes_no_bounds_at_all()
    {
        var (spec, _) = await RunAsync("events_full_refresh",
            [
                Bound(inclusive: false, $"coalesce({SentinelExpr}, TIMESTAMP '2026-01-01')"),
                UpperBound(inclusive: true, $"{SentinelExpr} + interval 7 day"),
            ],
            ConnectorCapabilities.BoundedWindow, StoredValue, fullRefresh: true);

        Assert.Null(spec.WatermarkValue);
        Assert.Null(spec.WatermarkUpperBound);
    }

    /// <summary>Universal-path source that records the exact <see cref="DatasetSpec"/> the executor hands
    /// it and lands an empty staging table with the cursor column present (so post-extract watermark
    /// capture runs cleanly). Capabilities are configurable to exercise the inclusive-bound gate.</summary>
    private sealed class RecordingSource(ConnectorCapabilities capabilities) : ISourceConnector, ISource
    {
        public DatasetSpec? Received { get; private set; }

        public ConnectorInfo Info => new("recording", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => capabilities;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        private static readonly Schema CursorSchema =
            new([new Field(Cursor, new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true)], null);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
        {
            Received = spec;
            return new(new DatasetSchema(CursorSchema));
        }

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = null;
            return false;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
        {
            Received = spec;
            return new(System.Array.Empty<IDatasetPartition>());
        }

        public ValueTask DisposeAsync() => default;
    }
}
