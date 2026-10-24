using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>A SQL-declared incremental read on the native tier whose pipeline neither selects the cursor
/// nor leaves its predicate behind: compiled by <see cref="DagCompiler"/> with the real
/// <see cref="DuckDbSqlAstReader"/> (so the read hints are the compiler's own), executed through
/// <see cref="RunOrchestrator"/> against a fresh DuckDB per run, and advanced by
/// <see cref="WatermarkAdvancement"/> between runs.</summary>
public sealed class NativeHintedWatermarkFlowTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-native-hinted-wm-tests", Guid.NewGuid().ToString("N"));
    private WatermarkStore _store = null!;

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
        return Task.CompletedTask;
    }

    private static CompiledDag Compile()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null);
        var files = new ConnectionDef("files", "stub", new Dictionary<string, object?>(), [dataset], "connections.yml");
        var pipeline = new PipelineDef("orders_out",
            "select id, c1 from {{ source('files', 'orders') }} " +
            "where k = 7 and updated_at > {{ watermark('files', 'orders') }}",
            "table", [], [], "pipelines/orders_out.sql");
        var project = new PzProject("t", "0.0.0", new EngineConfig(),
            new Dictionary<string, object?>(), [], [files], [pipeline]);
        var render = new RenderContext(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        using var sqlAst = new DuckDbSqlAstReader();
        return DagCompiler.Compile(project, render, sqlAst: sqlAst);
    }

    private async Task<(RunResult Result, long StagedColumns)> RunAsync(CompiledDag dag, string runId, long rows)
    {
        var source = dag.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new WatermarkHonouringNativeSource(rows));
        var plan = new ExecutionPlan(
            [new PlannedNode(source.Id, source.Kind, source.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new EngineConfig()));
        await using var duck = DuckSession.Open(Path.Combine(_dir, $"{runId}.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        var ctx = new RunContext(duck, registry, new RunPaths(_dir, runId), NullRunEvents.Instance, plan,
            Watermarks: _store);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);
        var staged = await duck.ScalarAsync<long>(
            $"select count(*) from pragma_table_info('{StagingNames.ForSourceLoad("files", "orders")}')");
        return (result, staged);
    }

    [Fact]
    public async Task Unselected_cursor_with_a_pushed_predicate_still_advances_the_watermark()
    {
        var dag = Compile();
        var sourceNode = dag.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        var hints = Assert.IsType<SourceDatasetDef>(sourceNode.Definition).Hints;
        Assert.NotNull(hints);
        Assert.Contains("updated_at", hints!.Columns!, StringComparer.OrdinalIgnoreCase);
        Assert.False(string.IsNullOrWhiteSpace(hints.PredicateSql));
        var key = WatermarkStore.Key("files", "orders");

        // Run 1: rows 0..19, k = id % 10, so the pushed predicate stages ids 7 and 17.
        var (first, firstColumns) = await RunAsync(dag, "run-1", rows: 20);
        Assert.Equal(RunStatus.Success, first.Status);
        var firstSource = first.Nodes.Single(n => n.Id == sourceNode.Id);
        Assert.Equal(2, firstSource.RowsMoved);
        Assert.Equal(hints.Columns!.Count, firstColumns); // c2 is never staged
        Assert.Equal("17", firstSource.WatermarkCandidate?.Value);
        Assert.Empty(WatermarkAdvancement.Advance(dag, first.Nodes, _store));
        Assert.Equal("17", _store.Get(key)?.Value);

        // Run 2: rows 0..29; the source reads past the stored 17, the predicate keeps id 27.
        var (second, _) = await RunAsync(dag, "run-2", rows: 30);
        Assert.Equal(RunStatus.Success, second.Status);
        var secondSource = second.Nodes.Single(n => n.Id == sourceNode.Id);
        Assert.Equal(1, secondSource.RowsMoved);
        Assert.Equal("27", secondSource.WatermarkCandidate?.Value);
        Assert.Empty(WatermarkAdvancement.Advance(dag, second.Nodes, _store));
        Assert.Equal("27", _store.Get(key)?.Value);
    }

    /// <summary>Native-only source over <c>range(rows)</c> with columns id, k, c1, c2, updated_at (= id) that
    /// applies <see cref="DatasetSpec.WatermarkValue"/> inside its fragment, as a database source would.</summary>
    private sealed class WatermarkHonouringNativeSource(long rows) : ISourceConnector, ISource
    {
        public ConnectorInfo Info => new("stub-native", "0.1.0", ProtocolVersion.Major);
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
            var where = spec.WatermarkValue is { } low ? $" where range > {long.Parse(low, System.Globalization.CultureInfo.InvariantCulture)}" : "";
            scan = new NativeScan(
                $"(select range as id, range % 10 as k, 'x' || range as c1, 'y' as c2, range as updated_at " +
                $"from range({rows}){where}) t", [])
            { Mechanism = "stub_scan" };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }
}
