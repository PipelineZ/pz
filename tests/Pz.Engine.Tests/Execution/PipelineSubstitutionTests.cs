using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>PipelineExecutor rewrites each <see cref="WatermarkSubstitution"/>'s quoted
/// sentinel out of a pipeline's rendered SQL before executing it -- a typed literal from the stored
/// watermark when one exists (and this isn't a full-refresh run), else <c>NULL</c>, which makes the
/// compiler's NULL-guard arm <c>(&lt;expr&gt; IS NULL OR ...)</c> true and passes every row. Driven
/// through the real executor + a real in-memory DuckDB session, mirroring the harness
/// <c>SqlBoundEvaluationTests</c> uses.</summary>
public sealed class PipelineSubstitutionTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-pipelinesub-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private WatermarkStore _store = null!;

    private const string SourceName = "src_a";
    private const string Dataset = "x";
    private const string CursorType = "timestamp";
    private const string Sentinel = "__pz_watermark__src_a__x__";
    private const string StoredValue = "2026-07-15T08:00:00.000000";

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync(
            "create table staging.src_a__x as select * from (values " +
            "(TIMESTAMP '2026-07-15T06:00:00.000000'), " +
            "(TIMESTAMP '2026-07-15T08:00:00.000000'), " +
            "(TIMESTAMP '2026-07-15T10:00:00.000000')) as t(ts)");
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>The NULL-guarded form the compiler produces for a SQL-declared watermark bound: passes
    /// every row when the sentinel resolves to NULL (unbounded), else filters to rows past the bound.</summary>
    private static string GuardedSql => $"select * from staging.src_a__x where ('{Sentinel}' is null or ts > '{Sentinel}')";

    private DagNode BuildNode(string sql, IReadOnlyList<WatermarkSubstitution> subs)
    {
        var def = new PipelineDef("p", sql, "table", [], [], "pipelines/p.sql");
        return new DagNode(NodeId.Compute("p"), NodeKind.Pipeline, "p", [], sql, def)
        {
            WatermarkSubstitutions = subs,
        };
    }

    private RunContext BuildContext(bool fullRefresh = false) =>
        new(_duck, new ConnectorRegistry(), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh);

    private Task<long> RowCountAsync() => _duck.ScalarAsync<long>("select count(*) from staging.p", default);

    private Task<string> JoinedRowsAsync() =>
        _duck.ScalarAsync<string>(
            "select string_agg(ts::varchar, ',' order by ts) from staging.p", default);

    [Fact]
    public async Task Stored_watermark_present_filters_rows_past_the_bound()
    {
        _store.Set(WatermarkStore.Key(SourceName, Dataset), new Watermark("ts", CursorType, StoredValue, "prior-run"));
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1L, await RowCountAsync());
        Assert.Equal("2026-07-15 10:00:00", await JoinedRowsAsync());
    }

    [Fact]
    public async Task No_stored_watermark_substitutes_null_and_passes_all_rows()
    {
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await RowCountAsync());
    }

    [Fact]
    public async Task Full_refresh_ignores_stored_watermark_and_passes_all_rows()
    {
        _store.Set(WatermarkStore.Key(SourceName, Dataset), new Watermark("ts", CursorType, StoredValue, "prior-run"));
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(fullRefresh: true), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await RowCountAsync());
    }

    [Fact]
    public async Task Undeclared_cursor_type_takes_the_type_from_the_stored_watermark()
    {
        // A dataset with no columns: contract carries a null CursorType.
        // The stored watermark's own TypeName types the literal, so the bound still filters.
        _store.Set(WatermarkStore.Key(SourceName, Dataset), new Watermark("ts", CursorType, StoredValue, "prior-run"));
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType: null)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1L, await RowCountAsync());
        Assert.Equal("2026-07-15 10:00:00", await JoinedRowsAsync());
    }

    [Fact]
    public async Task Undeclared_cursor_type_with_no_stored_watermark_passes_all_rows()
    {
        // First run: the literal is NULL, the compiler's guard arms, and no type is needed at all --
        // which is why the declared type was never an input.
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType: null)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await RowCountAsync());
    }

    [Fact]
    public async Task Undeclared_cursor_type_cannot_drift_so_no_mismatch_error_is_raised()
    {
        // A stored 'date' against a declared 'timestamp' fails
        // Cursor_type_mismatch_fails_the_node_with_a_pz_coded_error below. With nothing declared there
        // is nothing to drift FROM, so the run proceeds and the store's own type governs the literal.
        _store.Set(WatermarkStore.Key(SourceName, Dataset), new Watermark("ts", "date", "2026-07-15", "prior-run"));
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType: null)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        // DATE '2026-07-15' is midnight, so every staged row (06:00, 08:00, 10:00) is past the bound.
        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await RowCountAsync());
    }

    [Fact]
    public async Task Cursor_type_mismatch_fails_the_node_with_a_pz_coded_error()
    {
        _store.Set(WatermarkStore.Key(SourceName, Dataset), new Watermark("ts", "date", StoredValue, "prior-run"));
        var node = BuildNode(GuardedSql, [new WatermarkSubstitution(Sentinel, SourceName, Dataset, CursorType)]);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.UnsupportedCursorType, result.Error!.Code);
        Assert.Contains("date", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains(CursorType, result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("--full-refresh", result.Error.Hint, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Empty_substitutions_leaves_sql_byte_untouched()
    {
        const string plainSql = "select * from staging.src_a__x";
        var node = BuildNode(plainSql, []);

        var result = await new PipelineExecutor().ExecuteAsync(node, BuildContext(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await RowCountAsync());
    }
}
