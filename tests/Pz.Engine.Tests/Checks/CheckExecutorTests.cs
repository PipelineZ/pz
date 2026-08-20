using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Checks;
using Pz.Engine.Execution;
using Pz.Engine.Tests.Resilience;

namespace Pz.Engine.Tests.Checks;

/// <summary>Real-DuckSession coverage for <see cref="CheckExecutor"/>: every test seeds a staging
/// table by SQL insert (mirroring how <c>PipelineExecutor</c> would have already materialized it),
/// then runs the check node directly against it.</summary>
public sealed class CheckExecutorTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-check-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private RunContext _ctx = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _ctx = new RunContext(_duck, new ConnectorRegistry(), new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode CheckNode(string pipelineName, CheckDef check, string name = "check_x") =>
        new(new NodeId("cccccccccccccccc"), NodeKind.Check, name, [], null, new CheckNodeDef(pipelineName, check));

    private RunContext TimeCtx(ManualTimeProvider time) => new(_duck, new ConnectorRegistry(),
        new RunPaths(_dir, "test-run"), NullRunEvents.Instance, Time: time);

    private static ManualTimeProvider ClockAt(DateTime utc)
    {
        var time = new ManualTimeProvider();
        time.Advance(utc - DateTime.UnixEpoch);
        return time;
    }

    private static CheckDef Freshness(string column, string maxAge) =>
        new("freshness", [column], new Dictionary<string, object?> { ["max_age"] = maxAge });

    private static CheckDef AcceptedValues(string column, List<object?> values) =>
        new("accepted_values", [column], new Dictionary<string, object?> { ["values"] = values });

    private static CheckDef CustomSql(string sql) =>
        new("custom_sql", [], new Dictionary<string, object?> { ["name"] = "t", ["sql"] = sql });

    [Fact]
    public async Task Freshness_passes_when_max_is_within_max_age()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, updated_at timestamp)");
        await _duck.ExecuteAsync("insert into staging.p values " +
            "(1, timestamp '2026-07-22 08:00:00'), (2, timestamp '2026-07-20 03:00:00')");
        var ctx = TimeCtx(ClockAt(new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc)));

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("updated_at", "24h")), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, result.RowsMoved);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task Freshness_fails_when_max_is_older_than_max_age()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, updated_at timestamp)");
        await _duck.ExecuteAsync("insert into staging.p values (1, timestamp '2026-07-20 09:14:00')");
        var ctx = TimeCtx(ClockAt(new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc)));

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("updated_at", "24h")), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(1, result.RowsMoved);
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("max(updated_at)=", result.Error.Message);
        Assert.Contains("2026-07-20", result.Error.Message);
        Assert.Contains("max_age=24h", result.Error.Message);
    }

    /// <summary>An empty table fails as stale — no rows is no
    /// evidence of recent data, exactly the silent-upstream-death scenario freshness exists for.</summary>
    [Fact]
    public async Task Freshness_fails_on_empty_table()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, updated_at timestamp)");
        var ctx = TimeCtx(ClockAt(new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc)));

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("updated_at", "24h")), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(1, result.RowsMoved);
        Assert.Contains("null (no rows or all-null column)", result.Error!.Message);
    }

    /// <summary>A non-empty table whose cursor column is entirely NULL
    /// also has max() = NULL, so the failure text must not claim "no rows" — it must cover both
    /// causes.</summary>
    [Fact]
    public async Task Freshness_fails_on_all_null_column()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, updated_at timestamp)");
        await _duck.ExecuteAsync("insert into staging.p values (1, null), (2, null)");
        var ctx = TimeCtx(ClockAt(new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc)));

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("updated_at", "24h")), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(1, result.RowsMoved);
        Assert.Contains("null (no rows or all-null column)", result.Error!.Message);
    }

    [Fact]
    public async Task Freshness_works_on_date_columns()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, load_date date)");
        await _duck.ExecuteAsync("insert into staging.p values (1, date '2026-07-20')");
        var ctx = TimeCtx(ClockAt(new DateTime(2026, 7, 22, 12, 0, 0, DateTimeKind.Utc)));

        var fresh = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("load_date", "7d")), ctx, default);
        var stale = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", Freshness("load_date", "24h")), ctx, default);

        Assert.Equal(NodeStatus.Success, fresh.Status);
        Assert.Equal(NodeStatus.Failed, stale.Status);
    }

    [Fact]
    public async Task Not_null_passes_on_clean_table()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, email varchar)");
        await _duck.ExecuteAsync("insert into staging.p values (1, 'a@x.com'), (2, 'b@x.com')");

        var check = new CheckDef("not_null", ["id", "email"], new Dictionary<string, object?>());
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, result.RowsMoved);
        Assert.Null(result.Error);
    }

    [Fact]
    public async Task Not_null_fails_with_count_and_samples()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, email varchar)");
        await _duck.ExecuteAsync(
            "insert into staging.p values (1, 'a@x.com'), (2, null), (3, 'c@x.com'), (4, null)");

        var check = new CheckDef("not_null", ["email"], new Dictionary<string, object?>());
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(2, result.RowsMoved);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("2 violation(s)", result.Error.Message);
        // one of the two offending rows' id must show up in the sample.
        Assert.True(
            result.Error.Message.Contains("id=2", StringComparison.Ordinal) ||
            result.Error.Message.Contains("id=4", StringComparison.Ordinal),
            $"expected a sample offending row in: {result.Error.Message}");
        Assert.Contains("email=null", result.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unique_fails_on_duplicate_keys()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (1), (2), (3), (3), (3)");

        var check = new CheckDef("unique", ["id"], new Dictionary<string, object?>());
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        // two duplicated key groups: id=1 (count 2), id=3 (count 3).
        Assert.Equal(2, result.RowsMoved);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("2 violation(s)", result.Error.Message);
    }

    [Theory]
    [InlineData("min", 1L, NodeStatus.Success)]
    [InlineData("min", 10L, NodeStatus.Failed)]
    [InlineData("max", 2L, NodeStatus.Failed)]
    public async Task Row_count_respects_min_and_max(string boundKey, long boundValue, NodeStatus expected)
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2), (3)");

        var check = new CheckDef("row_count", [], new Dictionary<string, object?> { [boundKey] = boundValue });
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(expected, result.Status);
        if (expected == NodeStatus.Failed)
        {
            Assert.Equal(1, result.RowsMoved);
            Assert.NotNull(result.Error);
            Assert.Equal("PZ0510", result.Error!.Code);
            Assert.Contains("1 violation(s)", result.Error.Message);
        }
        else
        {
            Assert.Equal(0, result.RowsMoved);
            Assert.Null(result.Error);
        }
    }

    [Fact]
    public async Task Custom_sql_passes_on_zero_rows()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2), (3)");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", CustomSql("select * from staging.p where id < 0")), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, result.RowsMoved);
    }

    /// <summary>Violations = the query's row count; sample = its first rows. A trailing semicolon
    /// must not break the subquery wrapping.</summary>
    [Fact]
    public async Task Custom_sql_fails_with_row_count_and_sample()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2), (3)");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", CustomSql("select id from staging.p where id > 1;")), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(2, result.RowsMoved);
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("2 violation(s)", result.Error.Message);
        Assert.Contains("id=2", result.Error.Message);
    }

    [Fact]
    public async Task Custom_sql_suppresses_sample_when_SampleValues_false()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2)");

        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_x", [], null,
            new CheckNodeDef("p", CustomSql("select id from staging.p where id > 1"), SampleValues: false));
        var result = await new CheckExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Contains("1 violation(s) (samples disabled)", result.Error!.Message);
        Assert.DoesNotContain("id=2", result.Error.Message);
    }

    [Fact]
    public async Task Unknown_check_type_is_clean_PZ0510()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1)");

        var check = new CheckDef("bogus", [], new Dictionary<string, object?>());
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("unknown check type 'bogus'", result.Error.Message);
        Assert.Equal("not_null | unique | row_count | freshness | accepted_values | custom_sql",
            result.Error.Hint);
    }

    /// <summary>CheckNodeDef.SampleValues=false suppresses the sample
    /// query entirely for not_null -- the violation count still shows, but no row data (not even the
    /// column name/value pairs SampleAsync would have formatted) reaches the message.</summary>
    [Fact]
    public async Task Not_null_suppresses_sample_when_SampleValues_false()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer, email varchar)");
        await _duck.ExecuteAsync(
            "insert into staging.p values (1, 'a@x.com'), (2, null), (3, 'c@x.com'), (4, null)");

        var check = new CheckDef("not_null", ["email"], new Dictionary<string, object?>());
        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_x", [], null,
            new CheckNodeDef("p", check, SampleValues: false));
        var result = await new CheckExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(2, result.RowsMoved);
        Assert.NotNull(result.Error);
        Assert.Contains("2 violation(s) (samples disabled)", result.Error!.Message);
        Assert.DoesNotContain("email=null", result.Error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("id=", result.Error.Message, StringComparison.Ordinal);
    }

    /// <summary>Mirrors the not_null suppression test above for unique -- same opt-out, same executor
    /// path.</summary>
    [Fact]
    public async Task Unique_suppresses_sample_when_SampleValues_false()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (1), (2), (3), (3), (3)");

        var check = new CheckDef("unique", ["id"], new Dictionary<string, object?>());
        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_x", [], null,
            new CheckNodeDef("p", check, SampleValues: false));
        var result = await new CheckExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Contains("2 violation(s) (samples disabled)", result.Error!.Message);
    }

    /// <summary>row_count is unaffected by SampleValues: its "sample" is a bound
    /// description (`row_count=N`), never per-row data, so it is shown regardless of the opt-out.</summary>
    [Fact]
    public async Task Row_count_ignores_SampleValues_false()
    {
        await _duck.ExecuteAsync("create table staging.p (id integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2), (3)");

        var check = new CheckDef("row_count", [], new Dictionary<string, object?> { ["min"] = 10L });
        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_x", [], null,
            new CheckNodeDef("p", check, SampleValues: false));
        var result = await new CheckExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Contains("row_count=3", result.Error!.Message);
        Assert.DoesNotContain("(samples disabled)", result.Error.Message);
    }

    [Fact]
    public async Task Check_sql_quotes_identifiers()
    {
        await _duck.ExecuteAsync("create table staging.p (\"select\" integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2)");

        var check = new CheckDef("not_null", ["select"], new Dictionary<string, object?>());
        var result = await new CheckExecutor().ExecuteAsync(CheckNode("p", check), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
    }

    [Fact]
    public async Task Accepted_values_passes_when_all_values_in_list()
    {
        await _duck.ExecuteAsync("create table staging.p (status varchar)");
        await _duck.ExecuteAsync("insert into staging.p values ('pending'), ('shipped'), ('pending')");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", AcceptedValues("status", ["pending", "shipped"])), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, result.RowsMoved);
    }

    [Fact]
    public async Task Accepted_values_fails_with_distinct_offending_values()
    {
        await _duck.ExecuteAsync("create table staging.p (status varchar)");
        await _duck.ExecuteAsync("insert into staging.p values " +
            "('pending'), ('canceled'), ('canceled'), ('refunded'), ('shipped')");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", AcceptedValues("status", ["pending", "shipped"])), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(3, result.RowsMoved); // 3 offending ROWS
        Assert.Equal("PZ0510", result.Error!.Code);
        Assert.Contains("3 violation(s)", result.Error.Message);
        // sample = DISTINCT offending VALUES, not whole rows
        Assert.Contains("status=canceled", result.Error.Message);
        Assert.Contains("status=refunded", result.Error.Message);
    }

    /// <summary>NULLs pass — null-checking is not_null's job.</summary>
    [Fact]
    public async Task Accepted_values_passes_nulls()
    {
        await _duck.ExecuteAsync("create table staging.p (status varchar)");
        await _duck.ExecuteAsync("insert into staging.p values ('pending'), (null), (null)");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", AcceptedValues("status", ["pending"])), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
    }

    [Fact]
    public async Task Accepted_values_handles_integer_lists()
    {
        await _duck.ExecuteAsync("create table staging.p (code integer)");
        await _duck.ExecuteAsync("insert into staging.p values (1), (2), (9)");

        var result = await new CheckExecutor().ExecuteAsync(
            CheckNode("p", AcceptedValues("code", [1L, 2L])), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(1, result.RowsMoved);
        Assert.Contains("code=9", result.Error!.Message);
    }

    [Fact]
    public async Task Accepted_values_suppresses_sample_when_SampleValues_false()
    {
        await _duck.ExecuteAsync("create table staging.p (status varchar)");
        await _duck.ExecuteAsync("insert into staging.p values ('bad'), ('pending')");

        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.Check, "check_x", [], null,
            new CheckNodeDef("p", AcceptedValues("status", ["pending"]), SampleValues: false));
        var result = await new CheckExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Contains("1 violation(s) (samples disabled)", result.Error!.Message);
        Assert.DoesNotContain("status=bad", result.Error.Message);
    }
}
