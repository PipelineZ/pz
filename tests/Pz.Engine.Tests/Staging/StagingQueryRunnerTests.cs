using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Preview;
using Pz.Engine.Staging;

namespace Pz.Engine.Tests.Staging;

/// <summary>One read-only SELECT against a finished run's staging: it reads the run's tables, in the
/// query's own order and formatted as a preview formats cells, and nothing else. Every way the run is
/// missing is PZ0543; every way the SQL cannot run is PZ0545, never a raw DuckDB exception.</summary>
public sealed class StagingQueryRunnerTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "pz-staging-query-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>A run dir whose staging holds <c>staging.orders</c> (ids 0-9, amounts, a null status on
    /// id 3) and <c>staging.customers</c>.</summary>
    private async Task<string> RunDir()
    {
        var dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        await using var duck = DuckSession.Open(Path.Combine(dir, "staging.duckdb"));
        await duck.ExecuteAsync("create schema staging");
        await duck.ExecuteAsync(
            "create table staging.orders as select range as id, range % 3 as customer_id, range * 1.5 as amount, " +
            "case when range = 3 then null else 'open' end as status from range(10)");
        await duck.ExecuteAsync("create table staging.customers as select * from (values (0, 'ann'), (1, 'bob'), (2, 'cy')) t(id, name)");
        return dir;
    }

    private static Task<StagingQueryResult> Query(string dir, string sql, int limit = 50) =>
        StagingQueryRunner.RunAsync(dir, "r1", sql, limit, default);

    private static async Task<PzError> FailsWith(Task task)
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(() => task);
        return Assert.Single(ex.Errors);
    }

    [Fact]
    public async Task A_join_over_the_runs_tables_returns_rows_in_the_querys_order()
    {
        var result = await Query(await RunDir(),
            "select c.name, count(*) as orders, sum(o.amount) as total from staging.orders o " +
            "join staging.customers c on c.id = o.customer_id group by c.name order by c.name");

        Assert.Equal("r1", result.RunId);
        Assert.Equal(["name", "orders", "total"], result.Columns.Select(c => c.Name));
        Assert.Equal(["ann", "bob", "cy"], result.Rows.Select(r => r[0].Text));
        Assert.Equal(PreviewCellKind.Number, result.Rows[0][1].Kind);
        Assert.Equal("4", result.Rows[0][1].Text);
        Assert.False(result.Truncated);
        Assert.All(result.Columns, c => Assert.False(c.Redacted));
    }

    [Fact]
    public async Task More_rows_than_the_limit_are_cut_and_flagged()
    {
        var result = await Query(await RunDir(), "select id from staging.orders order by id desc", limit: 3);

        Assert.Equal(["9", "8", "7"], result.Rows.Select(r => r[0].Text));
        Assert.True(result.Truncated);
        Assert.Equal(3, result.Limit);
    }

    [Fact]
    public async Task Exactly_the_limit_is_not_truncated()
    {
        var result = await Query(await RunDir(), "select id from staging.orders", limit: 10);

        Assert.Equal(10, result.Rows.Count);
        Assert.False(result.Truncated);
    }

    [Theory]
    [InlineData("select count(*) from staging.orders;")]
    [InlineData("  select count(*) from staging.orders ;; \n")]
    [InlineData("select count(*) from staging.orders -- trailing comment")]
    [InlineData("with o as (select * from staging.orders) select count(*) from o")]
    public async Task A_terminated_commented_or_with_query_runs(string sql)
    {
        var result = await Query(await RunDir(), sql);
        Assert.Equal("10", Assert.Single(result.Rows)[0].Text);
    }

    [Fact]
    public async Task Nulls_and_duplicate_output_names_come_through()
    {
        var result = await Query(await RunDir(), "select o.id, c.id, o.status from staging.orders o join staging.customers c on c.id = o.customer_id where o.id = 3");

        Assert.Equal(["id", "id_1", "status"], result.Columns.Select(c => c.Name));
        Assert.Equal(PreviewCellKind.Null, Assert.Single(result.Rows)[2].Kind);
    }

    [Theory]
    [InlineData("delete from staging.orders")]
    [InlineData("create table staging.x as select 1")]
    [InlineData("select 1; drop table staging.orders")]
    [InlineData("pragma database_list")]
    [InlineData("select * from staging.nope")]
    [InlineData("select * from read_text('/etc/hostname')")]
    [InlineData("select * from read_csv('http://127.0.0.1:9/x.csv')")]
    public async Task Anything_but_a_read_of_the_runs_tables_is_PZ0545(string sql)
    {
        var dir = await RunDir();

        var error = await FailsWith(Query(dir, sql));

        Assert.Equal(PzErrorCode.StagingQueryFailed, error.Code);
        Assert.DoesNotContain('\n', error.Message);
        if (!sql.StartsWith("select", StringComparison.Ordinal))
        {
            Assert.Contains("not a single SELECT", error.Message, StringComparison.Ordinal);
        }
        Assert.Equal("10", Assert.Single((await Query(dir, "select count(*) from staging.orders")).Rows)[0].Text);
    }

    [Fact]
    public async Task A_run_in_progress_is_refused_PZ0543()
    {
        var dir = await RunDir();
        using var running = RunDirLock.Acquire(dir);

        var error = await FailsWith(Query(dir, "select 1"));

        Assert.Equal(PzErrorCode.StagingQueryUnavailable, error.Code);
        Assert.Contains("still in progress", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_missing_run_or_cleaned_staging_is_PZ0543()
    {
        Assert.Contains("does not exist",
            (await FailsWith(Query(Path.Combine(_root, "nope"), "select 1"))).Message, StringComparison.Ordinal);

        var cleaned = Path.Combine(_root, "cleaned");
        Directory.CreateDirectory(cleaned);
        Assert.Equal(PzErrorCode.StagingQueryUnavailable, (await FailsWith(Query(cleaned, "select 1"))).Code);
    }

    [Fact]
    public async Task The_run_dir_lock_is_released_after_the_query()
    {
        var dir = await RunDir();
        await Query(dir, "select 1");

        using var after = RunDirLock.TryAcquire(dir);
        Assert.NotNull(after);
    }
}
