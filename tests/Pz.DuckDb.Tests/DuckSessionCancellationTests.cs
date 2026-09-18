namespace Pz.DuckDb.Tests;

/// <summary>Cancelling a token must stop a statement that is already running inside DuckDB, not only
/// one still waiting for the connection: a node timeout or Ctrl-C that has to wait for a runaway query
/// to finish on its own is no bound at all. The query here never finishes by itself, so each test can
/// only end if the statement really was interrupted; the outer wait turns a regression into a failure
/// instead of a hang. When the cancel lands does not matter to the outcome.</summary>
public sealed class DuckSessionCancellationTests
{
    private const string Endless = "select count(*) from range(10000000000000) t where t.range % 7 = 3";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task A_running_statement_is_interrupted_by_cancellation()
    {
        await using var duck = DuckSession.Open(":memory:");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        var running = duck.ExecuteAsync($"create table t as {Endless}", cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Bound));
    }

    [Fact]
    public async Task A_running_scalar_query_is_interrupted_by_cancellation()
    {
        await using var duck = DuckSession.Open(":memory:");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        var running = duck.ScalarAsync<long>(Endless, cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Bound));
    }

    [Fact]
    public async Task An_interrupted_transaction_rolls_back_and_leaves_the_session_usable()
    {
        await using var duck = DuckSession.Open(":memory:");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        var running = duck.ExecuteTransactionAsync(
            ["create table kept_out(x int)", $"create table t as {Endless}"], cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running.WaitAsync(Bound));
        Assert.Equal(0, await duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'kept_out'"));
    }
}
