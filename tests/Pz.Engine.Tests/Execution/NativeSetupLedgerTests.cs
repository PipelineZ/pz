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
        var ledger = new NativeSetupLedger(_duck);
        await ledger.ExecuteOnceAsync(statement, CancellationToken.None);
        await ledger.ExecuteOnceAsync(statement, CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'once_t'"));
    }

    [Fact]
    public async Task Distinct_statement_texts_each_run()
    {
        var ledger = new NativeSetupLedger(_duck);
        await ledger.ExecuteOnceAsync("create table a_t(x int)", CancellationToken.None);
        await ledger.ExecuteOnceAsync("create table b_t(x int)", CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name = 'a_t'"));
        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from duckdb_tables() where table_name = 'b_t'"));
    }

    [Fact]
    public async Task A_failed_statement_is_forgotten_so_a_retry_reissues_it()
    {
        const string statement = "insert into later_t values (1)";
        var ledger = new NativeSetupLedger(_duck);

        var failure = await Assert.ThrowsAsync<PzConnectorException>(
            () => ledger.ExecuteOnceAsync(statement, CancellationToken.None));
        Assert.Contains("PZ0311", failure.Message, StringComparison.Ordinal);

        await _duck.ExecuteAsync("create table later_t(x int)");

        // Same ledger, same statement text: the failed attempt above must have been forgotten, not
        // memoized as "already ran" -- a node retry re-issues it.
        await ledger.ExecuteOnceAsync(statement, CancellationToken.None);

        Assert.Equal(1, await _duck.ScalarAsync<long>("select count(*) from later_t"));
    }

    [Fact]
    public async Task Concurrent_callers_share_one_execution()
    {
        var gate = new TaskCompletionSource();
        var blocking = new GatedFirstCallDuckSession(_duck, gate.Task);
        var ledger = new NativeSetupLedger(blocking);
        const string statement = "create table concurrent_t(x int)";

        var first = ledger.ExecuteOnceAsync(statement, CancellationToken.None);
        var second = ledger.ExecuteOnceAsync(statement, CancellationToken.None);

        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        gate.SetResult();
        await first;
        await second;

        Assert.Equal(1, blocking.ExecuteCallCount);
        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'concurrent_t'"));
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
}
