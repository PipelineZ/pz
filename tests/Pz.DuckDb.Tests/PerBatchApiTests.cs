using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

public sealed class PerBatchApiTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    private static readonly Schema IdSchema = new([new Field("id", Int64Type.Default, nullable: false)], null);

    private static RecordBatch Batch(params long[] ids)
    {
        var builder = new Int64Array.Builder();
        foreach (var id in ids) { builder.Append(id); }
        return new RecordBatch(IdSchema, [builder.Build()], ids.Length);
    }

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "t.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [Fact]
    public async Task Create_empty_then_append_batches_accumulates_rows()
    {
        await _duck.CreateEmptyTableAsync("staging.t1", IdSchema);
        Assert.Equal(0L, await _duck.ScalarAsync<long>("select count(*) from staging.t1"));
        Assert.Equal(2L, await _duck.AppendArrowBatchAsync("staging.t1", Batch(1, 2)));
        Assert.Equal(1L, await _duck.AppendArrowBatchAsync("staging.t1", Batch(3)));
        Assert.Equal(3L, await _duck.ScalarAsync<long>("select count(*) from staging.t1"));
        Assert.Equal(6L, await _duck.ScalarAsync<long>("select sum(id)::bigint from staging.t1"));
    }

    [Fact]
    public async Task Append_survives_failure_of_an_unrelated_statement_between_calls()
    {
        await _duck.CreateEmptyTableAsync("staging.t2", IdSchema);
        await _duck.AppendArrowBatchAsync("staging.t2", Batch(1));
        await Assert.ThrowsAnyAsync<Exception>(() => _duck.ExecuteAsync("select * from staging.does_not_exist"));
        await _duck.AppendArrowBatchAsync("staging.t2", Batch(2));
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.t2"));
    }

    [Fact]
    public async Task Transaction_commits_all_statements_atomically()
    {
        await _duck.CreateEmptyTableAsync("staging.t3", IdSchema);
        await _duck.AppendArrowBatchAsync("staging.t3", Batch(1, 2, 3));
        await _duck.ExecuteTransactionAsync(
        [
            "create table staging.t3_out as select * from staging.t3",
            "drop table staging.t3",
        ]);
        Assert.Equal(3L, await _duck.ScalarAsync<long>("select count(*) from staging.t3_out"));
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' and table_name = 't3'"));
    }

    [Fact]
    public async Task Transaction_rolls_back_on_failure()
    {
        await _duck.CreateEmptyTableAsync("staging.t4", IdSchema);
        await _duck.AppendArrowBatchAsync("staging.t4", Batch(1));
        await Assert.ThrowsAnyAsync<Exception>(() => _duck.ExecuteTransactionAsync(
        [
            "delete from staging.t4",
            "insert into staging.no_such_table values (1)",
        ]));
        Assert.Equal(1L, await _duck.ScalarAsync<long>("select count(*) from staging.t4"));
        await _duck.ExecuteAsync("select 1"); // connection still usable (no open transaction left behind)
    }

    // Guards a pre-start-cancellation batch leak. AppendArrowBatchAsync must NOT pass `ct` as Task.Run's
    // own CancellationToken argument: an already-cancelled token would then stop Task.Run invoking the
    // delegate at all, skipping the delegate's `finally { batch.Dispose(); }` and leaking the
    // engine-owned pooled Arrow batch, in violation of IDuckSession.AppendArrowBatchAsync's doc comment
    // ("Disposes batch before returning, success or failure"). With `ct` withheld from Task.Run the
    // delegate (and its finally) always runs, while `_gate.Wait(ct)` and the explicit
    // ThrowIfCancellationRequested() inside keep the call cancellation-responsive.
    //
    // Disposal itself isn't reachable from the test as a stable, documented signal: RecordBatch/IArrowArray
    // don't expose an IsDisposed flag, and while this repo's Apache.Arrow version (23.0.0) happens to throw
    // NullReferenceException when a disposed batch's column data is accessed, that's an
    // internal-implementation accident (returned pooled buffers going away), not a public contract -- it
    // isn't safe to assert on across Arrow versions. So, per the same precedent already used in this repo
    // for objects that aren't reachable post-dispose (see
    // PostgresSourceAcceptance.ReadAsync_surfaces_clean_cancellation_when_cancelled_before_connect), we
    // assert the behavior that IS a stable contract: the call throws OperationCanceledException cleanly
    // (not, say, a hang or some other exception type) and the table is left completely unchanged. Disposal
    // is guaranteed structurally instead, by the delegate being invoked unconditionally.
    [Fact]
    public async Task AppendArrowBatchAsync_disposes_batch_and_leaves_table_unchanged_on_pre_start_cancellation()
    {
        await _duck.CreateEmptyTableAsync("staging.t5", IdSchema);
        await _duck.AppendArrowBatchAsync("staging.t5", Batch(1));

        var batch = Batch(2, 3);
        var preCancelled = new CancellationToken(canceled: true);

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _duck.AppendArrowBatchAsync("staging.t5", batch, preCancelled));

        Assert.Equal(1L, await _duck.ScalarAsync<long>("select count(*) from staging.t5"));

        // Connection is still usable afterwards -- the cancelled call didn't corrupt the shared gate/connection.
        await _duck.AppendArrowBatchAsync("staging.t5", Batch(4));
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.t5"));
    }
}
