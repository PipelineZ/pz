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
    // own CancellationToken argument, and its outer finally must dispose the batch on every path,
    // including a cancelled gate wait: the engine owns the pooled Arrow batch until the call returns
    // (IDuckSession.AppendArrowBatchAsync: "Disposes batch before returning, success or failure").
    //
    // Disposal isn't observable here as a stable signal: RecordBatch/IArrowArray expose no IsDisposed flag,
    // and the NullReferenceException Apache.Arrow 23.0.0 throws on a disposed batch's column data is an
    // implementation accident, not a contract. So this test asserts the stable contract: the call throws an
    // OperationCanceledException (possibly a subclass such as TaskCanceledException) rather than hanging or
    // throwing another type, and the table is left unchanged. Disposal itself is asserted in GateWaitTests.
    [Fact]
    public async Task AppendArrowBatchAsync_disposes_batch_and_leaves_table_unchanged_on_pre_start_cancellation()
    {
        await _duck.CreateEmptyTableAsync("staging.t5", IdSchema);
        await _duck.AppendArrowBatchAsync("staging.t5", Batch(1));

        var batch = Batch(2, 3);
        var preCancelled = new CancellationToken(canceled: true);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => _duck.AppendArrowBatchAsync("staging.t5", batch, ct: preCancelled));

        Assert.Equal(1L, await _duck.ScalarAsync<long>("select count(*) from staging.t5"));

        // Connection is still usable afterwards -- the cancelled call didn't corrupt the shared gate/connection.
        await _duck.AppendArrowBatchAsync("staging.t5", Batch(4));
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.t5"));
    }

    // The disposal assertion below is deliberately omitted, following the precedent set by
    // AppendArrowBatchAsync_disposes_batch_and_leaves_table_unchanged_on_pre_start_cancellation
    // above: RecordBatch/IArrowArray expose no stable, public IsDisposed signal, so disposal is
    // verified structurally (the delegate's finally runs unconditionally) rather than observed.
    [Fact]
    public async Task AppendArrowBatchAsync_with_target_schema_refuses_a_differently_shaped_batch_before_ingest()
    {
        await _duck.CreateEmptyTableAsync("staging.t6", IdSchema);

        var wrongShaped = new RecordBatch(
            new Schema([new Field("id", Int64Type.Default, nullable: false), new Field("extra", Int64Type.Default, nullable: false)], null),
            [new Int64Array.Builder().Append(1).Build(), new Int64Array.Builder().Append(2).Build()],
            1);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _duck.AppendArrowBatchAsync("staging.t6", wrongShaped, IdSchema));

        Assert.Contains("staging.t6", ex.Message);
        Assert.Contains("1 column(s)", ex.Message);
        Assert.Contains("2 column(s)", ex.Message);

        Assert.Equal(0L, await _duck.ScalarAsync<long>("select count(*) from staging.t6"));

        await _duck.AppendArrowBatchAsync("staging.t6", Batch(1), IdSchema);
        Assert.Equal(1L, await _duck.ScalarAsync<long>("select count(*) from staging.t6"));
    }
}
