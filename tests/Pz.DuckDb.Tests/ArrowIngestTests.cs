using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Memory;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

public sealed class ArrowIngestTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public ArrowIngestTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, recursive: true); } catch { } }

    private DuckSession Open() => DuckSession.Open(Path.Combine(_dir, "t.duckdb"));

    private static Schema MatrixSchema() => new(
    [
        new Field("c_int", Int32Type.Default, nullable: true),
        new Field("c_long", Int64Type.Default, nullable: true),
        new Field("c_double", DoubleType.Default, nullable: true),
        new Field("c_dec", new Decimal128Type(38, 9), nullable: true),
        new Field("c_str", StringType.Default, nullable: true),
        new Field("c_bool", BooleanType.Default, nullable: true),
        new Field("c_date", Date32Type.Default, nullable: true),
        new Field("c_ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
    ], null);

    private static async IAsyncEnumerable<RecordBatch> MatrixBatches(int rows, int rowsPerBatch)
    {
        var builder = new ArrowBatchBuilder(MatrixSchema(), targetBatchBytes: int.MaxValue);
        for (var i = 0; i < rows; i++)
        {
            var isNullRow = i % 7 == 0;
            builder.AppendRow(isNullRow
                ? [null, null, null, null, null, null, null, null]
                : [i, (long)i * 10, i * 0.5, (decimal)i + 0.123456789m, $"s-{i}", i % 2 == 0,
                   new DateOnly(2026, 1, 1).AddDays(i % 300),
                   new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(i)]);
            if ((i + 1) % rowsPerBatch == 0 && builder.Flush() is { } b) { yield return b; }
        }
        if (builder.Flush() is { } last) { yield return last; }
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Ingest_roundtrips_type_matrix_with_nulls()
    {
        await using var duck = Open();
        var rows = await duck.IngestArrowAsync("main.matrix", MatrixSchema(), MatrixBatches(100, 17));

        Assert.Equal(100, rows);
        Assert.Equal(100, await duck.ScalarAsync<long>("select count(*) from main.matrix"));
        Assert.Equal(15, await duck.ScalarAsync<long>("select count(*) from main.matrix where c_int is null"));
        Assert.Equal(850L, await duck.ScalarAsync<long>("select c_long from main.matrix where c_int = 85"));
        Assert.Equal("s-85", await duck.ScalarAsync<string>("select c_str from main.matrix where c_int = 85"));
        Assert.Equal(85.123456789m, await duck.ScalarAsync<decimal>("select c_dec from main.matrix where c_int = 85"));
        Assert.True(await duck.ScalarAsync<bool>("select c_bool from main.matrix where c_int = 86"));
        Assert.Equal("2026-01-01 00:01:25", await duck.ScalarAsync<string>(
            "select strftime(c_ts, '%Y-%m-%d %H:%M:%S') from main.matrix where c_int = 85"));
        Assert.Equal("2026-03-27", await duck.ScalarAsync<string>(
            "select strftime(c_date, '%Y-%m-%d') from main.matrix where c_int = 85"));
    }

    [Fact]
    public async Task Ingest_streams_without_buffering()
    {
        await using var duck = Open();
        // Gate: producer may run at most 4 batches ahead of consumption. If the implementation
        // buffers the stream (e.g. ToList) before ingesting, the producer starves waiting for
        // releases that only happen on consumption -> the WhenAny below times out.
        var gate = new SemaphoreSlim(4);
        var consumed = 0;
        duck.OnBatchConsumedForTests = () => { Interlocked.Increment(ref consumed); gate.Release(); };
        try
        {
            async IAsyncEnumerable<RecordBatch> Gated()
            {
                await foreach (var b in MatrixBatches(2_000, 20))
                {
                    await gate.WaitAsync();
                    yield return b;
                }
            }

            var ingest = duck.IngestArrowAsync("main.streamed", MatrixSchema(), Gated());
            var winner = await Task.WhenAny(ingest, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(ReferenceEquals(winner, ingest),
                "ingest deadlocked behind the 4-batch gate — implementation is buffering instead of streaming");
            Assert.Equal(2_000, await ingest);
            Assert.Equal(100, consumed); // 2000 rows / 20 per batch
        }
        finally
        {
            duck.OnBatchConsumedForTests = null;
        }
    }

    [Fact]
    public async Task Ingest_cancellation_stops_promptly()
    {
        await using var duck = Open();
        using var cts = new CancellationTokenSource();

        async IAsyncEnumerable<RecordBatch> CancelAfterThree()
        {
            var n = 0;
            await foreach (var b in MatrixBatches(10_000, 50))
            {
                if (++n == 3) { cts.Cancel(); }
                yield return b;
            }
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => duck.IngestArrowAsync("main.cancelled", MatrixSchema(), CancelAfterThree(), cts.Token));

        // A failed or cancelled ingest must leave nothing
        // behind. CREATE TABLE for "main.cancelled" ran before the cancellation hit, so this proves the
        // post-CREATE cleanup path (dispose writer, then best-effort DROP TABLE) actually ran.
        Assert.Equal(0L, await duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'cancelled'"));
    }

    [Fact]
    public async Task Ingest_with_invalid_table_name_throws_and_does_not_leak()
    {
        await using var duck = Open();

        // ArrowIngestWriter.Create's SplitQualified only accepts a bare "table" or "schema.table" (1 or 2
        // dot-separated parts); 3+ parts throw ArgumentException. DuckSession.Open always ATTACHes the
        // database file under the fixed catalog alias "pz" and switches to it (see
        // DuckSession.AttachDatabase) — current_catalog() is therefore always "pz", never derived from the
        // database file's name — so "<catalog>.main.<table>" is still a *valid* 3-part catalog.schema.table
        // reference for CREATE TABLE — it succeeds — but is then rejected by SplitQualified once
        // ArrowIngestWriter.Create runs, i.e. *after* duckdb_schema_from_arrow has already allocated the
        // native converted-schema handle. That is the leak window: the converted schema must be
        // destroyed on this throw path, since ownership never transfers to a writer.
        //
        // We have no managed handle to the native allocation and so cannot assert directly that it was
        // freed (no leak-detection hook exists at this layer); what we assert instead is functional
        // no-corruption — the exception propagates cleanly out of IngestArrowAsync, the half-created
        // table left by the (already-succeeded) CREATE TABLE is cleaned up by the DROP-on-failure
        // path, and — the real proof there was no corruption of shared native state (connection handle,
        // duckdb instance) — a subsequent, ordinary ingest on the very same session still works.
        var catalog = await duck.ScalarAsync<string>("select current_catalog()");
        var badTable = $"{catalog}.main.leaktest";

        await Assert.ThrowsAsync<ArgumentException>(
            () => duck.IngestArrowAsync(badTable, MatrixSchema(), MatrixBatches(5, 5)));

        Assert.Equal(0L, await duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'leaktest'"));

        var rows = await duck.IngestArrowAsync("main.after_leak_check", MatrixSchema(), MatrixBatches(5, 5));
        Assert.Equal(5, rows);
        Assert.Equal(5, await duck.ScalarAsync<long>("select count(*) from main.after_leak_check"));
    }

    /// <summary>Does a
    /// <see cref="System.Buffers.MemoryManager{T}"/>-over-<see cref="NativeMemory"/> buffer survive C
    /// Data Interface export + DuckDB ingest across the full v0 type matrix? Builds batches through an
    /// explicit, freshly-instantiated <see cref="PooledNativeAllocator"/> (not <c>.Shared</c>, so the
    /// rented/pooled byte assertions below are exact) instead of relying on
    /// <see cref="ArrowBatchBuilder"/>'s default. <see cref="DuckSession.IngestArrowAsync"/> disposes
    /// each batch immediately after <c>ArrowIngestWriter.AppendBatch</c> returns — by the
    /// time this method returns, every buffer must be back in the free list, which is only true if the
    /// export's C Data Interface release callback ran synchronously within that same call (already
    /// established for the unpooled default allocator; this proves it holds for pooled
    /// memory too — a premature or missing release would corrupt the assertions below, most likely by
    /// crashing the process outright rather than merely failing an assertion).</summary>
    [Fact]
    public async Task Pooled_batches_roundtrip_duckdb_ingest()
    {
        await using var duck = Open();
        var pooled = new PooledNativeAllocator();

        async IAsyncEnumerable<RecordBatch> PooledMatrixBatches(int rows, int rowsPerBatch)
        {
            var builder = new ArrowBatchBuilder(MatrixSchema(), targetBatchBytes: int.MaxValue, allocator: pooled);
            for (var i = 0; i < rows; i++)
            {
                var isNullRow = i % 7 == 0;
                builder.AppendRow(isNullRow
                    ? [null, null, null, null, null, null, null, null]
                    : [i, (long)i * 10, i * 0.5, (decimal)i + 0.123456789m, $"s-{i}", i % 2 == 0,
                       new DateOnly(2026, 1, 1).AddDays(i % 300),
                       new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(i)]);
                if ((i + 1) % rowsPerBatch == 0 && builder.Flush() is { } b) { yield return b; }
            }
            if (builder.Flush() is { } last) { yield return last; }
            await Task.CompletedTask;
        }

        var rows = await duck.IngestArrowAsync("main.pooled_matrix", MatrixSchema(), PooledMatrixBatches(100, 17));

        Assert.Equal(100, rows);
        Assert.Equal(100, await duck.ScalarAsync<long>("select count(*) from main.pooled_matrix"));
        Assert.Equal(15, await duck.ScalarAsync<long>("select count(*) from main.pooled_matrix where c_int is null"));
        Assert.Equal(850L, await duck.ScalarAsync<long>("select c_long from main.pooled_matrix where c_int = 85"));
        Assert.Equal("s-85", await duck.ScalarAsync<string>("select c_str from main.pooled_matrix where c_int = 85"));
        Assert.Equal(85.123456789m, await duck.ScalarAsync<decimal>("select c_dec from main.pooled_matrix where c_int = 85"));
        Assert.True(await duck.ScalarAsync<bool>("select c_bool from main.pooled_matrix where c_int = 86"));
        Assert.Equal("2026-01-01 00:01:25", await duck.ScalarAsync<string>(
            "select strftime(c_ts, '%Y-%m-%d %H:%M:%S') from main.pooled_matrix where c_int = 85"));
        Assert.Equal("2026-03-27", await duck.ScalarAsync<string>(
            "select strftime(c_date, '%Y-%m-%d') from main.pooled_matrix where c_int = 85"));

        // Every rented buffer came back: IngestArrowAsync disposed each batch after appending it, and
        // nothing else is holding a reference — proves the pool's lifetime accounting survived the
        // real C Data Interface export/import round trip, not just a happy-path in-memory Build/Dispose.
        Assert.Equal(0, pooled.RentedBytes);
        Assert.True(pooled.PooledBytes > 0);
    }

    /// <summary><see cref="Pooled_batches_roundtrip_duckdb_ingest"/>
    /// only proves the happy path. Mirrors <see cref="Ingest_with_invalid_table_name_throws_and_does_not_leak"/>'s
    /// shape (force a failure, assert clean unwind, then prove no corruption via a subsequent successful
    /// ingest) but forces the failure mid-stream via <see cref="DuckSession.OnBatchConsumedForTests"/> —
    /// the same mechanism <c>NodeExecutorTests.Consumer_failure_cancels_partition_pump_promptly</c> uses —
    /// after at least one pooled batch has already been appended and returned to the pool. That batch's
    /// own release still runs (see <see cref="DuckSession"/>'s <c>finally { batch.Dispose(); }</c>, which
    /// executes even though the hook above it threw), so this proves the pool's rent/return accounting
    /// survives an exception raised between a successful append and that batch's dispose — no leaked
    /// native buffer, and no double-return corrupting a free list a later rent would hand out twice.</summary>
    [Fact]
    public async Task Pooled_batches_survive_ingest_failure_without_leak_or_double_return()
    {
        await using var duck = Open();
        var pooled = new PooledNativeAllocator();

        async IAsyncEnumerable<RecordBatch> PooledMatrixBatches(int rows, int rowsPerBatch)
        {
            var builder = new ArrowBatchBuilder(MatrixSchema(), targetBatchBytes: int.MaxValue, allocator: pooled);
            for (var i = 0; i < rows; i++)
            {
                var isNullRow = i % 7 == 0;
                builder.AppendRow(isNullRow
                    ? [null, null, null, null, null, null, null, null]
                    : [i, (long)i * 10, i * 0.5, (decimal)i + 0.123456789m, $"s-{i}", i % 2 == 0,
                       new DateOnly(2026, 1, 1).AddDays(i % 300),
                       new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(i)]);
                if ((i + 1) % rowsPerBatch == 0 && builder.Flush() is { } b) { yield return b; }
            }
            if (builder.Flush() is { } last) { yield return last; }
            await Task.CompletedTask;
        }

        var consumedBatches = 0;
        duck.OnBatchConsumedForTests = () =>
        {
            // Let the first batch's append + dispose complete cleanly, then fail on the second — proves
            // the failure path doesn't disturb buffers already returned to the pool, and that the
            // in-flight batch's own dispose (in DuckSession's finally) still runs after this throws.
            if (Interlocked.Increment(ref consumedBatches) == 2)
            {
                throw new InvalidOperationException("injected mid-ingest failure");
            }
        };

        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => duck.IngestArrowAsync("main.pooled_fail", MatrixSchema(), PooledMatrixBatches(100, 17)));
        }
        finally
        {
            duck.OnBatchConsumedForTests = null;
        }

        // No leak, no double-return crash: every buffer rented for both the succeeded first batch and the
        // batch whose post-append hook threw made it back to the pool.
        Assert.Equal(0, pooled.RentedBytes);

        // Post-failure reuse proves no corruption: an ordinary pooled ingest on the very same allocator and
        // session must still produce correct data (a corrupted free list would hand out an already-live
        // pointer here, producing wrong values or a crash rather than merely failing an assertion).
        var rows = await duck.IngestArrowAsync(
            "main.pooled_after_fail", MatrixSchema(), PooledMatrixBatches(100, 17));

        Assert.Equal(100, rows);
        Assert.Equal(100, await duck.ScalarAsync<long>("select count(*) from main.pooled_after_fail"));
        Assert.Equal(850L, await duck.ScalarAsync<long>("select c_long from main.pooled_after_fail where c_int = 85"));
        Assert.Equal("s-85", await duck.ScalarAsync<string>("select c_str from main.pooled_after_fail where c_int = 85"));
        Assert.Equal(85.123456789m, await duck.ScalarAsync<decimal>("select c_dec from main.pooled_after_fail where c_int = 85"));

        Assert.Equal(0, pooled.RentedBytes);
        Assert.True(pooled.PooledBytes > 0);
    }

    [Fact]
    public async Task Ingested_data_persists_across_session_reopen()
    {
        // Proves the ATTACH/USE-under-a-fixed-alias scheme in DuckSession.Open (see its comment) actually
        // persists to the physical file rather than only to the in-memory ADO connection it opens first —
        // a second, independent session opened against the same path must see everything the first one
        // committed to disk.
        await using (var duck = Open())
        {
            var rows = await duck.IngestArrowAsync("main.persisted", MatrixSchema(), MatrixBatches(50, 10));
            Assert.Equal(50, rows);
        }

        await using var reopened = Open();
        Assert.Equal(50, await reopened.ScalarAsync<long>("select count(*) from main.persisted"));
    }
}
