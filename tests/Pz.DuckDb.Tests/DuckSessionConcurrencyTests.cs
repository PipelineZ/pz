using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

/// <summary>
/// Regression test for the race <see cref="DuckSession"/>'s <c>_gate</c> exists to prevent (see its doc
/// comment): a single shared connection is not safe for concurrent statement execution, and
/// <c>RunOrchestrator</c> dispatches nodes concurrently against one shared <see cref="DuckSession"/>, so
/// two node-shaped operations touching the connection at the same time would otherwise race directly on
/// native pending-query state (it shows up as a nondeterministic "duckdb_appender_close failed" / PZ0501).
/// This test reproduces that shape — many concurrent ingest/query/scalar/execute/schema operations
/// dispatched against ONE session — and asserts pure data correctness (no corruption, every row accounted
/// for) rather than timing. Connection-per-operation was measured and gave no real
/// speedup, so the gate stays; this test guards that the race cannot recur regardless of which connection
/// strategy a future change ships (whichever strategy ships, all N operations below must still complete
/// with fully correct results).
///
/// Deterministic by construction — no wall-clock sleeps: a <see cref="Barrier"/> released only once every
/// operation's task has reached it forces genuine simultaneous dispatch (real contention for the shared
/// session), and every assertion afterward is a plain equality check on committed data, not on timing.
/// </summary>
public sealed class DuckSessionConcurrencyTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public DuckSessionConcurrencyTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private DuckSession Open() => DuckSession.Open(Path.Combine(_dir, "concurrency.duckdb"));

    private static Schema IngestSchema() => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("val").DataType(Int64Type.Default).Nullable(false))
        .Build();

    /// <summary>Every value is a pure function of the row index — no randomness — so each ingested
    /// table's row count and checksum (sum of `val`) are exactly predictable and can be asserted without
    /// any dependency on execution order.</summary>
    private static async IAsyncEnumerable<RecordBatch> IngestBatches(int rowCount, int rowsPerBatch)
    {
        var builder = new ArrowBatchBuilder(IngestSchema(), targetBatchBytes: int.MaxValue);
        for (var i = 0; i < rowCount; i++)
        {
            builder.AppendRow([(long)i, (long)i * 2]);
            if ((i + 1) % rowsPerBatch == 0 && builder.Flush() is { } batch)
            {
                yield return batch;
            }
        }

        if (builder.Flush() is { } last)
        {
            yield return last;
        }

        await Task.CompletedTask;
    }

    private static long ExpectedChecksum(int rowCount)
    {
        // sum(i * 2) for i in [0, rowCount) == 2 * (rowCount - 1) * rowCount / 2 == (rowCount - 1) * rowCount
        return (long)(rowCount - 1) * rowCount;
    }

    [Fact]
    public async Task Concurrent_ingest_query_scalar_execute_and_schema_operations_are_all_correct()
    {
        await using var duck = Open();

        // Seed data every query/scalar/schema operation below reads concurrently — ingested up front,
        // synchronously, so it's stable and already committed before the barrier below releases.
        const int SeedRows = 3_000;
        await duck.IngestArrowAsync("main.seed", IngestSchema(), IngestBatches(SeedRows, 50));
        var expectedSeedChecksum = ExpectedChecksum(SeedRows);

        const int IngestOpCount = 3;
        const int QueryOpCount = 2;
        const int ScalarOpCount = 2;
        const int ExecuteOpCount = 1;
        const int SchemaOpCount = 1;
        const int TotalOps = IngestOpCount + QueryOpCount + ScalarOpCount + ExecuteOpCount + SchemaOpCount;

        // Released only once every one of the TotalOps tasks below has reached it -- forces genuinely
        // simultaneous dispatch against the shared session instead of hoping the thread pool happens to
        // overlap them. No timing/sleep involved: SignalAndWait blocks until the last participant arrives.
        using var startBarrier = new Barrier(TotalOps);

        const int RowsPerIngest = 4_000;
        var expectedIngestChecksum = ExpectedChecksum(RowsPerIngest);

        var ingestTasks = Enumerable.Range(0, IngestOpCount).Select(i => Task.Run(async () =>
        {
            startBarrier.SignalAndWait();
            var rows = await duck.IngestArrowAsync(
                $"main.ingest_{i}", IngestSchema(), IngestBatches(RowsPerIngest, 25));
            Assert.Equal(RowsPerIngest, rows);
        })).ToArray();

        var queryTasks = Enumerable.Range(0, QueryOpCount).Select(_ => Task.Run(async () =>
        {
            startBarrier.SignalAndWait();
            long rows = 0;
            long checksum = 0;
            await foreach (var batch in duck.QueryArrowAsync(
                "select id, val from main.seed order by id", targetBatchBytes: 4096))
            {
                using var b = batch;
                var vals = (Int64Array)b.Column(1);
                for (var i = 0; i < b.Length; i++)
                {
                    rows++;
                    checksum += vals.GetValue(i)!.Value;
                }
            }

            Assert.Equal(SeedRows, rows);
            Assert.Equal(expectedSeedChecksum, checksum);
        })).ToArray();

        var scalarTasks = Enumerable.Range(0, ScalarOpCount).Select(_ => Task.Run(async () =>
        {
            startBarrier.SignalAndWait();
            var count = await duck.ScalarAsync<long>("select count(*) from main.seed");
            Assert.Equal(SeedRows, count);
        })).ToArray();

        var executeTasks = Enumerable.Range(0, ExecuteOpCount).Select(_ => Task.Run(async () =>
        {
            startBarrier.SignalAndWait();
            await duck.ExecuteAsync("create table main.exec_marker as select 1 as x");
        })).ToArray();

        var schemaTasks = Enumerable.Range(0, SchemaOpCount).Select(_ => Task.Run(async () =>
        {
            startBarrier.SignalAndWait();
            var schema = await duck.GetResultSchemaAsync("select id, val from main.seed");
            Assert.Equal(["id", "val"], schema.FieldsList.Select(f => f.Name));
        })).ToArray();

        await Task.WhenAll(ingestTasks.Concat(queryTasks).Concat(scalarTasks).Concat(executeTasks).Concat(schemaTasks));

        // Post-hoc correctness: every table the concurrent operations touched is exactly what it should
        // be -- no partial writes, no cross-contamination between operations, no corruption.
        for (var i = 0; i < IngestOpCount; i++)
        {
            var count = await duck.ScalarAsync<long>($"select count(*) from main.ingest_{i}");
            var checksum = await duck.ScalarAsync<long>($"select sum(val)::bigint from main.ingest_{i}");
            Assert.Equal(RowsPerIngest, count);
            Assert.Equal(expectedIngestChecksum, checksum);
        }

        Assert.Equal(SeedRows, await duck.ScalarAsync<long>("select count(*) from main.seed"));
        Assert.Equal(expectedSeedChecksum, await duck.ScalarAsync<long>("select sum(val)::bigint from main.seed"));
        Assert.Equal(1L, await duck.ScalarAsync<long>("select x from main.exec_marker"));
    }
}
