#!/usr/bin/env dotnet
#:property PublishAot=false
#:project ../src/Pz.Connectors.Abstractions/Pz.Connectors.Abstractions.csproj
#:project ../src/Pz.DuckDb/Pz.DuckDb.csproj

// Quantifies the DuckSession gate's serialization
// cost -- the correctness fix from the full-suite-parallel-flake investigation (see DuckSession._gate's
// doc comment) that made every operation touching the one shared native connection queue instead of
// racing, at the cost of zero real overlap between concurrent nodes. This probe measures that cost
// directly against the real DuckSession/ArrowBatchBuilder production code (no test doubles).
//
// Three operations -- two source-style ingests and one deliberately slow "sink" egress query (a
// per-row hash WHERE clause forces real DuckDB compute that can't be constant-folded away) -- are run
// twice, back to back against the SAME already-warmed DuckSession (JIT/plan-cache/buffer-pool state
// equalized first by a throwaway warm-up pass, so the two measured runs are apples-to-apples):
//
//   1. SEQUENTIAL: awaited one at a time, in order -- never attempts overlap.
//   2. CONCURRENT: issued together via Task.WhenAll -- the same shape RunOrchestrator's concurrent
//      node dispatch uses in a real run.
//
// If the gate produces zero real overlap (the documented, correctness-first behavior), concurrent
// elapsed ~= sequential elapsed: asking for concurrency bought nothing. Any daylight where concurrent
// is faster is overlap the single-connection gate still permits (e.g. between DuckDB's own internal
// query threads and .NET-side work); any daylight the other way is pure Task.WhenAll/dispatch overhead.

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.DuckDb;

const int RowsPerSource = 200_000;
const long SlowSinkRangeRows = 200_000_000;

var dir = Path.Combine(Path.GetTempPath(), "pz-gate-probe", Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(dir);

try
{
    await using var duck = DuckSession.Open(Path.Combine(dir, "probe.duckdb"));
    await duck.ExecuteAsync("create schema if not exists staging");

    var schema = BuildSchema();

    async Task IngestAsync(string table, int seed)
    {
        var rows = GenerateRows(RowsPerSource, seed);
        await duck.IngestArrowAsync(table, schema, BuildBatches(schema, rows));
    }

    async Task SlowSinkAsync()
    {
        // count(*), not sum(hash(...)), so the result stays a plain bigint (DuckDB's hash() returns
        // HugeInt, which has no Arrow mapping in the v0 type matrix) -- the WHERE clause still forces a
        // real per-row hash computation DuckDB can't constant-fold away.
        await foreach (var batch in duck.QueryArrowAsync(
            $"select count(*) as h from range({SlowSinkRangeRows}) t(i) where hash(i) % 1000000 = 0"))
        {
            batch.Dispose();
        }
    }

    async Task<TimeSpan> Time(Func<Task> action)
    {
        var sw = Stopwatch.StartNew();
        await action();
        sw.Stop();
        return sw.Elapsed;
    }

    async Task DropWarmTablesAsync()
    {
        await duck.ExecuteAsync("drop table if exists staging.warm_a");
        await duck.ExecuteAsync("drop table if exists staging.warm_b");
    }

    async Task DropRunTablesAsync(string suffix)
    {
        await duck.ExecuteAsync($"drop table if exists staging.{suffix}_a");
        await duck.ExecuteAsync($"drop table if exists staging.{suffix}_b");
    }

    // Throwaway warm-up: JITs every code path, populates DuckDB's query-plan cache and buffer pool, so
    // the two MEASURED runs below start from equalized state (apples-to-apples) instead of one of them
    // unfairly eating first-run costs the other doesn't pay.
    await DropWarmTablesAsync();
    await IngestAsync("staging.warm_a", 1);
    await IngestAsync("staging.warm_b", 2);
    await SlowSinkAsync();
    await DropWarmTablesAsync();

    var sequential = await Time(async () =>
    {
        await IngestAsync("staging.seq_a", 1);
        await IngestAsync("staging.seq_b", 2);
        await SlowSinkAsync();
    });
    await DropRunTablesAsync("seq");
    Console.WriteLine($"Sequential (one at a time, in order): {sequential.TotalSeconds:F2}s");

    var concurrent = await Time(() => Task.WhenAll(
        IngestAsync("staging.conc_a", 1),
        IngestAsync("staging.conc_b", 2),
        SlowSinkAsync()));
    await DropRunTablesAsync("conc");
    Console.WriteLine($"Concurrent (all three via Task.WhenAll, same DuckSession): {concurrent.TotalSeconds:F2}s");

    Console.WriteLine();
    Console.WriteLine($"Serialization cost (concurrent - sequential): {(concurrent - sequential).TotalSeconds:F2}s");
    Console.WriteLine($"Concurrent / sequential ratio: {concurrent.TotalSeconds / sequential.TotalSeconds:F2} " +
        "(near 1.0 = fully serialized, zero overlap gained from concurrency; well below 1.0 = real overlap achieved)");
}
finally
{
    try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
}

static Schema BuildSchema() => new Schema.Builder()
    .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
    .Field(f => f.Name("amount").DataType(DoubleType.Default).Nullable(false))
    .Build();

static object?[][] GenerateRows(int rowCount, int seed)
{
    var random = new Random(seed);
    var rows = new object?[rowCount][];
    for (var i = 0; i < rowCount; i++)
    {
        rows[i] = [(long)i, random.NextDouble() * 1000];
    }

    return rows;
}

static async IAsyncEnumerable<RecordBatch> BuildBatches(Schema schema, object?[][] rows)
{
    var builder = new ArrowBatchBuilder(schema);
    foreach (var row in rows)
    {
        builder.AppendRow(row);
        if (builder.TryTakeBatch(out var batch))
        {
            yield return batch!;
            await Task.Yield();
        }
    }

    var final = builder.Flush();
    if (final is not null)
    {
        yield return final;
    }
}
