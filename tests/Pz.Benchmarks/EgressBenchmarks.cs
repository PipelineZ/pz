using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Pz.Connectors.Abstractions.Batches;
using Pz.DuckDb;

namespace Pz.Benchmarks;

/// <summary>1M-row egress throughput from a real temp DuckDB database: the source table is
/// populated once in <see cref="GlobalSetup"/> (not measured); the benchmark measures
/// <see cref="DuckSession.QueryArrowAsync"/> draining the full result set -- the same call
/// <c>SinkWriteExecutor</c>'s universal path makes in a real run. Reports elapsed time; rows/sec is
/// computed from <see cref="RowCount"/> in https://pipelinez.dev/performance/.</summary>
[MemoryDiagnoser]
[Config(typeof(MacroishBenchmarkConfig))]
public class EgressBenchmarks
{
    private const int RowCount = 1_000_000;
    private const string SourceTable = "staging.bench_egress";

    private string _dir = null!;
    private DuckSession _duck = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "pz-benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "bench.duckdb"));
        _duck.ExecuteAsync("create schema if not exists staging").GetAwaiter().GetResult();

        var schema = BenchData.BuildSchema();
        var rows = BenchData.GenerateRows(RowCount);
        _duck.IngestArrowAsync(SourceTable, schema, BuildBatches(schema, rows)).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _duck.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Benchmark(Description = "Egress 1M rows: DuckSession.QueryArrowAsync draining the full result set")]
    public async Task<long> Egress1MRows()
    {
        var rows = 0L;
        await foreach (var batch in _duck.QueryArrowAsync($"select * from {SourceTable}"))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static async IAsyncEnumerable<RecordBatch> BuildBatches(Schema schema, object?[][] rows)
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
}
