using System.Runtime.CompilerServices;
using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Pz.Connectors.Abstractions.Batches;
using Pz.DuckDb;

namespace Pz.Benchmarks;

/// <summary>1M-row ingest throughput against a real temp DuckDB database: builds Arrow
/// batches from pre-generated (fixed-seed) row values and streams them through
/// <see cref="DuckSession.IngestArrowAsync"/> -- the same call <c>SourceLoadExecutor</c> makes in a real
/// run. Reports elapsed time; rows/sec is computed from <see cref="RowCount"/> in https://pipelinez.dev/performance/.</summary>
[MemoryDiagnoser]
[Config(typeof(MacroishBenchmarkConfig))]
public class IngestBenchmarks
{
    private const int RowCount = 1_000_000;
    private const string TargetTable = "staging.bench_ingest";

    private string _dir = null!;
    private DuckSession _duck = null!;
    private Schema _schema = null!;
    private object?[][] _rows = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _dir = Path.Combine(Path.GetTempPath(), "pz-benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "bench.duckdb"));
        _duck.ExecuteAsync("create schema if not exists staging").GetAwaiter().GetResult();

        _schema = BenchData.BuildSchema();
        _rows = BenchData.GenerateRows(RowCount);
    }

    [IterationSetup]
    public void IterationSetup() =>
        _duck.ExecuteAsync($"drop table if exists {TargetTable}").GetAwaiter().GetResult();

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _duck.DisposeAsync().AsTask().GetAwaiter().GetResult();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Benchmark(Description = "Ingest 1M rows: build batches + DuckSession.IngestArrowAsync")]
    public async Task<long> Ingest1MRows() => await _duck.IngestArrowAsync(TargetTable, _schema, BuildBatches());

    private async IAsyncEnumerable<RecordBatch> BuildBatches([EnumeratorCancellation] CancellationToken ct = default)
    {
        var builder = new ArrowBatchBuilder(_schema);
        foreach (var row in _rows)
        {
            ct.ThrowIfCancellationRequested();
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
