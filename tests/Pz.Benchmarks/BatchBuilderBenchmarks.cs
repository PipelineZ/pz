using Apache.Arrow;
using BenchmarkDotNet.Attributes;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Benchmarks;

/// <summary>Micro-benchmark for <see cref="ArrowBatchBuilder"/>'s hot path: append N rows, taking and
/// disposing batches as the target size is reached. Uses the default allocator
/// (<c>PooledNativeAllocator.Shared</c>) -- the production default -- so this number reflects what every
/// real run actually pays per row.</summary>
[MemoryDiagnoser]
[Config(typeof(MicroBenchmarkConfig))]
public class BatchBuilderBenchmarks
{
    private const int RowCount = 100_000;

    private Schema _schema = null!;
    private object?[][] _rows = null!;

    [GlobalSetup]
    public void Setup()
    {
        _schema = BenchData.BuildSchema();
        _rows = BenchData.GenerateRows(RowCount);
    }

    [Benchmark(Description = "Append 100k rows through ArrowBatchBuilder (pooled allocator, default batch size)")]
    public int AppendAllRowsAndFlush()
    {
        var builder = new ArrowBatchBuilder(_schema);
        var batchesEmitted = 0;

        foreach (var row in _rows)
        {
            builder.AppendRow(row);
            if (builder.TryTakeBatch(out var batch))
            {
                batch!.Dispose();
                batchesEmitted++;
            }
        }

        var final = builder.Flush();
        if (final is not null)
        {
            final.Dispose();
            batchesEmitted++;
        }

        return batchesEmitted;
    }
}
