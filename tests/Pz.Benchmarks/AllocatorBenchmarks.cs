using Apache.Arrow.Memory;
using BenchmarkDotNet.Attributes;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Benchmarks;

/// <summary>Pooled-vs-unpooled allocator comparison: renting and returning a
/// batch-sized buffer through <see cref="PooledNativeAllocator"/> vs Apache.Arrow's own
/// <see cref="NativeMemoryAllocator"/> (a fresh <c>NativeMemory.AlignedAlloc</c>/free pair every call --
/// the "unpooled" baseline). Both instances are shared across invocations within a run so the pooled
/// case measures warm-pool steady state, which is the case that matters in a long-running pipeline.</summary>
[MemoryDiagnoser]
[Config(typeof(MicroBenchmarkConfig))]
public class AllocatorBenchmarks
{
    private static readonly PooledNativeAllocator Pooled = new();
    private static readonly NativeMemoryAllocator Unpooled = new(64);

    [Params(64 * 1024, 1024 * 1024)]
    public int RequestSize { get; set; }

    [Benchmark(Baseline = true, Description = "Unpooled: Apache.Arrow.NativeMemoryAllocator rent+return")]
    public void Unpooled_RentReturn()
    {
        using var owner = Unpooled.Allocate(RequestSize);
    }

    [Benchmark(Description = "Pooled: PooledNativeAllocator rent+return (warm pool)")]
    public void Pooled_RentReturn()
    {
        using var owner = Pooled.Allocate(RequestSize);
    }
}
