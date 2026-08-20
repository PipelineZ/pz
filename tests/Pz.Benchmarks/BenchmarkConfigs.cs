using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace Pz.Benchmarks;

/// <summary>Shared stability settings for the cheap, high-iteration-count micro-benchmarks (builder
/// append, allocator rent/return): <see cref="MemoryDiagnoser"/> on, and a raised
/// MinIterationCount so BenchmarkDotNet's own statistics settle before it decides "enough samples" --
/// these ops are fast enough that its default pilot stage would otherwise stop very early.</summary>
public sealed class MicroBenchmarkConfig : ManualConfig
{
    public MicroBenchmarkConfig()
    {
        AddJob(Job.Default.WithMinIterationCount(10).WithId("PzMicro"));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}

/// <summary>Shared settings for the expensive, 1M-row DuckDB round-trip benchmarks (ingest/egress):
/// one real invocation per iteration (no BenchmarkDotNet unrolling multiple calls together, which would
/// blur a single 1M-row run's cost across several DB operations) and a small fixed iteration count --
/// full statistical rigor isn't the point here, a real measured number for https://pipelinez.dev/performance/ is.</summary>
public sealed class MacroishBenchmarkConfig : ManualConfig
{
    public MacroishBenchmarkConfig()
    {
        AddJob(Job.Default
            .WithStrategy(RunStrategy.Monitoring)
            .WithLaunchCount(1)
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithInvocationCount(1)
            .WithUnrollFactor(1)
            .WithId("PzMacroish"));
        AddDiagnoser(MemoryDiagnoser.Default);
    }
}
