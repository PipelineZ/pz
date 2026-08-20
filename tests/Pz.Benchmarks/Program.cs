using BenchmarkDotNet.Running;

namespace Pz.Benchmarks;

internal static class Program
{
    // BenchmarkDotNet's own CLI parsing handles `--job short` (its ShortRun preset) out of the box --
    // that is the smoke-mode entry point CI calls; no custom argument handling needed here.
    private static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
}
