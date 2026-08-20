using System.Diagnostics;
using Pz.Diagnostics.Otel;

namespace Pz.Diagnostics.Tests.Otel;

/// <summary>Proves the "zero-cost when unset" claim for
/// <see cref="PzActivitySource"/> — with no <see cref="ActivityListener"/> registered anywhere in the
/// process, <c>StartActivity</c> is a documented BCL no-op. This file must NEVER register an
/// <see cref="ActivityListener"/> itself (that would leak process-wide via the static
/// <see cref="ActivitySource"/> and invalidate every assertion below, including in tests that run after
/// it) — listener-based span-tree assertions live in tests/Pz.Engine.Tests/Otel/SpanParentageTests.cs,
/// in a separate test process.</summary>
public sealed class OtelPrimitivesTests
{
    [Fact]
    public void No_listener_means_null_activities()
    {
        using var activity = PzActivitySource.Instance.StartActivity("x");

        Assert.Null(activity);
        Assert.Null(Activity.Current);
    }

    [Fact]
    public void Zero_listener_start_activity_allocates_nothing_material()
    {
        // Warm-up: JIT the call path and let any one-time static initialization happen before measuring.
        for (var i = 0; i < 10; i++)
        {
            PzActivitySource.Instance.StartActivity("warmup")?.Dispose();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            PzActivitySource.Instance.StartActivity("x")?.Dispose();
        }

        var delta = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(delta < 1024,
            $"expected under 1KB allocated across 1000 no-listener StartActivity calls, got {delta} bytes");
    }
}
