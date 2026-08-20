using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Resilience;

public class InstancePacingStateTests
{
    private static ConnectionDef Source(RateLimitDef? rateLimit) => new("pg", "postgres",
        new Dictionary<string, object?>(),
        [new DatasetDef("orders", new Dictionary<string, object?>(), null)],
        "sources/pg.yml", RateLimit: rateLimit);

    private static DagNode Node(NodeKind kind, object definition) =>
        new(new NodeId("aaaaaaaaaaaaaaaa"), kind, "n", [], null, definition);

    [Fact]
    public void Bucket_starts_full_and_drains()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(new RateLimitDef(60, 3), time);

        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.FromSeconds(1), state.TryAcquire());
    }

    [Fact]
    public void Refill_math()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(new RateLimitDef(60, 3), time);

        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.FromSeconds(1), state.TryAcquire());

        time.Advance(TimeSpan.FromSeconds(0.5));
        Assert.Equal(TimeSpan.FromSeconds(0.5), state.TryAcquire());

        time.Advance(TimeSpan.FromSeconds(0.5));
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
    }

    [Fact]
    public void Capacity_caps_refill()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(new RateLimitDef(60, 2), time);

        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());

        time.Advance(TimeSpan.FromHours(1));

        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.True(state.TryAcquire() > TimeSpan.Zero);
    }

    [Fact]
    public void No_bucket_means_no_wait()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(null, time);

        Assert.False(state.HasBucket);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        }
    }

    [Fact]
    public void Budget_zero_blocks_until_reset()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(null, time);

        state.ReportBudget(0, time.GetUtcNow() + TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.FromSeconds(30), state.TryAcquire());

        time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
    }

    // An unbounded budget-hint wait can silently wedge every
    // node of the instance run-wide (no event, breaker never trips) and an overflowing resetAt
    // (e.g. DateTimeOffset.MaxValue) would overflow the caller's Task.Delay. ReportBudget clamps
    // the stored resetAt to now + 15 minutes.

    [Fact]
    public void Budget_reset_clamped_to_max()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(null, time);

        state.ReportBudget(0, time.GetUtcNow() + TimeSpan.FromHours(2));
        Assert.Equal(TimeSpan.FromMinutes(15), state.TryAcquire());

        time.Advance(TimeSpan.FromMinutes(15));
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
    }

    [Fact]
    public void Budget_overflow_reset_clamped()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(null, time);

        state.ReportBudget(0, DateTimeOffset.MaxValue);
        Assert.Equal(TimeSpan.FromMinutes(15), state.TryAcquire());
    }

    [Fact]
    public void Budget_positive_is_inert()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(null, time);

        state.ReportBudget(5, time.GetUtcNow() + TimeSpan.FromSeconds(30));
        Assert.Equal(TimeSpan.Zero, state.TryAcquire());
    }

    [Fact]
    public void Budget_and_bucket_compose()
    {
        var time = new ManualTimeProvider();
        var state = new InstancePacingState(new RateLimitDef(60, 1), time);

        Assert.Equal(TimeSpan.Zero, state.TryAcquire()); // drain the one token

        state.ReportBudget(0, time.GetUtcNow() + TimeSpan.FromSeconds(10));
        Assert.Equal(TimeSpan.FromSeconds(10), state.TryAcquire()); // budget first

        time.Advance(TimeSpan.FromSeconds(10));
        // The bucket refills passively while the caller was gated on the budget wait: by the time the
        // 10s budget hint clears, the 1-token/sec bucket (burst=1) has long since refilled to capacity.
        Assert.Equal(TimeSpan.Zero, state.TryAcquire()); // remaining bucket wait
    }

    [Fact]
    public void Registry_shares_state_per_instance()
    {
        var time = new ManualTimeProvider();
        var registry = new RateLimiterRegistry(time);

        var source = Source(new RateLimitDef(60, 1));
        var nodeA = Node(NodeKind.SourceLoad, new SourceDatasetDef(source, source.Datasets[0]));
        var nodeB = Node(NodeKind.SourceLoad, new SourceDatasetDef(source, source.Datasets[0]));

        var stateA = registry.For(nodeA);
        var stateB = registry.For(nodeB);

        Assert.NotNull(stateA);
        Assert.Same(stateA, stateB);

        Assert.Equal(TimeSpan.Zero, stateA!.TryAcquire());
        Assert.Equal(TimeSpan.FromSeconds(1), stateB!.TryAcquire());
    }

    [Fact]
    public void Registry_null_for_pipeline_nodes()
    {
        var time = new ManualTimeProvider();
        var registry = new RateLimiterRegistry(time);

        var checkDef = new CheckNodeDef("p", new CheckDef("not_null", ["id"], new Dictionary<string, object?>(), null));
        var node = Node(NodeKind.Check, checkDef);

        Assert.Null(registry.For(node));
    }
}
