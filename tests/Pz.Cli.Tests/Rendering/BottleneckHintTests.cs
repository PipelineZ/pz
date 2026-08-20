using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

/// <summary>Pure wording/threshold
/// coverage for <see cref="BottleneckHint.For"/>, plus <see cref="ConsoleRenderer"/> integration proving
/// the hint prints as an extra line right after the node's own line.</summary>
public class BottleneckHintTests
{
    private static readonly DateTimeOffset At = new(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);

    private static NodeCompletedEvent Node(string kind, string name, long durationMs, long producerStallMs,
        long consumerStallMs) =>
        new(At, "run-1", "node-a", kind, name, "success", 100, durationMs, null, null,
            new NodeTimingsPayload(producerStallMs, consumerStallMs));

    [Fact]
    public void SourceLoad_consumer_dominant_is_source_bound()
    {
        // consumerStallMs = 70% of duration.
        var evt = Node("SourceLoad", "src_crm__customers", 100, 5, 70);

        Assert.Equal("hint: src_crm__customers: source-bound — ingest idle 70% of the node's runtime",
            BottleneckHint.For(evt));
    }

    [Fact]
    public void SourceLoad_producer_dominant_is_ingest_bound()
    {
        // producerStallMs = 65% of duration.
        var evt = Node("SourceLoad", "src_crm__customers", 100, 65, 5);

        Assert.Equal("hint: src_crm__customers: ingest-bound — reader idle 65% of the node's runtime",
            BottleneckHint.For(evt));
    }

    [Fact]
    public void SinkWrite_producer_dominant_is_staging_bound()
    {
        // producerStallMs = 80% of duration.
        var evt = Node("SinkWrite", "lake.orders_curated", 100, 80, 5);

        Assert.Equal("hint: lake.orders_curated: staging-bound — writer idle 80% of the node's runtime",
            BottleneckHint.For(evt));
    }

    [Fact]
    public void SinkWrite_consumer_dominant_is_sink_bound()
    {
        // consumerStallMs = 75% of duration.
        var evt = Node("SinkWrite", "lake.orders_curated", 100, 5, 75);

        Assert.Equal("hint: lake.orders_curated: sink-bound — staging idle 75% of the node's runtime",
            BottleneckHint.For(evt));
    }

    [Theory]
    [InlineData(59, 0)]
    [InlineData(0, 59)]
    [InlineData(30, 30)]
    public void Below_threshold_produces_no_hint(long producerStallMs, long consumerStallMs)
    {
        var evt = Node("SourceLoad", "src_crm__customers", 100, producerStallMs, consumerStallMs);

        Assert.Null(BottleneckHint.For(evt));
    }

    [Fact]
    public void Exactly_at_threshold_produces_a_hint()
    {
        var evt = Node("SourceLoad", "src_crm__customers", 100, 0, 60);

        Assert.NotNull(BottleneckHint.For(evt));
    }

    [Fact]
    public void Null_timings_produces_no_hint()
    {
        var evt = new NodeCompletedEvent(At, "run-1", "node-a", "SourceLoad", "orders", "success", 3, 10,
            null, null, null);

        Assert.Null(BottleneckHint.For(evt));
    }

    [Fact]
    public void Pipeline_and_check_kinds_never_hint_even_with_dominant_stall()
    {
        var pipeline = Node("Pipeline", "evens", 100, 90, 5);
        var check = Node("Check", "check_evens", 100, 5, 90);

        Assert.Null(BottleneckHint.For(pipeline));
        Assert.Null(BottleneckHint.For(check));
    }

    [Fact]
    public void ConsoleRenderer_prints_hint_immediately_after_the_node_line()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(Node("SourceLoad", "src_crm__customers", 100, 5, 70));

        var expected =
            $"ok src_crm__customers 100 rows 100ms{Environment.NewLine}" +
            $"hint: src_crm__customers: source-bound — ingest idle 70% of the node's runtime{Environment.NewLine}";
        Assert.Equal(expected, writer.ToString());
    }

    [Fact]
    public void ConsoleRenderer_prints_nothing_extra_below_threshold()
    {
        var writer = new StringWriter();
        var renderer = new ConsoleRenderer(writer);

        renderer.Render(Node("SourceLoad", "src_crm__customers", 100, 10, 10));

        Assert.Equal($"ok src_crm__customers 100 rows 100ms{Environment.NewLine}", writer.ToString());
    }
}
