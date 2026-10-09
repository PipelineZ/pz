using System.Collections.Concurrent;
using System.Diagnostics;
using Pz.Cli.Commands;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Tests.Otel;

/// <summary>A caller that hands pz a W3C <c>TRACEPARENT</c> gets pz's <c>run</c> span inside its own trace.
/// The listener here is process-global BCL state, so each test identifies its spans by a trace id it chose.</summary>
public sealed class TraceParentTests
{
    private const string TraceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    private const string ParentSpanId = "00f067aa0ba902b7";
    private static readonly string Valid = $"00-{TraceId}-{ParentSpanId}-01";

    [Fact]
    public void Unset_resolves_to_null_without_a_note()
    {
        Assert.Null(RunCommand.ResolveTraceParent(null, null, out var note));
        Assert.Null(note);
        Assert.Null(RunCommand.ResolveTraceParent("  ", "k=v", out note));
        Assert.Null(note);
    }

    [Fact]
    public void A_valid_traceparent_resolves_to_its_trace_and_parent_span()
    {
        var context = RunCommand.ResolveTraceParent($" {Valid} ", "vendor=x", out var note);

        Assert.Null(note);
        Assert.NotNull(context);
        Assert.Equal(TraceId, context!.Value.TraceId.ToHexString());
        Assert.Equal(ParentSpanId, context.Value.SpanId.ToHexString());
        Assert.Equal(ActivityTraceFlags.Recorded, context.Value.TraceFlags);
        Assert.True(context.Value.IsRemote);
        Assert.Equal("vendor=x", context.Value.TraceState);
    }

    [Theory]
    [InlineData("not-a-traceparent")]
    [InlineData("00-00000000000000000000000000000000-00f067aa0ba902b7-01")]
    [InlineData("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")]
    public void An_invalid_traceparent_is_ignored_with_a_note(string value)
    {
        Assert.Null(RunCommand.ResolveTraceParent(value, null, out var note));
        Assert.NotNull(note);
        Assert.Contains("TRACEPARENT", note, StringComparison.Ordinal);
    }

    [Fact]
    public void The_run_span_joins_the_callers_trace()
    {
        var seen = new ConcurrentBag<Activity>();
        using var listener = Listen(seen);

        var parent = RunCommand.ResolveTraceParent(Valid, null, out _);
        using (RunCommand.StartRunActivity(parent)) { }

        var run = Assert.Single(seen, a => a.TraceId.ToHexString() == TraceId);
        Assert.Equal("run", run.OperationName);
        Assert.Equal(ParentSpanId, run.ParentSpanId.ToHexString());
    }

    [Fact]
    public void Without_a_parent_the_run_span_is_a_root()
    {
        var seen = new ConcurrentBag<Activity>();
        using var listener = Listen(seen);

        using var run = RunCommand.StartRunActivity(null);

        Assert.NotNull(run);
        Assert.Equal(default, run!.ParentSpanId);
    }

    private static ActivityListener Listen(ConcurrentBag<Activity> seen)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == PzActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = seen.Add,
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
