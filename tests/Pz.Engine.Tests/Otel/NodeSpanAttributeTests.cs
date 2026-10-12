using System.Diagnostics;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Otel;

public sealed class NodeSpanAttributeTests : IDisposable
{
    private readonly ActivitySource _source = new("node-span-tags-" + Guid.NewGuid().ToString("N"));
    private readonly ActivityListener _listener;

    public NodeSpanAttributeTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == _source.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _source.Dispose();
    }

    private static NodeResult Result(NodeStatus status, PzError? error = null) =>
        new(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_pg__orders", status, 42,
            TimeSpan.FromSeconds(2), error, Ops: new OpStats(10, 2, 350),
            Facts: new NodeFacts("postgres", "warehouse", NodeFacts.Arrow,
                new StageTimings(TimeSpan.FromMilliseconds(900), TimeSpan.FromMilliseconds(1100), null, null), 3));

    [Fact]
    public void Succeeded_node_span_carries_the_facts()
    {
        using var span = _source.StartActivity("node.SourceLoad")!;

        NodeSpanTags.Apply(span, Result(NodeStatus.Success));

        Assert.Equal("SourceLoad", span.GetTagItem("pz.node.kind"));
        Assert.Equal(42L, span.GetTagItem("pz.node.rows"));
        Assert.Equal("postgres", span.GetTagItem("pz.connector"));
        Assert.Equal("warehouse", span.GetTagItem("pz.connection"));
        Assert.Equal("arrow", span.GetTagItem("pz.node.strategy"));
        Assert.Equal(900L, span.GetTagItem("pz.stage.extract_ms"));
        Assert.Equal(1100L, span.GetTagItem("pz.stage.ingest_ms"));
        Assert.Null(span.GetTagItem("pz.stage.write_ms"));
        Assert.Equal(3, span.GetTagItem("pz.node.attempts"));
        Assert.Equal(2L, span.GetTagItem("pz.ops.retried"));
        Assert.Equal(350L, span.GetTagItem("pz.ops.throttle_wait_ms"));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.Empty(span.Events);
    }

    [Fact]
    public void Failed_node_span_description_is_sanitized_first_line()
    {
        using var span = _source.StartActivity("node.SourceLoad")!;
        var message = "Binder Error: column \"x\" not found\nLINE 1: select x from secret_table\n        ^";
        var error = new PzError(PzErrorCode.NodeFailed, message, null, null, null);

        NodeSpanTags.Apply(span, Result(NodeStatus.Failed, error));

        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(PzErrorCode.NodeFailed, span.GetTagItem("error.type"));
        Assert.DoesNotContain("secret_table", span.StatusDescription);
        Assert.DoesNotContain("\n", span.StatusDescription);
        var ex = Assert.Single(span.Events);
        Assert.Equal("exception", ex.Name);
        Assert.Equal(span.StatusDescription, ex.Tags.Single(t => t.Key == "exception.message").Value);
        Assert.Equal(PzErrorCode.NodeFailed, ex.Tags.Single(t => t.Key == "exception.type").Value);
    }

    [Fact]
    public void Failed_check_span_never_carries_sampled_row_values()
    {
        using var span = _source.StartActivity("node.Check")!;
        var message = "check not_null failed on staging.orders: 2 violation(s); sample: {id=17; email=jane@example.com}";
        var error = new PzError("PZ0510", message, null, null, null);

        NodeSpanTags.Apply(span, Result(NodeStatus.Failed, error));

        Assert.Equal("check not_null failed on staging.orders: 2 violation(s)", span.StatusDescription);
        var ex = Assert.Single(span.Events);
        var exceptionMessage = (string)ex.Tags.Single(t => t.Key == "exception.message").Value!;
        Assert.DoesNotContain("jane@example.com", exceptionMessage);
        Assert.DoesNotContain("sample", exceptionMessage);
    }

    [Fact]
    public void Description_is_capped_at_200_chars()
    {
        Assert.Equal(200, NodeSpanTags.Describe(new string('a', 500)).Length);
    }
}
