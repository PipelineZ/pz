using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Dispatch;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>A connector's native-path refusal (PZ0353) fails the plan only for a node the run will
/// execute. Given the run's effective set, a refusal on a node outside it is recorded in that node's
/// reason (never carrying the connector's message, which may name a path) and planning succeeds —
/// which is what lets the flow that writes a file run before the same project's flow that reads it.
/// Every other plan-time gate stays selection-blind.</summary>
public sealed class DeferredRefusalPlanningTests
{
    private static readonly NodeId LoadId = new("1111111111111111");
    private static readonly NodeId PipelineId = new("2222222222222222");
    private static readonly NodeId SinkId = new("3333333333333333");

    [Fact]
    public async Task Refusal_on_an_executing_source_is_PZ0353()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubRefusingNativeSource(), new StubUniversalSink());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId> { LoadId, PipelineId, SinkId }));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.NativePathContractMismatch, error.Code);
    }

    [Fact]
    public async Task Refusal_with_no_effective_set_is_PZ0353()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubRefusingNativeSource(), new StubUniversalSink());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None));

        Assert.Equal(PzErrorCode.NativePathContractMismatch, Assert.Single(ex.Errors).Code);
    }

    [Fact]
    public async Task Refusal_on_a_source_outside_the_effective_set_is_recorded_not_raised()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubRefusingNativeSource(), new StubUniversalSink());

        var plan = await new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId>());

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.ArrowStream, load.Strategy);
        Assert.Equal(
            "arrow stream: connector 'stub' native path refused; PZ0353 deferred because this node is not part of the run (read=full)",
            load.Reason);
        Assert.DoesNotContain("/secret/path", load.Reason);
    }

    [Fact]
    public async Task Refusal_on_a_sink_outside_the_effective_set_is_recorded_not_raised()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubRefusingNativeSink());

        var plan = await new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId> { LoadId });

        var sink = plan.Nodes.Single(n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(
            "arrow stream: connector 'stub' native path refused; PZ0353 deferred because this node is not part of the run",
            sink.Reason);
        Assert.Equal(EdgeStrategy.NativeScan, plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad).Strategy);
    }

    [Fact]
    public async Task Refusal_on_a_sink_inside_the_effective_set_is_PZ0353()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(), new StubRefusingNativeSink());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId> { SinkId }));

        Assert.Equal(PzErrorCode.NativePathContractMismatch, Assert.Single(ex.Errors).Code);
    }

    // The effective set is the selection plus every ancestor: selecting the sink alone still executes
    // the load, so its refusal is fatal — a deferred refusal is never one the run would then hit.
    [Fact]
    public async Task Effective_set_includes_ancestors_so_selecting_the_sink_still_refuses_the_load()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubRefusingNativeSource(), new StubUniversalSink());
        var executing = RunOrchestrator.EffectiveNodeIds(dag, new HashSet<NodeId> { SinkId });

        Assert.Equal(new HashSet<NodeId> { LoadId, PipelineId, SinkId }, executing);
        var ex = await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: executing));
        Assert.Equal(PzErrorCode.NativePathContractMismatch, Assert.Single(ex.Errors).Code);
        Assert.Null(RunOrchestrator.EffectiveNodeIds(dag, null));
    }
}
