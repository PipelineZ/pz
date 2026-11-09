using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>A connector refusing a dataset's config while the planner probes it (opening the connection,
/// resolving the read shape) is a config error: PZ0301, aggregated and naming the connection and dataset -- never
/// an exception escaping the plan, which the CLI renders as PZ0500 "a bug in pz".</summary>
public sealed class ConfigRefusalPlanningTests
{
    [Fact]
    public async Task A_config_refusal_while_planning_a_source_is_PZ0301()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubConfigRefusingSource(), new StubUniversalSink());

        var ex = await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'stub' dataset 'orders': dataset 'orders': option 'x' clashes with option 'y'", error.Message);
        Assert.Equal("sources/stub.yml", error.File);
        Assert.NotNull(error.Hint);
    }

    [Fact]
    public async Task A_config_refusal_outside_the_effective_set_is_recorded_not_raised()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubConfigRefusingSource(), new StubUniversalSink());

        var plan = await new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId>());

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(
            "arrow stream: connector 'stub' refused the config; PZ0301 deferred because this node is not part of the run",
            load.Reason);
    }

    [Fact]
    public async Task A_transient_connector_failure_is_not_classified_as_config()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubConfigRefusingSource(transient: true), new StubUniversalSink());

        await Assert.ThrowsAsync<PzConnectorException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None));
    }
}
