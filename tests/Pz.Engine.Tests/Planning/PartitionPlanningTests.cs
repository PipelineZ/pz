using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>Capability-combination gate (PZ0319), mirroring <see cref="SyncPlanningTests"/>:
/// <see cref="ConnectorCapabilities.CheckpointableReads"/> without
/// <see cref="ConnectorCapabilities.StablePartitionIds"/> is a connector defect (checkpoints are
/// keyed by stable partition identity), refused at plan time, static Capabilities read only.</summary>
public sealed class PartitionPlanningTests
{
    [Fact]
    public async Task Checkpointable_without_stable_ids_is_refused_with_PZ0319()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.CheckpointableReads);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.PartitionIdentityInvalid);
    }

    [Fact]
    public async Task Checkpointable_with_stable_ids_plans_cleanly()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset,
            ConnectorCapabilities.StablePartitionIds | ConnectorCapabilities.CheckpointableReads);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }
}
