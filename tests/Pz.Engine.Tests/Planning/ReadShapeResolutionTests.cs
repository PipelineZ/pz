using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary><see cref="ReadShapeResolver"/> is the single source of an `auto` dataset's read shape:
/// it comes from the opened connector's <see cref="INaturalReadShapeSource"/> (or Full when the
/// connector doesn't implement one), exercised end-to-end through <see cref="ExecutionPlanner"/> since
/// that's where the resolved shape becomes both a Reason-string token and PZ0316's predicate.</summary>
public sealed class ReadShapeResolutionTests
{
    [Fact]
    public async Task Auto_dataset_on_plain_stub_source_resolves_full()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Contains("read=full", load.Reason);
    }

    [Fact]
    public async Task Auto_dataset_on_feed_resolving_stub_resolves_feed()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        // Feed-compatible sink: the default replace output would trip the feed x replace refusal (PZ0335).
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(
            dataset, new StubFeedSource(ConnectorCapabilities.None), TestDags.FeedCompatibleOutput());

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Contains("read=feed", load.Reason);
    }

    [Fact]
    public async Task Declared_incremental_resolves_incremental_with_cursor_in_reason()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Contains("read=incremental cursor=updated_at", load.Reason);
    }

    [Fact]
    public async Task Feed_resolving_dataset_on_partitionedread_connector_is_still_PZ0316()
    {
        // Ports Sync_dataset_on_partitionedread_connector_is_PZ0316 (SyncPlanningTests) to the resolved-
        // shape surface: PZ0316's predicate is now `shape == ResolvedReadShape.Feed`, driven by the
        // connector's INaturalReadShapeSource rather than the retired SyncMode.Auto bridge.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(
            dataset, new StubFeedSource(ConnectorCapabilities.PartitionedRead));

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncPartitionedReadConflict);
        Assert.Contains("orders", error.ToString());
        Assert.Contains("stub", error.ToString());
    }
}
