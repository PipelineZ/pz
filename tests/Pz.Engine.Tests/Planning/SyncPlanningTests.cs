using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>The PZ0316 capability gate: mirrors the PZ0313/PZ0314 tests in
/// <see cref="ExecutionPlannerTests"/> -- a dataset whose read shape RESOLVES to Feed
/// paired with a connector that declares
/// <see cref="ConnectorCapabilities.PartitionedRead"/> must be refused at plan time, because a single
/// opaque sync token cannot reconcile across independent partition reads. Uses
/// <see cref="StubFeedSource"/> (resolves Feed for every dataset) rather than the retired
/// `SyncMode.Auto`-as-feed bridge -- <see cref="ReadShapeResolutionTests"/> covers the resolver itself
/// end-to-end; these tests are the PZ0316-specific regression net.</summary>
public sealed class SyncPlanningTests
{
    [Fact]
    public async Task Feed_dataset_on_partitionedread_connector_is_PZ0316()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(
            dataset, new StubFeedSource(ConnectorCapabilities.PartitionedRead));

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncPartitionedReadConflict);
        Assert.Contains("orders", error.ToString());
        Assert.Contains("stub", error.ToString());
    }

    [Fact]
    public async Task Feed_dataset_on_streamingpartitions_connector_is_PZ0316()
    {
        // A connector declaring StreamingPartitions | SyncState streams N partitions with no
        // materialized list for the runtime guard to inspect -- the plan-time PZ0316 gate must
        // refuse this combination too, not just the materialized PartitionedRead case, or the
        // dataset silently never advances (the runtime guard is null on the streaming path).
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(
            dataset, new StubFeedSource(ConnectorCapabilities.StreamingPartitions | ConnectorCapabilities.SyncState));

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncPartitionedReadConflict);
        Assert.Contains("orders", error.ToString());
        Assert.Contains("stub", error.ToString());
    }

    [Fact]
    public async Task Feed_dataset_on_non_partitionedread_connector_plans_clean()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        // Feed-compatible sink: the default replace output would trip the feed x replace refusal (PZ0335).
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(
            dataset, new StubFeedSource(ConnectorCapabilities.None), TestDags.FeedCompatibleOutput());

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    [Fact]
    public async Task Full_shaped_dataset_on_partitionedread_connector_is_not_PZ0316()
    {
        // Implicit `mode: auto` (SyncMode null) on a connector that does NOT implement
        // INaturalReadShapeSource resolves Full -- exactly the "connector's natural read shape is a
        // plain re-read" case the retired SyncMode.Auto bridge could never distinguish from Feed.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.PartitionedRead);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }

    // A cursor-incremental declaration on a connector that
    // manages its OWN change feed for the dataset (INaturalReadShapeSource resolves Feed) pits two resume
    // mechanisms against each other -- refused at plan time with PZ0315 (SyncStateConflict). Covers both
    // the YAML `sync: {mode: incremental}` and SQL-synthesized watermark() paths (both resolve Incremental).
    [Fact]
    public async Task Incremental_declared_on_feed_natural_connector_is_SyncStateConflict()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, new StubFeedSource(ConnectorCapabilities.None));

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SyncStateConflict);
        Assert.Contains("orders", error.Message);
        Assert.Contains("own change feed", error.Message);
    }

    [Fact]
    public async Task Incremental_declared_on_full_natural_connector_plans_clean()
    {
        // Same declared-incremental dataset, but the connector does NOT implement INaturalReadShapeSource
        // (resolves Full) -- the ordered cursor is the only resume mechanism, so no conflict.
        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp" },
            new SyncModeDef(SyncMode.Incremental, new IncrementalDef("updated_at")));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
    }
}
