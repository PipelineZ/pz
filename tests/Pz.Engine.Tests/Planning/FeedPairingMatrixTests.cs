using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>The planner's half of the (read x write) pairing
/// matrix -- the feed row. An implicit/`mode: auto` dataset that a connector resolves to Feed
/// (<see cref="StubFeedSource"/>) is DagCompiler-invisible (no connector-capability access there), so
/// ExecutionPlanner alone enforces its cells: `replace` refused (PZ0335), `append` without consent
/// refused (PZ0214), `merge` and consented `append` accepted. The compile-time mirror for the Incremental
/// row lives in PairingMatrixTests.</summary>
public sealed class FeedPairingMatrixTests
{
    private static OutputDef Output(string mode, bool acceptDuplicates = false, params string[] keys) =>
        new("out", "stg_orders", mode, "fail_on_change", new Dictionary<string, object?>(), keys,
            AcceptDuplicates: acceptDuplicates);

    // (F1) feed -> replace: refused with PZ0335.
    [Fact]
    public async Task Feed_dataset_feeding_replace_output_is_PZ0335()
    {
        var (dag, registry) = TestDags.FeedSourceToSink(Output("replace"), ConnectorCapabilities.ReplaceWrites);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair);
        Assert.Contains("stub.out", error.Message);
        Assert.Contains("stub.orders", error.Message);
        Assert.Contains("complete snapshot", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("write.strategy: merge", error.Hint);
    }

    // (F1) feed -> merge: accepted.
    [Fact]
    public async Task Feed_dataset_feeding_merge_output_plans_clean()
    {
        var (dag, registry) = TestDags.FeedSourceToSink(Output("merge", keys: "id"), ConnectorCapabilities.Merge);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    // (F2) feed -> append without consent: refused with PZ0214.
    [Fact]
    public async Task Feed_dataset_feeding_append_output_without_consent_is_PZ0214()
    {
        var (dag, registry) = TestDags.FeedSourceToSink(Output("append"), ConnectorCapabilities.None);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged);
        Assert.Contains("stub.out", error.Message);
        Assert.Contains("stub.orders", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("write:\n  strategy: append\n  duplicates: accept", error.Hint);
    }

    // (F2) feed -> append with duplicates: accept: accepted.
    [Fact]
    public async Task Feed_dataset_feeding_append_output_with_consent_plans_clean()
    {
        var (dag, registry) = TestDags.FeedSourceToSink(
            Output("append", acceptDuplicates: true), ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }
}
