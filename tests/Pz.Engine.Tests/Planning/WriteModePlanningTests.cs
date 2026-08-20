using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>PZ0324: a KNOWN mode the target connector does not
/// declare support for is refused at plan time — merge needs Merge, replace needs
/// ReplaceWrites, append is the universal floor. Static Capabilities read, aggregated,
/// deduped per (sink instance, mode), mirroring PZ0317/PZ0319.</summary>
public sealed class WriteModePlanningTests
{
    private static OutputDef Output(string mode, IReadOnlyList<string>? keys = null) =>
        new("out", "stg_orders", mode, "fail_on_change", new Dictionary<string, object?>(), keys ?? []);

    [Fact]
    public async Task Merge_without_Merge_capability_is_refused_with_PZ0324()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            Output("merge", ["id"]), ConnectorCapabilities.None);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WriteModeUnsupported);
        Assert.Contains("merge", error.Message);
    }

    [Fact]
    public async Task Replace_without_ReplaceWrites_is_refused_with_PZ0324()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            Output("replace"), ConnectorCapabilities.Merge);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WriteModeUnsupported);
    }

    [Fact]
    public async Task Append_needs_no_capability()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            Output("append"), ConnectorCapabilities.None);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.NotNull(plan);
    }

    [Fact]
    public async Task Supported_modes_plan_cleanly()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(
            Output("replace"), ConnectorCapabilities.ReplaceWrites);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.NotNull(plan);
    }
}
