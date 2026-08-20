using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>Plan-time cdc capability gates -- PZ0338 (a
/// `sync: {mode: cdc}` dataset on a connector without ConnectorCapabilities.ChangeCapture) and PZ0339
/// (an `on_delete: delete|soft` output on a sink without ConnectorCapabilities.ApplyDeletes). Both
/// gates mirror the existing static-capability-read, never-connects shape of the BoundedWindow
/// (PZ0313) and mode (PZ0324) gates they sit next to.</summary>
public sealed class CdcPlanningTests
{
    [Fact]
    public async Task Cdc_dataset_on_incapable_source_is_PZ0338()
    {
        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null,
            new SyncModeDef(SyncMode.Cdc, null));
        var (dag, registry) = TestDags.DagAndRegistryWithStubSource(dataset, ConnectorCapabilities.None);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.ChangeCaptureUnsupported);
        Assert.Contains("stub", error.ToString());
        Assert.Contains("orders", error.ToString());
        Assert.Contains("use a ChangeCapture-capable connector (postgres, sqlserver) or a different sync mode",
            error.ToString());
    }

    [Fact]
    public async Task Cdc_dataset_on_capable_source_plans_clean_with_no_connection_leak()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubConfigurableCapabilitiesSource(ConnectorCapabilities.ChangeCapture));
        registry.AddSink("stub", new StubUniversalSink());

        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null,
            new SyncModeDef(SyncMode.Cdc, null));
        var sourceDef = new ConnectionDef("stub", "stub",
            new Dictionary<string, object?> { ["password"] = "SECRET_MARKER" }, [dataset], "sources/stub.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_stub__orders",
            [], null, new SourceDatasetDef(sourceDef, dataset));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_stub__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var output = new OutputDef("out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>());
        var sinkDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml") { Outputs = [output] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "stub.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, output));

        var dag = new CompiledDag([loadNode, pipelineNode, sinkNode]);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        var load = plan.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Contains("read=cdc", load.Reason);
        Assert.All(plan.Nodes, n => Assert.DoesNotContain("SECRET_MARKER", n.Reason));
    }

    [Theory]
    [InlineData("delete")]
    [InlineData("soft")]
    public async Task OnDelete_on_incapable_sink_is_PZ0339(string onDelete)
    {
        var output = new OutputDef("out", "stg_orders", "merge", "fail_on_change",
            new Dictionary<string, object?>(), ["id"], OnDelete: onDelete);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(output, ConnectorCapabilities.Merge);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.DeleteApplyUnsupported);
        Assert.Contains("stub", error.ToString());
        Assert.Contains("out", error.ToString());
        Assert.Contains($"on_delete: {onDelete}", error.ToString());
        Assert.Contains("use on_delete: ignore, or an ApplyDeletes-capable sink (postgres, sqlserver)",
            error.ToString());
    }

    [Fact]
    public async Task OnDelete_ignore_plans_clean_without_capability()
    {
        var output = new OutputDef("out", "stg_orders", "merge", "fail_on_change",
            new Dictionary<string, object?>(), ["id"], OnDelete: "ignore");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSink(output, ConnectorCapabilities.Merge);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default);

        Assert.Contains(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    [Fact]
    public async Task PZ0338_deduped_across_datasets_on_one_instance()
    {
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceSyncModes(
            ConnectorCapabilities.None, new SyncModeDef(SyncMode.Cdc, null), "orders", "returns", "refunds");

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, default));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.ChangeCaptureUnsupported);
    }
}
