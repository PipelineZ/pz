using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.S3.Tests;

/// <summary><c>Pz.Engine.Tests.Planning.ExecutionPlannerTests</c> proves the
/// force_universal x native-only collision (PZ0312) only against a generic stub sink. This exercises the
/// exact same collision against the REAL <see cref="S3Connector"/> registered under "s3"
/// -- no container needed, since the planner's INativeOnlySink probe never connects.</summary>
public sealed class S3PlannerTests
{
    [Fact]
    public async Task Force_universal_with_s3_sink_is_error_PZ0312()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new LocalFilesConnector());
        registry.AddSink("s3", new S3Connector());

        var sourceDef = new ConnectionDef("files", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" }, null)],
            "sources/files.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_files__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_files__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("lake", "s3",
            new Dictionary<string, object?> { ["access_key"] = "AKIA_TEST", ["secret_key"] = "S3CRET_VALUE" }, [],
            "connections.yml") { Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["bucket"] = "my-bucket", ["format"] = "parquet" })] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "lake.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        var dag = new CompiledDag([loadNode, pipelineNode, sinkNode]);

        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: true, CancellationToken.None));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.NativePathRequired);
        Assert.Contains("lake", error.Message, StringComparison.Ordinal);
    }

    // --- the source half of the same contract, against the REAL connector ---

    private static ConnectorRegistry SourceRegistry()
    {
        var registry = new ConnectorRegistry();
        var s3 = new S3Connector();
        registry.AddSource("s3", s3);
        registry.AddSink("s3", s3);
        return registry;
    }

    private static CompiledDag S3ToS3Dag()
    {
        var connection = new Dictionary<string, object?>
        {
            ["access_key"] = "AKIA_TEST",
            ["secret_key"] = "S3CRET_VALUE",
            ["root"] = "my-bucket/raw",
        };
        var sourceDef = new ConnectionDef("lake", "s3", connection,
            [new DatasetDef("orders", new Dictionary<string, object?> { ["format"] = "parquet" }, null)],
            "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_lake__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_lake__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("mart", "s3", connection, [], "connections.yml")
        {
            Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["format"] = "parquet" })],
        };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "mart.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return new CompiledDag([loadNode, pipelineNode, sinkNode]);
    }

    [Fact]
    public async Task Both_directions_plan_onto_the_native_tier()
    {
        var plan = await new ExecutionPlanner(SourceRegistry())
            .PlanAsync(S3ToS3Dag(), forceUniversal: false, CancellationToken.None);

        var load = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Contains("read_parquet", load.Reason, StringComparison.Ordinal);

        var write = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, write.Strategy);
    }

    [Fact]
    public async Task Force_universal_is_PZ0312_in_both_directions()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(SourceRegistry())
                .PlanAsync(S3ToS3Dag(), forceUniversal: true, CancellationToken.None));

        var refusals = ex.Errors.Where(e => e.Code == PzErrorCode.NativePathRequired).ToArray();
        Assert.Equal(2, refusals.Length);
        Assert.Contains(refusals, e => e.Message.Contains("source 'lake'", StringComparison.Ordinal));
        Assert.Contains(refusals, e => e.Message.Contains("mart", StringComparison.Ordinal));
    }

    [Fact]
    public void Connector_is_native_only_as_a_source_with_the_read_capabilities()
    {
        var c = new S3Connector();
        Assert.IsAssignableFrom<Pz.Connectors.Abstractions.ISourceConnector>(c);
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.PathTemplating,
            c.Capabilities);
    }
}
