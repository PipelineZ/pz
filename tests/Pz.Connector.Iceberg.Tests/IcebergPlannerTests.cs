using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>The planner half of the native-only contract against the REAL connector: both directions
/// plan onto the native tier for every write mode, engine.force_universal collides with both markers
/// as PZ0312, and a connector refusal out of the native probe (a bare entity, a files-catalog write)
/// is a coded config error rather than an unhandled exception.</summary>
public sealed class IcebergPlannerTests
{
    private const string Token = "T0K3N";

    private static Dictionary<string, object?> Connection(string catalog = "rest") => catalog == "files"
        ? new() { ["catalog"] = "files", ["root"] = "s3://warehouse/" }
        : new() { ["catalog"] = "rest", ["endpoint"] = "http://127.0.0.1:1", ["warehouse"] = "wh", ["token"] = Token };

    private static CompiledDag Dag(string mode = "replace", string outputName = "mart.events_out", string sinkCatalog = "rest")
    {
        var sourceDef = new ConnectionDef("appdb", "iceberg", Connection(),
            [new DatasetDef("raw.events", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_appdb__events",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_events", "select * from staging.src_appdb__events",
            "table", [], [], "pipelines/stg_events.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_events",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var output = new OutputDef(outputName, "stg_events", mode, "fail_on_change",
            new Dictionary<string, object?>());
        if (mode == "merge")
        {
            output = output with { Keys = ["id"] };
        }

        var sinkDef = new ConnectionDef("mart", "iceberg", Connection(sinkCatalog), [], "connections.yml")
        {
            Outputs = [output],
        };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "mart.events_out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return new CompiledDag([loadNode, pipelineNode, sinkNode]);
    }

    private static ConnectorRegistry Registry()
    {
        var registry = new ConnectorRegistry();
        var iceberg = new IcebergConnector();
        registry.AddSource("iceberg", iceberg);
        registry.AddSink("iceberg", iceberg);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Theory]
    [InlineData("append", "iceberg insert")]
    [InlineData("replace", "iceberg overwrite")]
    [InlineData("merge", "iceberg merge")]
    public async Task Both_directions_plan_onto_the_native_tier(string mode, string mechanism)
    {
        var plan = await new ExecutionPlanner(Registry())
            .PlanAsync(Dag(mode), forceUniversal: false, CancellationToken.None);

        var load = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Contains("iceberg attach", load.Reason, StringComparison.Ordinal);

        var write = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, write.Strategy);
        Assert.Contains(mechanism, write.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Force_universal_is_PZ0312_in_both_directions()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(Dag(), forceUniversal: true, CancellationToken.None));

        var refusals = ex.Errors.Where(e => e.Code == PzErrorCode.NativePathRequired).ToArray();
        Assert.Equal(2, refusals.Length);
        Assert.Contains(refusals, e => e.Message.Contains("source 'appdb'", StringComparison.Ordinal));
        Assert.Contains(refusals, e => e.Message.Contains("mart", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_bare_output_entity_is_a_coded_config_error_not_an_unhandled_exception()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(Dag(outputName: "events_out"), forceUniversal: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.NativePathContractMismatch, error.Code);
        Assert.Contains("mart", error.Message, StringComparison.Ordinal);
        Assert.Contains("namespace.table", error.Message, StringComparison.Ordinal);
        Assert.Equal("connections.yml", error.File);
    }

    [Fact]
    public async Task A_write_to_a_files_catalog_is_a_coded_config_error()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(Dag(sinkCatalog: "files"), forceUniversal: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.NativePathContractMismatch, error.Code);
        Assert.Contains("read-only", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Plan_reasons_never_carry_the_token()
    {
        var plan = await new ExecutionPlanner(Registry())
            .PlanAsync(Dag(), forceUniversal: false, CancellationToken.None);
        Assert.All(plan.Nodes, n => Assert.DoesNotContain(Token, n.Reason, StringComparison.Ordinal));
    }
}
