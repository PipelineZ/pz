using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.Sqlite.Tests;

/// <summary>The planner half of the native-only contract against the REAL connector (the planner's
/// probes never open the file, so no fixture is needed): both directions plan onto the native tier,
/// engine.force_universal collides with both markers as PZ0312 at plan time, and a merge output is
/// the planner's PZ0324 (sqlite declares no Merge capability).</summary>
public sealed class SqlitePlannerTests
{
    private static Dictionary<string, object?> Connection() => new() { ["path"] = "/data/app.db" };

    private static CompiledDag SqliteToSqliteDag(string mode = "replace")
    {
        var sourceDef = new ConnectionDef("appdb", "sqlite", Connection(),
            [new DatasetDef("events", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_appdb__events",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_events", "select * from staging.src_appdb__events",
            "table", [], [], "pipelines/stg_events.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_events",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var output = new OutputDef("events_out", "stg_events", mode, "fail_on_change",
            new Dictionary<string, object?>());
        if (mode == "merge")
        {
            output = output with { Keys = ["id"] };
        }

        var sinkDef = new ConnectionDef("mart", "sqlite", Connection(), [], "connections.yml")
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
        var sqlite = new SqliteConnector();
        registry.AddSource("sqlite", sqlite);
        registry.AddSink("sqlite", sqlite);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Fact]
    public async Task Both_directions_plan_onto_the_native_tier()
    {
        var plan = await new ExecutionPlanner(Registry())
            .PlanAsync(SqliteToSqliteDag(), forceUniversal: false, CancellationToken.None);

        var load = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Contains("sqlite_scan", load.Reason, StringComparison.Ordinal);

        var write = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, write.Strategy);
        Assert.Contains("sqlite", write.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Force_universal_is_PZ0312_in_both_directions()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(SqliteToSqliteDag(), forceUniversal: true, CancellationToken.None));

        var refusals = ex.Errors.Where(e => e.Code == PzErrorCode.NativePathRequired).ToArray();
        Assert.Equal(2, refusals.Length);
        Assert.Contains(refusals, e => e.Message.Contains("source 'appdb'", StringComparison.Ordinal));
        Assert.Contains(refusals, e => e.Message.Contains("mart", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Merge_output_is_the_planners_PZ0324()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(SqliteToSqliteDag(mode: "merge"), forceUniversal: false, CancellationToken.None));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.WriteModeUnsupported);
    }
}
