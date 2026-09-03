using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.MotherDuck.Tests;

/// <summary>The planner half of the native-only contract against the REAL connector: both directions
/// plan onto the native tier for every write mode, and engine.force_universal collides with both
/// markers as PZ0312. Unlike duckdb/ducklake, motherduck's connection is a `database` + `token`
/// pair -- no local file, so no fixture is needed and there is no missing-file refusal to test.</summary>
public sealed class MotherDuckPlannerTests
{
    private static Dictionary<string, object?> Connection() => new() { ["database"] = "lake", ["token"] = "SECRET-TOKEN" };

    private static CompiledDag MotherDuckToMotherDuckDag(string mode = "replace", string outputName = "events_out")
    {
        var sourceDef = new ConnectionDef("appdb", "motherduck", Connection(),
            [new DatasetDef("events", new Dictionary<string, object?>(), null)], "connections.yml");
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

        var sinkDef = new ConnectionDef("mart", "motherduck", Connection(), [], "connections.yml")
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
        var motherduck = new MotherDuckConnector();
        registry.AddSource("motherduck", motherduck);
        registry.AddSink("motherduck", motherduck);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Theory]
    [InlineData("append", "motherduck insert")]
    [InlineData("replace", "motherduck create-or-replace")]
    [InlineData("merge", "motherduck merge")]
    public async Task Both_directions_plan_onto_the_native_tier(string mode, string mechanism)
    {
        var plan = await new ExecutionPlanner(Registry())
            .PlanAsync(MotherDuckToMotherDuckDag(mode), forceUniversal: false, CancellationToken.None);

        var load = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, load.Strategy);
        Assert.Contains("motherduck attach", load.Reason, StringComparison.Ordinal);

        var write = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, write.Strategy);
        Assert.Contains(mechanism, write.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Force_universal_is_PZ0312_in_both_directions()
    {
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(MotherDuckToMotherDuckDag(), forceUniversal: true, CancellationToken.None));

        var refusals = ex.Errors.Where(e => e.Code == PzErrorCode.NativePathRequired).ToArray();
        Assert.Equal(2, refusals.Length);
        Assert.Contains(refusals, e => e.Message.Contains("source 'appdb'", StringComparison.Ordinal));
        Assert.Contains(refusals, e => e.Message.Contains("mart", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_three_part_output_entity_is_a_coded_config_error_not_an_unhandled_exception()
    {
        // MotherDuckSql.SplitEntity only recognizes "table" or "schema.table"; "a.b.c" throws
        // PzConnectorException out of TryGetNativeCopy. The planner must catch it (mirroring the
        // source-side PZ0353 catch) and aggregate it rather than let it escape as an unhandled crash.
        var ex = await Assert.ThrowsAsync<PzValidationException>(
            () => new ExecutionPlanner(Registry())
                .PlanAsync(MotherDuckToMotherDuckDag(outputName: "a.b.c"), forceUniversal: false, CancellationToken.None));

        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.NativePathContractMismatch, error.Code);
        Assert.Contains("mart", error.Message, StringComparison.Ordinal);
        Assert.Contains("a.b.c", error.Message, StringComparison.Ordinal);
        Assert.Equal("connections.yml", error.File);
    }

    [Fact]
    public async Task Plan_reasons_never_carry_the_token()
    {
        var plan = await new ExecutionPlanner(Registry())
            .PlanAsync(MotherDuckToMotherDuckDag(), forceUniversal: false, CancellationToken.None);
        Assert.All(plan.Nodes, n => Assert.DoesNotContain("SECRET-TOKEN", n.Reason, StringComparison.Ordinal));
    }
}
