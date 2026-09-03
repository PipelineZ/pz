using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.Quack.Tests;

/// <summary>The token rides a quack secret, never the attach string: it may not reach a
/// <see cref="PlannedNode.Reason"/>, plan.json, or a <see cref="NativeSetup"/> failure message --
/// even when the carrier statement itself is malformed and DuckDB's parser echoes it.</summary>
public sealed class SecretRedactionTests
{
    private const string QuackToken = "QU4CK_T0KEN";

    private static Dictionary<string, object?> Connection() => new() { ["uri"] = "quack:lake.internal:9494", ["token"] = QuackToken };

    private static CompiledDag Dag()
    {
        var connection = Connection();
        var sourceDef = new ConnectionDef("wh", "quack", connection,
            [new DatasetDef("orders", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_wh__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));
        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_wh__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);
        var sinkDef = new ConnectionDef("mart", "quack", connection, [], "connections.yml")
        {
            Outputs = [new OutputDef("orders_out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>())],
        };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "mart.orders_out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));
        return new CompiledDag([loadNode, pipelineNode, sinkNode]);
    }

    private static ConnectorRegistry Registry()
    {
        var registry = new ConnectorRegistry();
        var quack = new QuackConnector();
        registry.AddSource("quack", quack);
        registry.AddSink("quack", quack);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Fact]
    public async Task Plan_reason_and_plan_json_never_contain_the_token()
    {
        var plan = await new ExecutionPlanner(Registry()).PlanAsync(Dag(), forceUniversal: false, CancellationToken.None);
        Assert.All(plan.Nodes, n => Assert.DoesNotContain(QuackToken, n.Reason, StringComparison.Ordinal));

        var dir = Path.Combine(Path.GetTempPath(), "pz-quack-redaction-tests", Guid.NewGuid().ToString("N"));
        try
        {
            PlanWriter.Write(plan, dir);
            var json = await File.ReadAllTextAsync(Path.Combine(dir, "plan.json"));
            Assert.DoesNotContain(QuackToken, json, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task A_malformed_credential_carrier_never_leaks_into_the_setup_failure()
    {
        await using var source = await ((ISourceConnector)new QuackConnector()).OpenAsync(new ConnectorConfig(Connection()), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), out var scan));

        // Sanity: the attach (last statement) never carries the token; exactly one earlier statement does.
        Assert.DoesNotContain(QuackToken, scan!.SetupStatements[^1], StringComparison.Ordinal);
        var carrier = Assert.Single(scan.SetupStatements, s => s.Contains(QuackToken, StringComparison.Ordinal));

        // Break the carrier's grammar (drop the "(" after "secret <name>") so DuckDB's parser rejects
        // it with a LINE-context block echoing the statement verbatim, before the quack extension's
        // secret type is ever resolved -- deterministic and offline, so no PZ_TESTS_OFFLINE gate.
        var broken = carrier.Replace(" (type ", " type ", StringComparison.Ordinal);
        Assert.NotEqual(carrier, broken);

        var dir = Path.Combine(Path.GetTempPath(), "pz-quack-redaction-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                () => NativeSetup.ExecuteSetupAsync(duck, broken, CancellationToken.None));
            Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(QuackToken, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
