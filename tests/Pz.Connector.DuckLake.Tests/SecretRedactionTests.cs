using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>Credentials ride secrets or a SET statement, never the attach string: none of the
/// postgres password, quack token, motherduck token or storage secret may reach a
/// <see cref="PlannedNode.Reason"/>, plan.json, or a <see cref="NativeSetup"/> failure message —
/// even when the carrier statement itself is malformed and DuckDB's parser echoes it.</summary>
public sealed class SecretRedactionTests
{
    private const string PgPassword = "PG_S3CRET";
    private const string StorageSecret = "ST0RAGE_S3CRET";
    private const string QuackToken = "QU4CK_T0KEN";
    private const string MdToken = "M0THER_T0KEN";

    private static Dictionary<string, object?> PostgresConnection() => new()
    {
        ["catalog"] = "postgres", ["host"] = "pg.example.com", ["database"] = "lake",
        ["user"] = "pz", ["password"] = PgPassword, ["data_path"] = "s3://bucket/lake/",
        ["storage_key_id"] = "AK", ["storage_secret_key"] = StorageSecret,
    };

    private static CompiledDag Dag(Dictionary<string, object?> connection)
    {
        var sourceDef = new ConnectionDef("wh", "ducklake", connection,
            [new DatasetDef("orders", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_wh__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));
        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_wh__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);
        var sinkDef = new ConnectionDef("mart", "ducklake", connection, [], "connections.yml")
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
        var ducklake = new DuckLakeConnector();
        registry.AddSource("ducklake", ducklake);
        registry.AddSink("ducklake", ducklake);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Fact]
    public async Task Plan_reason_and_plan_json_never_contain_a_credential()
    {
        var plan = await new ExecutionPlanner(Registry()).PlanAsync(Dag(PostgresConnection()), forceUniversal: false, CancellationToken.None);
        Assert.All(plan.Nodes, n =>
        {
            Assert.DoesNotContain(PgPassword, n.Reason, StringComparison.Ordinal);
            Assert.DoesNotContain(StorageSecret, n.Reason, StringComparison.Ordinal);
        });

        var dir = Path.Combine(Path.GetTempPath(), "pz-ducklake-redaction-tests", Guid.NewGuid().ToString("N"));
        try
        {
            PlanWriter.Write(plan, dir);
            var json = await File.ReadAllTextAsync(Path.Combine(dir, "plan.json"));
            Assert.DoesNotContain(PgPassword, json, StringComparison.Ordinal);
            Assert.DoesNotContain(StorageSecret, json, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [InlineData("postgres")]
    [InlineData("quack")]
    [InlineData("motherduck")]
    public async Task A_malformed_credential_carrier_never_leaks_into_the_setup_failure(string catalog)
    {
        var connection = catalog switch
        {
            "postgres" => PostgresConnection(),
            "quack" => new Dictionary<string, object?> { ["catalog"] = "quack", ["uri"] = "quack:lake.internal", ["token"] = QuackToken, ["data_path"] = "s3://b/" },
            _ => new Dictionary<string, object?> { ["catalog"] = "motherduck", ["database"] = "lake", ["token"] = MdToken, ["data_path"] = "s3://b/" },
        };
        var secret = catalog switch { "postgres" => PgPassword, "quack" => QuackToken, _ => MdToken };

        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), out var scan));

        // Sanity: the attach (last statement) never carries the credential; exactly one earlier statement does.
        Assert.DoesNotContain(secret, scan!.SetupStatements[^1], StringComparison.Ordinal);
        var carrier = Assert.Single(scan.SetupStatements, s => s.Contains(secret, StringComparison.Ordinal));

        // Break the carrier's grammar (drop the "(" after "secret <name>" / the "=" in SET) so DuckDB's
        // parser rejects it with a LINE-context block echoing the statement verbatim. Deterministic and
        // offline: the parser rejects before any extension type is resolved.
        var broken = carrier.StartsWith("set ", StringComparison.Ordinal)
            ? carrier.Replace(" = ", " ", StringComparison.Ordinal)
            : carrier.Replace(" (type ", " type ", StringComparison.Ordinal);
        Assert.NotEqual(carrier, broken);

        var dir = Path.Combine(Path.GetTempPath(), "pz-ducklake-redaction-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                () => NativeSetup.ExecuteSetupAsync(duck, broken, CancellationToken.None));
            Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
