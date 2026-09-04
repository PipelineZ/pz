using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>Credentials ride secrets, never the attach string: none of the bearer token, the OAuth2
/// client secret or the storage secret may reach a <see cref="PlannedNode.Reason"/>, plan.json, or a
/// <see cref="NativeSetup"/> failure message — even when the carrier statement itself is malformed
/// and DuckDB's parser echoes it.</summary>
public sealed class SecretRedactionTests
{
    private const string Token = "B3AR3R_T0KEN";
    private const string ClientSecret = "CL13NT_S3CRET";
    private const string StorageSecret = "ST0RAGE_S3CRET";

    private static Dictionary<string, object?> TokenConnection() => new()
    {
        ["catalog"] = "rest", ["endpoint"] = "https://cat.example.com", ["warehouse"] = "wh",
        ["token"] = Token, ["storage_key_id"] = "AK", ["storage_secret_key"] = StorageSecret,
    };

    private static Dictionary<string, object?> OAuthConnection() => new()
    {
        ["catalog"] = "rest", ["endpoint"] = "https://cat.example.com", ["warehouse"] = "wh",
        ["client_id"] = "pz", ["client_secret"] = ClientSecret,
    };

    private static CompiledDag Dag(Dictionary<string, object?> connection)
    {
        var sourceDef = new ConnectionDef("wh", "iceberg", connection,
            [new DatasetDef("raw.orders", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_wh__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));
        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_wh__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);
        var sinkDef = new ConnectionDef("mart", "iceberg", connection, [], "connections.yml")
        {
            Outputs = [new OutputDef("mart.orders_out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>())],
        };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "mart.orders_out",
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

    [Fact]
    public async Task Plan_reason_and_plan_json_never_contain_a_credential()
    {
        var plan = await new ExecutionPlanner(Registry()).PlanAsync(Dag(TokenConnection()), forceUniversal: false, CancellationToken.None);
        Assert.All(plan.Nodes, n =>
        {
            Assert.DoesNotContain(Token, n.Reason, StringComparison.Ordinal);
            Assert.DoesNotContain(StorageSecret, n.Reason, StringComparison.Ordinal);
        });

        var dir = Path.Combine(Path.GetTempPath(), "pz-iceberg-redaction-tests", Guid.NewGuid().ToString("N"));
        try
        {
            PlanWriter.Write(plan, dir);
            var json = await File.ReadAllTextAsync(Path.Combine(dir, "plan.json"));
            Assert.DoesNotContain(Token, json, StringComparison.Ordinal);
            Assert.DoesNotContain(StorageSecret, json, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Theory]
    [InlineData("token")]
    [InlineData("client_secret")]
    [InlineData("storage")]
    public async Task A_malformed_credential_carrier_never_leaks_into_the_setup_failure(string kind)
    {
        var connection = kind == "client_secret" ? OAuthConnection() : TokenConnection();
        var secret = kind switch { "token" => Token, "client_secret" => ClientSecret, _ => StorageSecret };

        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "raw.orders", new Dictionary<string, object?>()), out var scan));

        // Sanity: the attach (last statement) never carries the credential; exactly one earlier statement does.
        Assert.DoesNotContain(secret, scan!.SetupStatements[^1], StringComparison.Ordinal);
        var carrier = Assert.Single(scan.SetupStatements, s => s.Contains(secret, StringComparison.Ordinal));

        // Break the carrier's grammar (drop the "(" after "secret <name>") so DuckDB's parser rejects it
        // with a LINE-context block echoing the statement verbatim. Deterministic and offline: the
        // parser rejects before any extension type is resolved.
        var broken = carrier.Replace(" (type ", " type ", StringComparison.Ordinal);
        Assert.NotEqual(carrier, broken);

        var dir = Path.Combine(Path.GetTempPath(), "pz-iceberg-redaction-tests", Guid.NewGuid().ToString("N"));
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
