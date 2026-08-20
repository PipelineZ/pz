using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.MySql.Tests;

/// <summary>Credentials ride a DuckDB secret, never the attach path: the password must never leak
/// into a <see cref="PlannedNode.Reason"/>, plan.json, or a <see cref="NativeSetup"/> failure message
/// even when the CREATE SECRET statement itself is malformed (a DuckDB parser error's LINE-context
/// block echoes the offending statement verbatim). The live wrong-password-connect variant (a runtime
/// IO error, not a parser error) lives in
/// <see cref="MySqlNativeEndToEndTests.A_wrong_password_connect_failure_never_leaks_the_password"/>,
/// which needs a real server.</summary>
public sealed class SecretRedactionTests
{
    private const string SecretValue = "MY5QL_S3CRET";

    private static Dictionary<string, object?> Connection() => new()
    {
        ["host"] = "db.example.com",
        ["database"] = "analytics",
        ["user"] = "pz",
        ["password"] = SecretValue,
    };

    internal static CompiledDag MySqlToMySqlDag()
    {
        var sourceDef = new ConnectionDef("wh", "mysql", Connection(),
            [new DatasetDef("orders", new Dictionary<string, object?>(), null)], "connections.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_wh__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_wh__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("mart", "mysql", Connection(), [], "connections.yml")
        {
            Outputs = [new OutputDef("orders_out", "stg_orders", "replace", "fail_on_change",
                new Dictionary<string, object?>())],
        };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "mart.orders_out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return new CompiledDag([loadNode, pipelineNode, sinkNode]);
    }

    internal static ConnectorRegistry Registry()
    {
        var registry = new ConnectorRegistry();
        var mysql = new MySqlConnector();
        registry.AddSource("mysql", mysql);
        registry.AddSink("mysql", mysql);
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        return registry;
    }

    [Fact]
    public async Task Plan_reason_and_plan_json_never_contain_the_password()
    {
        var dag = MySqlToMySqlDag();
        var plan = await new ExecutionPlanner(Registry()).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        Assert.All(plan.Nodes, n => Assert.DoesNotContain(SecretValue, n.Reason, StringComparison.Ordinal));

        var dir = Path.Combine(Path.GetTempPath(), "pz-mysql-redaction-tests", Guid.NewGuid().ToString("N"));
        try
        {
            PlanWriter.Write(plan, dir);
            var json = await File.ReadAllTextAsync(Path.Combine(dir, "plan.json"));
            Assert.DoesNotContain(SecretValue, json, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task Malformed_create_secret_statement_never_leaks_the_password()
    {
        // Credentials ride the CREATE SECRET statement (index 2), never the ATTACH path (index 3,
        // always ''). This is the proven-redacted shape -- the password is an ordinary single-quoted
        // SQL literal inside a statement DuckDB's own parser rejects with a "LINE n: ..." context
        // block that echoes it verbatim (DuckDB v1.5.4/v1.5.5), exactly the case
        // NativeStatementRedactor's quoted-literal masking exists for.
        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(
            new ConnectorConfig(Connection()), CancellationToken.None);

        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), out var scan));
        var createSecret = scan!.SetupStatements[2];
        // Sanity: the carrier itself does hold the password -- the one legitimate place it appears.
        Assert.Contains(SecretValue, createSecret, StringComparison.Ordinal);
        // Sanity: the attach path itself (index 3) never carries the password at all.
        Assert.DoesNotContain(SecretValue, scan.SetupStatements[3], StringComparison.Ordinal);

        // Dropping the comma before `user` makes DuckDB's parser reject the statement with a
        // "LINE 1: ..." context block echoing the whole statement -- including the password literal --
        // verbatim. Deterministic and offline: the parser rejects before TYPE mysql is ever resolved,
        // so the extension need not be installed.
        var broken = createSecret.Replace("database 'analytics', user", "database 'analytics' user", StringComparison.Ordinal);
        Assert.NotEqual(createSecret, broken); // sanity: the replace actually took effect

        var dir = Path.Combine(Path.GetTempPath(), "pz-mysql-redaction-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));

            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                () => NativeSetup.ExecuteSetupAsync(duck, broken, CancellationToken.None));

            Assert.Contains("PZ0311", ex.Message, StringComparison.Ordinal);
            Assert.Contains("CREATE OR …", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(SecretValue, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
