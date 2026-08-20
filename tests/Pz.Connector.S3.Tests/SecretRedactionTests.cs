using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Connector.S3.Tests;

/// <summary>The <c>secret_key</c> connection value must never leak outside the CREATE SECRET statement
/// text itself -- not into a <see cref="PlannedNode.Reason"/>, not into plan.json, and not into a
/// <see cref="NativeSetup"/> failure message even when the setup statement itself is malformed (a
/// malformed CREATE SECRET is exactly the shape the redaction protects against -- see
/// <see cref="Pz.Engine.Execution.NativeStatementRedactor"/>).</summary>
public sealed class SecretRedactionTests
{
    private const string SecretValue = "S3CRET_VALUE";

    [Fact]
    public async Task Plan_reason_and_plan_json_never_contain_the_secret()
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
            new Dictionary<string, object?> { ["access_key"] = "AKIA_TEST", ["secret_key"] = SecretValue }, [],
            "connections.yml") { Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["bucket"] = "my-bucket", ["format"] = "parquet" })] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "lake.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        var dag = new CompiledDag([loadNode, pipelineNode, sinkNode]);
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        Assert.All(plan.Nodes, n => Assert.DoesNotContain(SecretValue, n.Reason, StringComparison.Ordinal));

        var dir = Path.Combine(Path.GetTempPath(), "pz-s3-redaction-tests", Guid.NewGuid().ToString("N"));
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
    public async Task Malformed_secret_setup_statement_never_leaks_the_secret()
    {
        await using var sink = await ((Pz.Connectors.Abstractions.ISinkConnector)new S3Connector()).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["access_key"] = "AKIA_TEST",
                ["secret_key"] = SecretValue,
            }), CancellationToken.None);

        var spec = new OutputSpec("lake", "data", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["bucket"] = "my-bucket", ["format"] = "parquet" });
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        var realSecretStatement = copy!.SetupStatements[2];
        // Sanity: the carrier itself does hold the secret -- this is the one legitimate place it's
        // allowed to appear (see S3SinkTests.Setup_statements_shape_secret_correctly).
        Assert.Contains(SecretValue, realSecretStatement, StringComparison.Ordinal);

        // Reproduces the exact DuckDB Parser Error shape validated at the engine level
        // (NativePathTests.Parser_error_never_leaks_setup_statement): dropping the comma before the
        // "secret" clause makes DuckDB reject the statement with a "LINE 1: ..." context block that
        // echoes the whole offending statement -- including the secret literal -- verbatim. This
        // reproduces deterministically without any network access (the parser rejects the statement
        // before "type s3" is ever resolved, so httpfs need not even be installed).
        var broken = realSecretStatement.Replace("', secret '", "' secret '", StringComparison.Ordinal);
        Assert.NotEqual(realSecretStatement, broken); // sanity: the replace actually took effect

        var dir = Path.Combine(Path.GetTempPath(), "pz-s3-redaction-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            await using var duck = DuckSession.Open(Path.Combine(dir, "t.duckdb"));

            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                () => NativeSetup.ExecuteSetupAsync(duck, broken, CancellationToken.None));

            Assert.False(ex.IsTransient);
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
