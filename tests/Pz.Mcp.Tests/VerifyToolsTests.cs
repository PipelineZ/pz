using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

public class VerifyToolsTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
    };

    [Fact]
    public async Task Compile_on_valid_project_returns_ok_with_dag()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await VerifyTools.CompileAsync(p.Dir, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var nodes = doc.RootElement.GetProperty("result").GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() >= 3); // source load + 2 pipelines + sink write
    }

    [Fact]
    public async Task Compile_on_broken_project_returns_aggregate_errors()
    {
        using var p = new TempProject();
        // A ref() to a missing pipeline (PZ0201) carries no hint — DagCompiler only attaches one to the
        // unknown-connection form of PZ0201 ("declare it in connections.yml: ..."). Using that form
        // rather than a plain ref('nope') is what makes the next_step assertion below meaningful.
        p.WritePipeline("bad", "SELECT * FROM {{ source('no_such_connection', 'x') }}\n");
        var doc = JsonDocument.Parse(await VerifyTools.CompileAsync(p.Dir, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var first = doc.RootElement.GetProperty("errors")[0];
        Assert.StartsWith("PZ", first.GetProperty("code").GetString());
        Assert.False(string.IsNullOrEmpty(first.GetProperty("next_step").GetString()));
    }

    [Fact]
    public async Task Validate_offline_runs_tiers_1_to_4()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(
            await VerifyTools.ValidateAsync(p.Dir, connect: false, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Plan_returns_strategy_per_node_and_no_sql_text()
    {
        using var p = new TempProject();
        var json = await VerifyTools.PlanAsync(p.Dir, RealServices(), CancellationToken.None);
        var doc = JsonDocument.Parse(json);
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.DoesNotContain("SELECT", json); // secret/SQL hygiene: plan results carry no SQL text
    }
}
