using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>pz_add_connection / pz_update_connection / pz_remove_connection: the shared four-step
/// mutation pipeline end to end. Postgres connection keys below (host/database required,
/// port/user/password/ssl_mode optional) are verified against connectors/Pz.Connector.Postgres's real
/// <c>ConnectionConfigSchema</c> -- the point of these tests is the credential guard and the mutation
/// contract, not postgres.</summary>
public class ConnectionAuthoringTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for authoring tools"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for connection authoring"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for connection authoring"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for connection authoring"),
    };

    [Fact]
    public async Task Add_connection_writes_block_and_self_verifies()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "raw2", "localfiles",
            new() { ["root"] = "data2" }, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Contains("raw2:", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Literal_password_is_refused_before_any_write()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "pg", "postgres",
            new() { ["host"] = "db", ["password"] = "hunter2" }, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0601", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.DoesNotContain("hunter2", doc.RootElement.ToString()); // the secret never transits the result
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Env_ref_password_is_accepted()
    {
        using var p = new TempProject();
        Environment.SetEnvironmentVariable("MCP_TEST_PW", "x");
        try
        {
            var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
                p.Dir, "pg", "postgres",
                new() { ["host"] = "db", ["port"] = 5432L, ["database"] = "d", ["user"] = "u",
                        ["password"] = "${MCP_TEST_PW}" }, RealServices(), CancellationToken.None));
            Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        }
        finally { Environment.SetEnvironmentVariable("MCP_TEST_PW", null); }
    }

    [Fact]
    public async Task Removing_a_connection_a_pipeline_uses_stays_applied_and_reports_errors()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.RemoveConnectionAsync(
            p.Dir, "raw", RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.DoesNotContain("raw:", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Add_existing_name_is_pz0602_pointing_at_update()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "raw", "localfiles", new() { ["root"] = "x" }, RealServices(), CancellationToken.None));
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("pz_update_connection", doc.RootElement.GetProperty("errors")[0].GetProperty("next_step").GetString());
    }

    [Fact]
    public async Task Update_missing_name_is_pz0602_pointing_at_add()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.UpdateConnectionAsync(
            p.Dir, "nope", "localfiles", new() { ["root"] = "x" }, RealServices(), CancellationToken.None));
        Assert.Equal("PZ0602", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Contains("pz_add_connection", doc.RootElement.GetProperty("errors")[0].GetProperty("next_step").GetString());
    }

    [Fact]
    public async Task Update_replaces_the_block_wholesale_and_self_verifies()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.UpdateConnectionAsync(
            p.Dir, "out", "localfiles", new() { ["root"] = "out2" }, RealServices(), CancellationToken.None));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("result").GetProperty("dropped_comment").GetBoolean() == false);
        var text = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        Assert.Contains("out2", text);
    }

    [Fact]
    public async Task Unknown_connector_name_is_refused()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "made_up", "no_such_connector", new() { ["root"] = "x" },
            RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0305", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.DoesNotContain("made_up:", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Schema_violation_is_refused_before_any_write()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));
        // postgres requires host + database -- neither given here, and no credential-shaped key fires
        // first, so this exercises step 2's schema pre-validate specifically.
        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "pg", "postgres", new() { ["ssl_mode"] = "require" }, RealServices(), CancellationToken.None));
        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }
}
