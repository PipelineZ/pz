using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>Under `pz mcp`, a localfiles
/// `path:`/`root:` that resolves outside the project directory is refused with PZ0606 — the agent
/// surface operates only on files inside the project, matching the posture PZ0602 already takes for
/// `../` in mutation targets. The plain CLI stays paths-are-trusted (your config, your files); only
/// the MCP surface refuses. Enforced at the shared project-load seam, so verify, execute,
/// introspect, and authoring tools all refuse uniformly.</summary>
public class PathGuardTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for path guard tests"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for path guard tests"),
        RunAsync = (_, _) => throw new InvalidOperationException("refusal must happen before any run"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("refusal must happen before any run"),
    };

    private static void RewriteOrdersPath(TempProject p, string path)
    {
        File.WriteAllText(Path.Combine(p.Dir, "connections.yml"),
            $"""
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: {path}
                    format: csv

            out:
              connector: localfiles
              root: out
            """ + "\n");
    }

    [Fact]
    public async Task Validate_refuses_a_relative_path_escaping_the_project()
    {
        using var p = new TempProject();
        RewriteOrdersPath(p, "../../../../etc/hostname");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var error = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0606", error.GetProperty("code").GetString());
        Assert.Contains("raw", error.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("outside the project directory", error.GetProperty("message").GetString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_an_absolute_path_outside_the_project()
    {
        using var p = new TempProject();
        RewriteOrdersPath(p, "/etc/hostname");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Run_refuses_an_escaping_path_before_any_execution()
    {
        using var p = new TempProject();
        RewriteOrdersPath(p, "../outside.csv");

        // RunAsync in RealServices throws if reached — the refusal must come first.
        var doc = JsonDocument.Parse(await ExecutionTools.RunAsync(
            p.Dir, ["orders_out"], false, false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Set_entity_options_refuses_a_proposed_escaping_path_without_writing()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));

        var doc = JsonDocument.Parse(await AuthoringTools.SetEntityOptionsAsync(
            p.Dir, "raw", "orders",
            read: new Dictionary<string, object?> { ["path"] = "../../secrets.csv", ["format"] = "csv" },
            write: null, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    [Fact]
    public async Task Paths_inside_the_project_stay_accepted()
    {
        using var p = new TempProject();
        RewriteOrdersPath(p, "data/orders.csv");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    // --- sqlite: the connection's `path:` is a file path exactly like a localfiles
    // root, so it joins the same PZ0606 posture — an agent-authored project cannot point pz at a
    // database outside the project directory.

    private static void AppendSqliteConnection(TempProject p, string path)
    {
        File.AppendAllText(Path.Combine(p.Dir, "connections.yml"),
            $"""

            appdb:
              connector: sqlite
              path: {path}
            """ + "\n");
    }

    [Fact]
    public async Task Validate_refuses_a_sqlite_path_escaping_the_project()
    {
        using var p = new TempProject();
        AppendSqliteConnection(p, "../../outside.db");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var error = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0606", error.GetProperty("code").GetString());
        Assert.Contains("sqlite", error.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("outside the project directory", error.GetProperty("message").GetString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Sqlite_paths_inside_the_project_stay_accepted()
    {
        using var p = new TempProject();
        AppendSqliteConnection(p, "data/app.db");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Add_connection_refuses_a_proposed_escaping_sqlite_path_without_writing()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));

        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "appdb", "sqlite",
            new Dictionary<string, object?> { ["path"] = "../../stolen.db" },
            RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }
}
