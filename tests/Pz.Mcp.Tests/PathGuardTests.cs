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

    // --- duckdb: the connection's `path:` is a database file exactly like sqlite's.

    private static void AppendDuckDbConnection(TempProject p, string path)
    {
        File.AppendAllText(Path.Combine(p.Dir, "connections.yml"),
            $"""

            lakedb:
              connector: duckdb
              path: {path}
            """ + "\n");
    }

    [Fact]
    public async Task Validate_refuses_a_duckdb_path_escaping_the_project()
    {
        using var p = new TempProject();
        AppendDuckDbConnection(p, "../../outside.duckdb");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var error = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0606", error.GetProperty("code").GetString());
        Assert.Contains("duckdb", error.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("outside the project directory", error.GetProperty("message").GetString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task DuckDb_paths_inside_the_project_stay_accepted()
    {
        using var p = new TempProject();
        AppendDuckDbConnection(p, "data/app.duckdb");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Add_connection_refuses_a_proposed_escaping_duckdb_path_without_writing()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));

        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "lakedb", "duckdb",
            new Dictionary<string, object?> { ["path"] = "../../stolen.duckdb" },
            RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }

    // --- ducklake: `path` is the catalog file (duckdb/sqlite catalogs), exactly like duckdb's own
    // `path:`; `data_path` is a second path-shaped key (the lake's data directory) that stays local
    // for every catalog except the object-store form, which the guard must ignore.

    private static void AppendDuckLakeConnection(TempProject p, string path, string dataPath)
    {
        File.AppendAllText(Path.Combine(p.Dir, "connections.yml"),
            $"""

            lake:
              connector: ducklake
              path: {path}
              data_path: {dataPath}
            """ + "\n");
    }

    [Fact]
    public async Task Validate_refuses_a_ducklake_catalog_path_escaping_the_project()
    {
        using var p = new TempProject();
        AppendDuckLakeConnection(p, "../../outside.ducklake", "lake/data");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        var error = doc.RootElement.GetProperty("errors")[0];
        Assert.Equal("PZ0606", error.GetProperty("code").GetString());
        Assert.Contains("ducklake", error.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Contains("outside the project directory", error.GetProperty("message").GetString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_refuses_a_ducklake_data_path_escaping_the_project()
    {
        using var p = new TempProject();
        AppendDuckLakeConnection(p, "lake/c.ducklake", "../../outside");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task DuckLake_paths_inside_the_project_stay_accepted()
    {
        using var p = new TempProject();
        AppendDuckLakeConnection(p, "lake/c.ducklake", "lake/data");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Validate_ignores_a_ducklake_object_store_data_path()
    {
        using var p = new TempProject();
        AppendDuckLakeConnection(p, "lake/c.ducklake", "s3://bucket/lake/");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
    }

    /// <summary>Only `data_path:` may name an object store; `path:` is checked regardless of what it
    /// contains, so a value that merely looks like a URL cannot bypass PZ0606.</summary>
    [Fact]
    public async Task Validate_still_refuses_a_url_looking_catalog_path()
    {
        using var p = new TempProject();
        AppendDuckLakeConnection(p, "file://../../outside.ducklake", "lake/data");

        var doc = JsonDocument.Parse(await VerifyTools.ValidateAsync(
            p.Dir, connect: false, RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Add_connection_refuses_a_proposed_escaping_ducklake_data_path_without_writing()
    {
        using var p = new TempProject();
        var before = File.ReadAllText(Path.Combine(p.Dir, "connections.yml"));

        var doc = JsonDocument.Parse(await AuthoringTools.AddConnectionAsync(
            p.Dir, "lake", "ducklake",
            new Dictionary<string, object?> { ["path"] = "lake/c.ducklake", ["data_path"] = "../../stolen" },
            RealServices(), CancellationToken.None));

        Assert.False(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.False(doc.RootElement.GetProperty("applied").GetBoolean());
        Assert.Equal("PZ0606", doc.RootElement.GetProperty("errors")[0].GetProperty("code").GetString());
        Assert.Equal(before, File.ReadAllText(Path.Combine(p.Dir, "connections.yml")));
    }
}
