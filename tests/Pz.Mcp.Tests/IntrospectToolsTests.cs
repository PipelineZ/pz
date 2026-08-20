using System.Text.Json;
using Pz.Mcp;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

public class IntrospectToolsTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (project, dir) =>
        {
            var backends = Pz.Cli.StateBackendFactory.Create(project, dir, TimeProvider.System);
            backends.EnsureSchema();
            return new McpStateStores(backends.Watermarks, backends.SyncState, backends.Schemas, backends.Artifacts);
        },
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for introspect tools"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for introspect tools"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for introspect tools"),
    };

    [Fact]
    public void Overview_lists_connections_without_config_values()
    {
        using var p = new TempProject();
        var json = IntrospectTools.Overview(p.Dir);
        Assert.Contains("\"raw\"", json);
        Assert.Contains("localfiles", json);
        Assert.DoesNotContain("root", json);      // connection NAMES and connector types only
        Assert.DoesNotContain("base_dir", json);
    }

    [Fact]
    public void Overview_reports_dag_edges_per_pipeline()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(IntrospectTools.Overview(p.Dir));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("mcp_test", result.GetProperty("name").GetString());

        var pipelines = result.GetProperty("pipelines").EnumerateArray().ToList();
        var stgOrders = pipelines.First(pl => pl.GetProperty("name").GetString() == "stg_orders");
        Assert.Contains("raw.orders", stgOrders.GetProperty("sources").EnumerateArray().Select(e => e.GetString()));

        var ordersOut = pipelines.First(pl => pl.GetProperty("name").GetString() == "orders_out");
        Assert.Contains("stg_orders", ordersOut.GetProperty("refs").EnumerateArray().Select(e => e.GetString()));
        Assert.Contains("out.orders_out", ordersOut.GetProperty("sinks").EnumerateArray().Select(e => e.GetString()));

        Assert.True(result.GetProperty("dag").GetProperty("nodes").GetArrayLength() >= 3);
        Assert.True(result.GetProperty("flows").GetArrayLength() >= 1);
    }

    [Fact]
    public void Overview_on_broken_project_still_returns_project_info_with_errors()
    {
        using var p = new TempProject();
        // Same trigger VerifyToolsTests uses: a ref() to an unknown CONNECTION (PZ0201's hinted form) —
        // this fails compile but leaves ProjectLoader.Load (YAML parsing) unaffected, so the fallback
        // path actually has something to report.
        p.WritePipeline("bad", "SELECT * FROM {{ source('no_such_connection', 'x') }}\n");
        var doc = JsonDocument.Parse(IntrospectTools.Overview(p.Dir));

        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("errors").GetArrayLength() > 0);

        var result = doc.RootElement.GetProperty("result");
        Assert.Equal("mcp_test", result.GetProperty("name").GetString());
        Assert.Contains("raw", result.GetProperty("connections").EnumerateArray()
            .Select(c => c.GetProperty("name").GetString()));
        // No compiled DAG on this path: best-effort only, never guessed at.
        Assert.Empty(result.GetProperty("flows").EnumerateArray());
        Assert.Empty(result.GetProperty("dag").GetProperty("nodes").EnumerateArray());
    }

    [Fact]
    public async Task Connector_reference_returns_option_schemas_verbatim()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(await IntrospectTools.ConnectorReferenceAsync(p.Dir, RealServices(), CancellationToken.None));
        var localfiles = doc.RootElement.GetProperty("result").GetProperty("connectors")
            .EnumerateArray().First(c => c.GetProperty("name").GetString() == "localfiles");
        Assert.True(localfiles.TryGetProperty("dataset_schema", out _));
        Assert.True(localfiles.TryGetProperty("capabilities", out _));
        Assert.True(localfiles.GetProperty("source").GetBoolean());
        Assert.True(localfiles.GetProperty("sink").GetBoolean());
        // A JSON Schema, not a dictionary of filled-in values — never a value like a real root path.
        Assert.Equal(JsonValueKind.Object, localfiles.GetProperty("connection_schema").ValueKind);
    }

    [Fact]
    public void State_on_fresh_project_returns_empty_watermarks_and_no_runs()
    {
        using var p = new TempProject();
        var doc = JsonDocument.Parse(IntrospectTools.State(p.Dir, RealServices()));
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        var result = doc.RootElement.GetProperty("result");
        Assert.False(result.GetProperty("watermarks").GetProperty("corrupt").GetBoolean());
        Assert.Empty(result.GetProperty("watermarks").GetProperty("entries").EnumerateArray());
        Assert.Equal(JsonValueKind.Null, result.GetProperty("latest_run").ValueKind);
    }
}
