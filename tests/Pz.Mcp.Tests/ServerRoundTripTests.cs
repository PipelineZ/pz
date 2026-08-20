using System.IO.Pipelines;
using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Pz.Mcp;

namespace Pz.Mcp.Tests;

public class ServerRoundTripTests
{
    private static CliServices FakeServices() => new()
    {
        CreateRegistryAsync = (_, _, _) => throw new InvalidOperationException("not needed for listing"),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for listing"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for listing"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for listing"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for listing"),
    };

    /// <summary>The real registry wiring (same one VerifyToolsTests uses) — needed only by the test
    /// that actually CALLS a tool; the listing tests never resolve a connector.</summary>
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        InitProject = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed for verify tools"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed for verify tools"),
    };

    private static async Task<McpClient> Connect(
        bool allowRun, string? projectDir = null, CliServices? services = null)
    {
        Pipe c2s = new(), s2c = new();
        var server = McpServer.Create(
            new StreamServerTransport(c2s.Reader.AsStream(), s2c.Writer.AsStream()),
            PzMcpServer.CreateOptions(
                projectDir ?? Directory.GetCurrentDirectory(), services ?? FakeServices(), allowRun));
        _ = server.RunAsync();
        return await McpClient.CreateAsync(
            new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));
    }

    [Fact]
    public async Task Tool_listing_without_allow_run_hides_execution_tools()
    {
        await using var client = await Connect(allowRun: false);
        var names = (await client.ListToolsAsync()).Select(t => t.Name).ToHashSet();
        Assert.Contains("pz_compile", names);
        Assert.DoesNotContain("pz_run", names);
        Assert.DoesNotContain("pz_retry", names);
        Assert.DoesNotContain("pz_run_results", names);
    }

    [Fact]
    public async Task Tool_listing_with_allow_run_shows_execution_tools()
    {
        await using var client = await Connect(allowRun: true);
        var names = (await client.ListToolsAsync()).Select(t => t.Name).ToHashSet();
        Assert.Contains("pz_run", names);
    }

    // ---- The published input schema IS the stability contract ------------------------------------
    // https://pipelinez.dev/reference/mcp-contract/ documents snake_case input names and marks several optional; the
    // SDK emits C# parameter names verbatim and marks only defaulted parameters optional, so nothing
    // but a wire-level assertion can keep code and contract doc honest. These two tests are that check.

    [Fact]
    public async Task Tool_input_schemas_publish_the_snake_case_names_the_contract_documents()
    {
        await using var client = await Connect(allowRun: true);
        var tools = (await client.ListToolsAsync()).ToDictionary(t => t.Name, t => t.ProtocolTool.InputSchema);

        Assert.Equal(
            ["all", "flow_names", "full_refresh"],
            PropertyNames(tools["pz_run"]));
        Assert.Equal(
            ["checks_yaml", "name", "sql"],
            PropertyNames(tools["pz_write_pipeline"]));
        Assert.Equal(["connect"], PropertyNames(tools["pz_validate"]));
        Assert.Equal(["full_refresh"], PropertyNames(tools["pz_retry"]));
        Assert.Equal(["run_id"], PropertyNames(tools["pz_run_results"]));

        // Optional inputs must be absent from `required` — an agent that omits them gets a real call,
        // not an invalid-params error.
        Assert.Empty(RequiredNames(tools["pz_run"]));
        Assert.Empty(RequiredNames(tools["pz_validate"]));
        Assert.Empty(RequiredNames(tools["pz_retry"]));
        Assert.Empty(RequiredNames(tools["pz_run_results"]));
        Assert.Equal(["name", "sql"], RequiredNames(tools["pz_write_pipeline"]));
        Assert.Equal(["connection", "entity"], RequiredNames(tools["pz_add_entity"]));
        Assert.Equal(["connection", "entity"], RequiredNames(tools["pz_set_entity_options"]));
    }

    [Fact]
    public async Task Calling_pz_validate_without_connect_returns_a_well_formed_envelope()
    {
        using var p = new TempProject();
        await using var client = await Connect(allowRun: false, p.Dir, RealServices());

        var result = await client.CallToolAsync("pz_validate", new Dictionary<string, object?>());

        var text = Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text;
        using var doc = JsonDocument.Parse(text);
        Assert.True(doc.RootElement.GetProperty("ok").GetBoolean());
        Assert.True(doc.RootElement.GetProperty("result").TryGetProperty("pipelines", out _));
    }

    // ---- Argument-shape errors must name the argument ---------------------------------------------
    // The SDK's own binder turns a binding failure into a generic "An error occurred
    // invoking '<tool>'." result an agent cannot self-correct from, so the server pre-validates every
    // call against the tool's published input schema and answers a real invalid-params error instead.

    [Fact]
    public async Task Mistyped_argument_yields_invalid_params_naming_argument_and_expected_type()
    {
        await using var client = await Connect(allowRun: false);

        var ex = await Assert.ThrowsAnyAsync<ModelContextProtocol.McpException>(() =>
            client.CallToolAsync("pz_validate", new Dictionary<string, object?> { ["connect"] = "yes" }).AsTask());

        Assert.Contains("connect", ex.Message, StringComparison.Ordinal);
        Assert.Contains("boolean", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Unknown_argument_yields_invalid_params_naming_it()
    {
        await using var client = await Connect(allowRun: false);

        var ex = await Assert.ThrowsAnyAsync<ModelContextProtocol.McpException>(() =>
            client.CallToolAsync("pz_validate", new Dictionary<string, object?> { ["connct"] = true }).AsTask());

        Assert.Contains("connct", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Missing_required_argument_yields_invalid_params_naming_it()
    {
        await using var client = await Connect(allowRun: false);

        var ex = await Assert.ThrowsAnyAsync<ModelContextProtocol.McpException>(() =>
            client.CallToolAsync("pz_entity_schema", new Dictionary<string, object?>()).AsTask());

        Assert.Contains("connection", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>An untyped input is an unanswerable question. The SDK infers the boolean schema
    /// `true` ("any value") for a JsonElement parameter, which is what an agent reads when deciding
    /// its first pz_add_connection call — so the option-map parameters republish as typed objects.
    /// The optional ones must stay optional while doing it: `default` is what marks them so.</summary>
    [Fact]
    public async Task Option_map_parameters_publish_as_typed_objects()
    {
        await using var client = await Connect(allowRun: false);
        var tools = (await client.ListToolsAsync()).ToDictionary(t => t.Name, t => t.ProtocolTool.InputSchema);

        foreach (var (tool, parameter) in new[]
        {
            ("pz_add_connection", "connection"), ("pz_update_connection", "connection"),
            ("pz_add_entity", "read"), ("pz_add_entity", "write"),
            ("pz_set_entity_options", "read"), ("pz_set_entity_options", "write"),
        })
        {
            var schema = tools[tool].GetProperty("properties").GetProperty(parameter);
            Assert.Equal("object", schema.GetProperty("type").GetString());
            Assert.True(schema.TryGetProperty("description", out _), $"{tool}.{parameter} has no description");
        }

        // read/write stay optional -- the `default` the SDK emitted must survive the rewrite.
        Assert.Equal(
            ["connection", "entity"],
            RequiredNames(tools["pz_add_entity"]));
    }

    private static string[] PropertyNames(JsonElement schema) =>
        schema.TryGetProperty("properties", out var properties)
            ? [.. properties.EnumerateObject().Select(p => p.Name).Order(StringComparer.Ordinal)]
            : [];

    private static string[] RequiredNames(JsonElement schema) =>
        schema.TryGetProperty("required", out var required)
            ? [.. required.EnumerateArray().Select(e => e.GetString()!).Order(StringComparer.Ordinal)]
            : [];
}
