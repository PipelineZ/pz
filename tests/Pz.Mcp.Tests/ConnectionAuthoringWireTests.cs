using System.IO.Pipelines;
using System.Text.Json;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Pz.Mcp.Tests;

/// <summary>The connection-authoring tools exercised OVER THE WIRE, not by calling
/// <see cref="Handlers.AuthoringTools"/> directly.
///
/// This distinction is the whole point of the file. pz_add_connection/pz_update_connection are the
/// only tools taking a non-nullable <see cref="JsonElement"/> parameter, and the SDK publishes the
/// boolean schema `true` ("any value") for one. Every direct-call test passed a
/// <c>Dictionary&lt;string, object?&gt;</c> and so never met that schema — while every real client
/// call died in <see cref="ArgumentValidatingTool"/>'s own <c>TryGetProperty</c>, which throws on a
/// non-object element. Both tools were unusable from any MCP client, the whole authoring surface with
/// them, and the suite was green throughout: the gap was the transport, not the logic.</summary>
public class ConnectionAuthoringWireTests
{
    private static CliServices RealServices() => new()
    {
        CreateRegistryAsync = (project, dir, ct) =>
            Pz.Cli.ConnectorRegistryFactory.CreateAsync(project, dir, noLockCheck: false, ct),
        CreateStateStores = (_, _) => throw new InvalidOperationException("not needed"),
        InitProject = (_, _) => throw new InvalidOperationException("not needed"),
        RunAsync = (_, _) => throw new InvalidOperationException("not needed"),
        RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed"),
    };

    private static async Task<McpClient> Connect(string projectDir)
    {
        Pipe c2s = new(), s2c = new();
        var server = McpServer.Create(
            new StreamServerTransport(c2s.Reader.AsStream(), s2c.Writer.AsStream()),
            PzMcpServer.CreateOptions(projectDir, RealServices(), allowRun: false));
        _ = server.RunAsync();
        return await McpClient.CreateAsync(
            new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));
    }

    private static JsonElement Envelope(CallToolResult result) =>
        JsonDocument.Parse(Assert.IsType<TextContentBlock>(Assert.Single(result.Content)).Text).RootElement.Clone();

    [Fact]
    public async Task Add_connection_applies_over_the_wire()
    {
        using var p = new TempProject();
        await using var client = await Connect(p.Dir);

        var result = await client.CallToolAsync("pz_add_connection", new Dictionary<string, object?>
        {
            ["name"] = "archive",
            ["connector"] = "localfiles",
            ["connection"] = new Dictionary<string, object?> { ["root"] = "archive" },
        });

        var envelope = Envelope(result);
        Assert.NotEqual(true, result.IsError);
        Assert.True(envelope.GetProperty("ok").GetBoolean(), envelope.ToString());
        Assert.Contains("archive:", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Update_connection_applies_over_the_wire()
    {
        using var p = new TempProject();
        await using var client = await Connect(p.Dir);

        var result = await client.CallToolAsync("pz_update_connection", new Dictionary<string, object?>
        {
            ["name"] = "out",
            ["connector"] = "localfiles",
            ["connection"] = new Dictionary<string, object?> { ["root"] = "elsewhere" },
        });

        Assert.True(Envelope(result).GetProperty("ok").GetBoolean());
        Assert.Contains("root: elsewhere", File.ReadAllText(Path.Combine(p.Dir, "connections.yml")), StringComparison.Ordinal);
    }

    /// <summary>A wholesale replace takes the connection's entities with it. Reporting only
    /// <c>dropped_comment</c> made the envelope actively reassuring about the one thing that had just
    /// been destroyed — an agent that called pz_add_entity and then adjusted a single connection
    /// option would be told <c>ok: true</c> over the wreckage of its own prior call.</summary>
    [Fact]
    public async Task Update_connection_names_the_entities_it_dropped()
    {
        using var p = new TempProject();
        await using var client = await Connect(p.Dir);

        var result = await client.CallToolAsync("pz_update_connection", new Dictionary<string, object?>
        {
            ["name"] = "raw", // TempProject declares raw.orders under an entities: block
            ["connector"] = "localfiles",
            ["connection"] = new Dictionary<string, object?> { ["root"] = "data" },
        });

        var envelope = Envelope(result);
        Assert.Equal(
            ["orders"],
            envelope.GetProperty("result").GetProperty("dropped_entities")
                .EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Contains(
            "pz_add_entity",
            envelope.GetProperty("result").GetProperty("warnings").EnumerateArray().Single().GetString()!,
            StringComparison.Ordinal);
    }

    /// <summary>Dropping an entity is usually the very reason self-verify then fails, so the report
    /// must ride the error envelope too — that is where it explains the errors above it.</summary>
    [Fact]
    public async Task Dropped_entities_are_reported_even_when_self_verify_fails()
    {
        using var p = new TempProject();
        // Any self-verify failure will do -- what is under test is that the result still rides the
        // error envelope, not which error put it there. A ref() to a pipeline that does not exist is
        // the cheapest guaranteed one.
        p.WritePipeline("broken", "select * from {{ ref('no_such_pipeline') }}\n");
        await using var client = await Connect(p.Dir);
        await client.CallToolAsync("pz_add_entity", new Dictionary<string, object?>
        {
            ["connection"] = "out",
            ["entity"] = "orders_out",
            ["write"] = new Dictionary<string, object?> { ["format"] = "csv", ["strategy"] = "replace" },
        });

        var result = await client.CallToolAsync("pz_update_connection", new Dictionary<string, object?>
        {
            ["name"] = "out",
            ["connector"] = "localfiles",
            ["connection"] = new Dictionary<string, object?> { ["root"] = "elsewhere" },
        });

        var envelope = Envelope(result);
        Assert.False(envelope.GetProperty("ok").GetBoolean());
        Assert.True(envelope.GetProperty("applied").GetBoolean());
        Assert.NotEmpty(envelope.GetProperty("errors").EnumerateArray());
        Assert.Equal(
            ["orders_out"],
            envelope.GetProperty("result").GetProperty("dropped_entities")
                .EnumerateArray().Select(e => e.GetString()!).ToArray());
    }

    /// <summary>A handler failure must still name something. The SDK's own catch would answer "An
    /// error occurred invoking '&lt;tool&gt;'." with the exception discarded and nothing logged
    /// (`pz mcp` wires no ILoggerFactory), so <see cref="ArgumentValidatingTool"/> translates one into
    /// a PZ0609 envelope instead. Provoked with a services wiring whose registry factory throws an
    /// exception type no handler classifies.</summary>
    [Fact]
    public async Task Unclassified_handler_failure_answers_PZ0609_not_the_SDK_generic_text()
    {
        using var p = new TempProject();
        var exploding = new CliServices
        {
            CreateRegistryAsync = (_, _, _) => throw new NotSupportedException("boom from the registry"),
            CreateStateStores = (_, _) => throw new InvalidOperationException("not needed"),
            InitProject = (_, _) => throw new InvalidOperationException("not needed"),
            RunAsync = (_, _) => throw new InvalidOperationException("not needed"),
            RetryAsync = (_, _, _) => throw new InvalidOperationException("not needed"),
        };
        Pipe c2s = new(), s2c = new();
        var server = McpServer.Create(
            new StreamServerTransport(c2s.Reader.AsStream(), s2c.Writer.AsStream()),
            PzMcpServer.CreateOptions(p.Dir, exploding, allowRun: false));
        _ = server.RunAsync();
        await using var client = await McpClient.CreateAsync(
            new StreamClientTransport(c2s.Writer.AsStream(), s2c.Reader.AsStream()));

        var result = await client.CallToolAsync("pz_add_connection", new Dictionary<string, object?>
        {
            ["name"] = "archive",
            ["connector"] = "localfiles",
            ["connection"] = new Dictionary<string, object?> { ["root"] = "archive" },
        });

        var envelope = Envelope(result);
        var error = envelope.GetProperty("errors").EnumerateArray().Single();
        Assert.Equal("PZ0609", error.GetProperty("code").GetString());
        Assert.Contains("pz_add_connection", error.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Contains("boom from the registry", error.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.False(envelope.GetProperty("applied").GetBoolean());
    }
}
