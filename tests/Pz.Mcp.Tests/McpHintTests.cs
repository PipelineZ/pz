using System.Text.Json;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>A next_step is only worth carrying if its reader can act on it. The load path's own hints
/// are written for someone at a shell — "run 'pz init &lt;name&gt;'" — and an MCP client has no shell:
/// it can call a tool, or ask its human, and the hint has to say which.</summary>
public class McpHintTests
{
    private static JsonElement FirstError(string envelope) =>
        JsonDocument.Parse(envelope).RootElement.GetProperty("errors").EnumerateArray().First().Clone();

    [Fact]
    public void Missing_project_points_at_pz_init_project_not_the_CLI_verb()
    {
        var empty = Path.Combine(Path.GetTempPath(), "pz-mcp-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(empty);
        try
        {
            var hint = FirstError(IntrospectTools.Overview(empty)).GetProperty("next_step").GetString()!;

            Assert.Contains("pz_init_project", hint, StringComparison.Ordinal);
            Assert.DoesNotContain("pz init <name>", hint, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(empty, recursive: true);
        }
    }

    /// <summary>The restart is the non-obvious half: the server snapshots the environment of its own
    /// process, which inherited the client's at launch, so exporting the variable anywhere else
    /// changes nothing. A hint that stops at "set the variable" sends an agent in a loop.</summary>
    [Fact]
    public void Undeclared_env_var_says_the_server_must_be_restarted()
    {
        using var p = new TempProject();
        File.AppendAllText(Path.Combine(p.Dir, "connections.yml"),
            "\nremote:\n  connector: localfiles\n  root: ${PZ_TEST_UNSET_ROOT}\n");

        var hint = FirstError(IntrospectTools.Overview(p.Dir)).GetProperty("next_step").GetString()!;

        Assert.Contains("PZ_TEST_UNSET_ROOT", hint, StringComparison.Ordinal);
        Assert.Contains("restart", hint, StringComparison.Ordinal);
    }
}
