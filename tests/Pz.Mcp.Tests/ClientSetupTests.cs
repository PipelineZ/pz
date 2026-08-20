using System.Text.Json;
using Pz.Cli.Commands;
using Pz.Cli;
using Pz.Mcp.ClientSetup;

namespace Pz.Mcp.Tests;

/// <summary>`pz mcp init`'s merge-preserving client config writer + aspire-style
/// skill installer, exercised through <see cref="McpCommand.Init"/> -- the same handler `pz mcp init`
/// itself calls (mirrors <see cref="ExecutionToolsTests"/>'s "call the CLI's own wiring" convention).
/// `homeOverride` redirects copilot-cli's `~/.copilot/mcp-config.json` target to a temp dir on every
/// call here -- no test in this file may ever touch a real `~/.copilot`.</summary>
public sealed class ClientSetupTests : IDisposable
{
    private readonly string _project = MakeTempDir("proj");
    private readonly string _home = MakeTempDir("home");

    public void Dispose()
    {
        TryDelete(_project);
        TryDelete(_home);
    }

    private static string MakeTempDir(string tag)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-mcp-clientsetup-" + tag + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void TryDelete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private int RunInit(string[] clients, bool all = false, bool allowRun = false, string? skillLocations = null) =>
        McpCommand.Init(clients, all, allowRun, skillLocations, _project, _home);

    // Scenario 1: vscode init on a fresh project creates .vscode/mcp.json with exactly the documented
    // entry, and re-running is byte-identical (idempotent).
    [Fact]
    public void Vscode_init_writes_the_documented_entry_and_is_idempotent()
    {
        var exit = RunInit(["vscode"]);
        Assert.Equal(ExitCodes.Ok, exit);

        var file = Path.Combine(_project, ".vscode", "mcp.json");
        var firstText = File.ReadAllText(file);
        using (var doc = JsonDocument.Parse(firstText))
        {
            var entry = doc.RootElement.GetProperty("servers").GetProperty("pz");
            Assert.Equal("stdio", entry.GetProperty("type").GetString());
            Assert.Equal("pz", entry.GetProperty("command").GetString());
            var args = entry.GetProperty("args");
            Assert.Equal(1, args.GetArrayLength());
            Assert.Equal("mcp", args[0].GetString());
        }

        Assert.EndsWith("\n", firstText, StringComparison.Ordinal);
        Assert.DoesNotContain("\r\n", firstText, StringComparison.Ordinal);

        var secondExit = RunInit(["vscode"]);
        Assert.Equal(ExitCodes.Ok, secondExit);
        var secondText = File.ReadAllText(file);
        Assert.Equal(firstText, secondText);
    }

    // Scenario 2: merge preservation -- a pre-seeded .mcp.json with another server plus a custom
    // top-level key keeps both when claude-code init adds `pz`.
    [Fact]
    public void ClaudeCode_init_preserves_sibling_servers_and_top_level_keys()
    {
        var file = Path.Combine(_project, ".mcp.json");
        File.WriteAllText(file, """{"mcpServers":{"other":{"command":"x"}},"customTopLevel":true}""" + "\n");

        var exit = RunInit(["claude-code"]);
        Assert.Equal(ExitCodes.Ok, exit);

        using var doc = JsonDocument.Parse(File.ReadAllText(file));
        Assert.True(doc.RootElement.GetProperty("customTopLevel").GetBoolean());
        var servers = doc.RootElement.GetProperty("mcpServers");
        Assert.Equal("x", servers.GetProperty("other").GetProperty("command").GetString());
        var pz = servers.GetProperty("pz");
        Assert.Equal("pz", pz.GetProperty("command").GetString());
        Assert.False(pz.TryGetProperty("type", out _));
        var args = pz.GetProperty("args");
        Assert.Equal(1, args.GetArrayLength());
        Assert.Equal("mcp", args[0].GetString());
    }

    // Scenario 3: an unparseable existing opencode.json refuses with PZ0605, and the file is left
    // byte-untouched.
    [Fact]
    public void Unparseable_existing_config_refuses_with_PZ0605_and_leaves_the_file_untouched()
    {
        var file = Path.Combine(_project, "opencode.json");
        const string broken = "{nope";
        File.WriteAllText(file, broken);

        var ex = Assert.Throws<Pz.Core.Validation.PzConfigException>(() =>
            ClientConfigWriter.Apply(file, "mcp", "pz", entry => entry["type"] = "local"));

        Assert.Equal(Pz.Core.Validation.PzErrorCode.McpClientConfigInvalid, ex.Error.Code);
        Assert.Contains(file, ex.Error.Message, StringComparison.Ordinal);
        Assert.Equal(broken, File.ReadAllText(file));
    }

    // Fix round 1, Finding 1: the same unparseable-file case, but driven through McpCommand.Init (the
    // CLI's own handler) rather than ClientConfigWriter directly -- reproduces the bug report's exact
    // repro ("echo '{nope' > opencode.json; pz mcp init opencode"): no exception may escape, stderr gets
    // the PZ0605 error line, exit code is ConfigError, and the file is left byte-untouched.
    [Fact]
    public void Init_on_unparseable_existing_config_does_not_throw_and_reports_PZ0605()
    {
        var file = Path.Combine(_project, "opencode.json");
        const string broken = "{nope";
        File.WriteAllText(file, broken);

        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = RunInit(["opencode"]);
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var message = stderr.ToString();
        Assert.Contains("PZ0605", message, StringComparison.Ordinal);
        Assert.Contains(file, message, StringComparison.Ordinal);
        Assert.Equal(broken, File.ReadAllText(file));
    }

    // Scenario 4: --allow-run puts the flag in args; without it, the flag is absent.
    [Fact]
    public void AllowRun_flag_is_only_present_when_requested()
    {
        RunInit(["claude-code"], allowRun: true);
        using (var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(_project, ".mcp.json"))))
        {
            var args = doc.RootElement.GetProperty("mcpServers").GetProperty("pz").GetProperty("args");
            Assert.Equal(2, args.GetArrayLength());
            Assert.Equal("mcp", args[0].GetString());
            Assert.Equal("--allow-run", args[1].GetString());
        }

        TryDelete(_project);
        Directory.CreateDirectory(_project);
        RunInit(["claude-code"], allowRun: false);
        using (var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(_project, ".mcp.json"))))
        {
            var args = doc.RootElement.GetProperty("mcpServers").GetProperty("pz").GetProperty("args");
            Assert.Equal(1, args.GetArrayLength());
            Assert.Equal("mcp", args[0].GetString());
        }
    }

    // opencode's --allow-run shape: appended to the command array, not a separate args key.
    [Fact]
    public void Opencode_entry_matches_the_documented_shape_and_appends_allow_run_to_command()
    {
        RunInit(["opencode"], allowRun: true);
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(_project, "opencode.json")));
        var entry = doc.RootElement.GetProperty("mcp").GetProperty("pz");
        Assert.Equal("local", entry.GetProperty("type").GetString());
        Assert.True(entry.GetProperty("enabled").GetBoolean());
        var command = entry.GetProperty("command");
        Assert.Equal(3, command.GetArrayLength());
        Assert.Equal("pz", command[0].GetString());
        Assert.Equal("mcp", command[1].GetString());
        Assert.Equal("--allow-run", command[2].GetString());
    }

    // copilot-cli entry shape, written to the (redirected) home directory.
    [Fact]
    public void CopilotCli_entry_matches_the_documented_shape_and_targets_the_redirected_home()
    {
        RunInit(["copilot-cli"]);
        var file = Path.Combine(_home, ".copilot", "mcp-config.json");
        Assert.True(File.Exists(file));
        using var doc = JsonDocument.Parse(File.ReadAllText(file));
        var entry = doc.RootElement.GetProperty("mcpServers").GetProperty("pz");
        Assert.Equal("local", entry.GetProperty("type").GetString());
        Assert.Equal("pz", entry.GetProperty("command").GetString());
        var tools = entry.GetProperty("tools");
        Assert.Equal(1, tools.GetArrayLength());
        Assert.Equal("*", tools[0].GetString());
    }

    // Scenario 5: skill install -- claude-code init (default locations) installs into both standard and
    // claudecode locations; SKILL.md carries aspire-style frontmatter; the guide file is non-empty;
    // --skill-locations none installs nothing; re-init overwrites cleanly.
    [Fact]
    public void ClaudeCode_init_installs_the_skill_into_standard_and_claudecode_locations()
    {
        RunInit(["claude-code"]);

        foreach (var relativeSkillDir in new[]
        {
            Path.Combine(".agents", "skills", "pz-pipelines"),
            Path.Combine(".claude", "skills", "pz-pipelines"),
        })
        {
            var skillMd = Path.Combine(_project, relativeSkillDir, "SKILL.md");
            var guide = Path.Combine(_project, relativeSkillDir, "references", "authoring-for-agents.md");
            Assert.True(File.Exists(skillMd), skillMd);
            Assert.True(File.Exists(guide), guide);

            var text = File.ReadAllText(skillMd);
            Assert.StartsWith("---", text, StringComparison.Ordinal);
            Assert.Contains("name: pz-pipelines", text, StringComparison.Ordinal);

            Assert.True(new FileInfo(guide).Length > 0);
        }

        // github/opencode locations are NOT implied by claude-code alone.
        Assert.False(Directory.Exists(Path.Combine(_project, ".github", "skills", "pz-pipelines")));
        Assert.False(Directory.Exists(Path.Combine(_project, ".opencode", "skill", "pz-pipelines")));
    }

    [Fact]
    public void SkillLocations_none_installs_nothing()
    {
        RunInit(["claude-code"], skillLocations: "none");

        Assert.False(Directory.Exists(Path.Combine(_project, ".agents")));
        Assert.False(Directory.Exists(Path.Combine(_project, ".claude")));
    }

    // Fix round 1, Finding 2: a typo'd --skill-locations token must not silently no-op -- it is a
    // PZ0605 usage error naming the bad token and the valid option set, exit 2, and NOTHING is
    // installed (including for locations that would otherwise be valid/implied).
    [Fact]
    public void SkillLocations_typo_is_a_PZ0605_error_naming_the_token_and_installs_nothing()
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = RunInit(["claude-code"], skillLocations: "claudcode");
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var message = stderr.ToString();
        Assert.Contains("PZ0605", message, StringComparison.Ordinal);
        Assert.Contains("claudcode", message, StringComparison.Ordinal);
        Assert.Contains("standard", message, StringComparison.Ordinal);
        Assert.Contains("claudecode", message, StringComparison.Ordinal);
        Assert.Contains("github", message, StringComparison.Ordinal);
        Assert.Contains("opencode", message, StringComparison.Ordinal);

        Assert.False(Directory.Exists(Path.Combine(_project, ".agents")));
        Assert.False(Directory.Exists(Path.Combine(_project, ".claude")));
        // No config file should have been written either -- validation happens before any write.
        Assert.False(File.Exists(Path.Combine(_project, ".mcp.json")));
    }

    [Fact]
    public void Reinit_overwrites_the_skill_cleanly()
    {
        RunInit(["claude-code"]);
        var skillMd = Path.Combine(_project, ".claude", "skills", "pz-pipelines", "SKILL.md");
        var firstText = File.ReadAllText(skillMd);

        RunInit(["claude-code"]);
        var secondText = File.ReadAllText(skillMd);

        Assert.Equal(firstText, secondText);
    }

    [Fact]
    public void SkillLocations_all_installs_into_every_location()
    {
        RunInit(["claude-code"], skillLocations: "all");

        foreach (var relativeDir in new[]
        {
            Path.Combine(".agents", "skills"),
            Path.Combine(".claude", "skills"),
            Path.Combine(".github", "skills"),
            Path.Combine(".opencode", "skill"),
        })
        {
            Assert.True(File.Exists(Path.Combine(_project, relativeDir, "pz-pipelines", "SKILL.md")));
        }
    }

    // Scenario 6: no clients and no --all -> exit 2, four options named.
    [Fact]
    public void No_clients_and_no_all_is_a_PZ0605_usage_error_naming_the_four_options()
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = McpCommand.Init([], all: false, allowRun: false, skillLocationsCsv: null, _project, _home);
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var message = stderr.ToString();
        Assert.Contains("PZ0605", message, StringComparison.Ordinal);
        Assert.Contains("vscode", message, StringComparison.Ordinal);
        Assert.Contains("claude-code", message, StringComparison.Ordinal);
        Assert.Contains("copilot-cli", message, StringComparison.Ordinal);
        Assert.Contains("opencode", message, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_client_name_is_a_PZ0605_error_naming_the_bad_value()
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = McpCommand.Init(["not-a-client"], all: false, allowRun: false, skillLocationsCsv: null, _project, _home);
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        var message = stderr.ToString();
        Assert.Contains("PZ0605", message, StringComparison.Ordinal);
        Assert.Contains("not-a-client", message, StringComparison.Ordinal);
    }

    [Fact]
    public void All_wires_up_every_client()
    {
        var exit = RunInit([], all: true);
        Assert.Equal(ExitCodes.Ok, exit);

        Assert.True(File.Exists(Path.Combine(_project, ".vscode", "mcp.json")));
        Assert.True(File.Exists(Path.Combine(_project, ".mcp.json")));
        Assert.True(File.Exists(Path.Combine(_project, "opencode.json")));
        Assert.True(File.Exists(Path.Combine(_home, ".copilot", "mcp-config.json")));
    }
}
