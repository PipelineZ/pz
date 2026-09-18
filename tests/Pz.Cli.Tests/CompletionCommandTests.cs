using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

/// <summary>`pz completion` end to end. Entirely offline and side-effect-free -- no project, no
/// filesystem writes, no network -- so these need no fixture at all.
///
/// Joins "console-and-env-serialized" (defined in RestoreCommandTests.cs) purely because it redirects
/// the process-global Console.Out/Error to assert on CLI output, and would otherwise race the other
/// classes that do.</summary>
[Collection("console-and-env-serialized")]
public sealed class CompletionCommandTests
{
    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Supported_shell_exits_ok(string shell)
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.NotEmpty(stdout);
    }

    /// <summary>The whole point of generating from <see cref="Commands.CommandTree"/> rather than a
    /// hand-maintained list: every verb AND sub-verb registered on the real <see cref="CliApp.Build"/>
    /// tree must appear in every shell's script, so a new verb cannot silently drift out of sync with
    /// `--help`.</summary>
    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Every_registered_command_name_appears_in_the_generated_script(string shell)
    {
        var names = CommandTree.AllNames(CliApp.Build());
        Assert.NotEmpty(names);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        foreach (var name in names)
        {
            Assert.Contains(name, stdout);
        }
    }

    [Theory]
    [InlineData("bash"), InlineData("zsh"), InlineData("fish"), InlineData("pwsh")]
    public void Script_is_lf_only_with_a_single_trailing_newline(string shell)
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["completion", shell]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.DoesNotContain('\r', stdout);
        Assert.EndsWith("\n", stdout);
        Assert.False(stdout.EndsWith("\n\n", StringComparison.Ordinal), "must have exactly one trailing newline");
    }

    [Fact]
    public void Unknown_shell_is_a_config_error()
    {
        var stderr = Capture(() => CliApp.Build().Parse(["completion", "cmd"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0535", stderr);
        Assert.Contains("cmd", stderr);
    }

    /// <summary>A missing required argument is a parse error -- only <see cref="CliApp.Run"/> (not a
    /// bare <c>ParseResult.Invoke()</c>) overrides System.CommandLine's own exit-1 default for that case
    /// (see CliAppTests' equivalent fact), so this goes through the same overload Program.cs actually
    /// runs.</summary>
    [Fact]
    public void Missing_shell_argument_is_a_config_error()
    {
        var exit = CliApp.Run(CliApp.Build(), ["completion"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    private static string Capture(Func<int> action, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }

    private static string CaptureOut(Func<int> action, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(original);
        }

        return stdout.ToString();
    }
}
