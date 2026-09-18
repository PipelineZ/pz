using System.CommandLine;
using Pz.Cli;

namespace Pz.Cli.Tests;

public class CliAppTests
{
    [Theory]
    [InlineData("init"), InlineData("restore"), InlineData("validate"), InlineData("compile"),
     InlineData("plan"), InlineData("run"), InlineData("retry"), InlineData("test"),
     InlineData("ls"), InlineData("connectors"), InlineData("runs")]
    public void Root_command_exposes_verb(string verb)
    {
        var root = CliApp.Build();
        Assert.Contains(root.Subcommands, c => c.Name == verb);
    }

    [Fact]
    public void Unimplemented_verb_returns_config_error_exit_code()
    {
        var result = CliApp.Build().Parse("plan").Invoke();
        Assert.Equal(ExitCodes.ConfigError, result);
    }

    /// <summary>An unrecognized command is a usage error, not a node failure -- a CI caller must be able
    /// to tell "pz bogus" apart from a run that actually executed nodes and failed some of them.
    /// System.CommandLine's own ParseErrorAction hardcodes exit 1 for this; CliApp.Run overrides it.
    /// Goes through the internal <see cref="CliApp.Run(RootCommand,string[],TextWriter,Func{string,string?})"/>
    /// overload -- unlike a bare <c>ParseResult.Invoke()</c>, that is the actual path Program.cs runs.</summary>
    [Fact]
    public void Unrecognized_command_is_config_error_not_node_failures()
    {
        var exit = CliApp.Run(CliApp.Build(), ["bogus"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    [Fact]
    public void Unrecognized_option_is_config_error_not_node_failures()
    {
        var exit = CliApp.Run(CliApp.Build(), ["restore", "--this-flag-does-not-exist"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    [Fact]
    public void Missing_required_argument_is_config_error_not_node_failures()
    {
        var exit = CliApp.Run(CliApp.Build(), ["connector", "test"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.ConfigError, exit);
    }

    [Fact]
    public void Help_still_exits_ok()
    {
        var exit = CliApp.Run(CliApp.Build(), ["--help"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.Ok, exit);
    }

    [Fact]
    public void Version_still_exits_ok()
    {
        var exit = CliApp.Run(CliApp.Build(), ["--version"], new StringWriter(), _ => null);
        Assert.Equal(ExitCodes.Ok, exit);
    }

    /// <summary>An exception no verb anticipated is a fatal (exit 3) with a PZ code and a next step —
    /// never a raw stack trace with exit 1.</summary>
    [Fact]
    public void Unhandled_exception_is_coded_fatal_without_a_stack_trace()
    {
        var (exit, stderr) = RunThrowing(debug: null);
        Assert.Equal(ExitCodes.Fatal, exit);
        Assert.Contains("error PZ0500", stderr);
        Assert.Contains("InvalidOperationException", stderr);
        Assert.Contains("boom", stderr);
        Assert.Contains("PZ_DEBUG=1", stderr);
        Assert.DoesNotContain("   at ", stderr);
    }

    [Fact]
    public void Unhandled_exception_prints_the_stack_under_PZ_DEBUG()
    {
        var (exit, stderr) = RunThrowing(debug: "1");
        Assert.Equal(ExitCodes.Fatal, exit);
        Assert.Contains("   at ", stderr);
    }

    private static (int Exit, string Stderr) RunThrowing(string? debug)
    {
        var root = new RootCommand();
        var verb = new Command("explode");
        verb.SetAction(int (ParseResult _) => throw new InvalidOperationException("boom"));
        root.Subcommands.Add(verb);

        var stderr = new StringWriter();
        var exit = CliApp.Run(root, ["explode"], stderr, name => name == "PZ_DEBUG" ? debug : null);
        return (exit, stderr.ToString());
    }
}
