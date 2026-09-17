using System.CommandLine;
using Pz.Cli;

namespace Pz.Cli.Tests;

public class CliAppTests
{
    [Theory]
    [InlineData("init"), InlineData("restore"), InlineData("validate"), InlineData("compile"),
     InlineData("plan"), InlineData("run"), InlineData("retry"), InlineData("test"),
     InlineData("ls"), InlineData("connectors")]
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
