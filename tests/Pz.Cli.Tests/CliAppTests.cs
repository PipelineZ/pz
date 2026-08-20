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
}
