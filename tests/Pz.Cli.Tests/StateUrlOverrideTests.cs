using Pz.Cli.Commands;
using Pz.Core.Model;

namespace Pz.Cli.Tests;

public sealed class StateUrlOverrideTests
{
    private static PzProject Project(StateConfig? state = null) =>
        new("t", "1.0", new EngineConfig(), new Dictionary<string, object?>(), [], [], [], null, state);

    private static readonly IReadOnlyDictionary<string, string> NoEnv = new Dictionary<string, string>();

    [Fact]
    public void Absent_url_leaves_the_project_untouched()
    {
        var project = Project();

        Assert.True(StateUrlOverride.TryApply(project, null, NoEnv, out var result, out var error));

        Assert.Same(project, result);
        Assert.Null(error);
    }

    [Fact]
    public void Url_outranks_a_project_yml_state_block()
    {
        var project = Project(new StateConfig(
            StateConfig.SqlServer, "ops", null, "pz", Artifacts: true, Events: true,
            BackendSource: "project.yml"));
        var env = new Dictionary<string, string> { ["PZ_STATE_TOKEN"] = "secret" };

        Assert.True(StateUrlOverride.TryApply(
            project, "https://state.example/api/agents/runs/x/state", env, out var result, out _));

        Assert.Equal(StateConfig.Http, result.State.Backend);
        Assert.Equal("https://state.example/api/agents/runs/x/state", result.State.Url);
        Assert.Equal("secret", result.State.Token);
        Assert.Equal("--state-url", result.State.BackendSource);
        Assert.False(result.State.Artifacts);
        Assert.False(result.State.Events);
    }

    [Fact]
    public void Missing_token_env_yields_null_token()
    {
        Assert.True(StateUrlOverride.TryApply(
            Project(), "http://localhost:5000/state", NoEnv, out var result, out _));

        Assert.Null(result.State.Token);
    }

    [Theory]
    [InlineData("not-a-url")]
    [InlineData("/relative/path")]
    [InlineData("ftp://host/state")]
    public void Non_http_url_is_refused(string raw)
    {
        Assert.False(StateUrlOverride.TryApply(Project(), raw, NoEnv, out _, out var error));

        Assert.Contains("--state-url", error);
    }
}
