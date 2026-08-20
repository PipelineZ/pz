using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

public sealed class StateConfigTests
{
    private static string WriteProject(string projectYaml)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pz-state-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "project.yml"), projectYaml);
        return dir;
    }

    private const string Minimal = "name: demo\nversion: \"1.0\"\n";

    [Fact]
    public void Absent_state_block_and_no_env_is_local()
    {
        var project = ProjectLoader.Load(WriteProject(Minimal), new Dictionary<string, string>());

        Assert.Equal(StateConfig.Local, project.State.Backend);
        Assert.Equal("default", project.State.BackendSource);
        Assert.False(project.State.Artifacts);
        Assert.False(project.State.Events);
    }

    [Fact]
    public void Environment_supplies_the_default_backend()
    {
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_BACKEND"] = "sqlserver",
            ["PZ_STATE_CONNECTION_STRING"] = "Server=x;Database=y;Integrated Security=true",
        };

        var project = ProjectLoader.Load(WriteProject(Minimal), env);

        Assert.Equal(StateConfig.SqlServer, project.State.Backend);
        Assert.Equal("PZ_STATE_BACKEND", project.State.BackendSource);
        // artifacts defaults to true for a non-local backend, events stays opt-in
        Assert.True(project.State.Artifacts);
        Assert.False(project.State.Events);
    }

    [Fact]
    public void Explicit_project_key_beats_its_environment_counterpart()
    {
        var env = new Dictionary<string, string> { ["PZ_STATE_BACKEND"] = "sqlserver" };
        var yaml = Minimal + "state:\n  backend: local\n";

        var project = ProjectLoader.Load(WriteProject(yaml), env);

        Assert.Equal(StateConfig.Local, project.State.Backend);
        Assert.Equal("project.yml", project.State.BackendSource);
    }

    [Fact]
    public void Unknown_backend_is_PZ0124()
    {
        var yaml = Minimal + "state:\n  backend: cassandra\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateBackendConfigInvalid);
    }

    [Fact]
    public void Backend_specific_keys_under_local_are_PZ0124()
    {
        var yaml = Minimal + "state:\n  backend: local\n  schema: pz\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateBackendConfigInvalid);
    }

    [Fact]
    public void Non_local_backend_with_no_credentials_anywhere_is_PZ0125()
    {
        var yaml = Minimal + "state:\n  backend: sqlserver\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateConnectionInvalid);
    }

    [Fact]
    public void Events_true_with_artifacts_false_is_PZ0124()
    {
        // With no runs header row (only artifacts writes it), a
        // truncated event stream has nowhere to report events_dropped and run_events rows are never
        // retention candidates -- the combination is refused, not half-honored.
        var yaml = Minimal + "state:\n  backend: sqlserver\n  artifacts: false\n  events: true\n";
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_CONNECTION_STRING"] = "Server=x;Database=y;Integrated Security=true",
        };

        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(WriteProject(yaml), env));

        Assert.Contains(ex.Errors, e =>
            e.Code == PzErrorCode.StateBackendConfigInvalid && e.Message.Contains("state.events"));
    }

    [Fact]
    public void Environment_supplied_events_with_explicit_artifacts_false_is_still_PZ0124()
    {
        // The refusal is about the effective combination, wherever each side came from -- here events
        // arrives as a host-wide PZ_STATE_EVENTS default against a project that pinned artifacts: false.
        var yaml = Minimal + "state:\n  backend: sqlserver\n  artifacts: false\n";
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_CONNECTION_STRING"] = "Server=x;Database=y;Integrated Security=true",
            ["PZ_STATE_EVENTS"] = "true",
        };

        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(WriteProject(yaml), env));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateBackendConfigInvalid);
    }

    [Fact]
    public void Events_true_with_default_artifacts_on_a_remote_backend_is_fine()
    {
        // artifacts defaults to true when the backend is not local, so plain `events: true` stays legal.
        var yaml = Minimal + "state:\n  backend: sqlserver\n  events: true\n";
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_CONNECTION_STRING"] = "Server=x;Database=y;Integrated Security=true",
        };

        var project = ProjectLoader.Load(WriteProject(yaml), env);

        Assert.True(project.State.Artifacts);
        Assert.True(project.State.Events);
    }

    [Fact]
    public void Named_connection_wins_over_the_environment_connection_string()
    {
        var env = new Dictionary<string, string> { ["PZ_STATE_CONNECTION_STRING"] = "Server=env" };
        var yaml = Minimal + "state:\n  backend: sqlserver\n  connection: ops\n";
        var dir = WriteProject(yaml);
        File.WriteAllText(Path.Combine(dir, "connections.yml"),
            "ops:\n  connector: sqlserver\n  connection_string: \"Server=named\"\n");

        var project = ProjectLoader.Load(dir, env);

        Assert.Equal("ops", project.State.Connection);
    }

    [Fact]
    public void Http_backend_takes_its_url_and_token_from_the_environment()
    {
        // An agent passes both, and neither has to appear in project.yml (the token never may -- it is
        // a credential).
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_BACKEND"] = "http",
            ["PZ_STATE_URL"] = "https://state.example/api/agents/runs/abc/state",
            ["PZ_STATE_TOKEN"] = "s3cret",
        };

        var project = ProjectLoader.Load(WriteProject(Minimal), env);

        Assert.Equal(StateConfig.Http, project.State.Backend);
        Assert.True(project.State.IsHttp);
        Assert.Equal("https://state.example/api/agents/runs/abc/state", project.State.Url);
        Assert.Equal("s3cret", project.State.Token);
        // This backend serves the keyed-state seam only; artifacts stay where they already live.
        Assert.False(project.State.Artifacts);
        Assert.False(project.State.Events);
    }

    [Fact]
    public void An_explicit_http_url_beats_its_environment_counterpart()
    {
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_URL"] = "https://ambient.example/api/agents/runs/env/state",
        };
        var yaml = Minimal + "state:\n  backend: http\n  url: https://declared.example/api/agents/runs/a/state\n";

        var project = ProjectLoader.Load(WriteProject(yaml), env);

        Assert.Equal("https://declared.example/api/agents/runs/a/state", project.State.Url);
    }

    [Fact]
    public void Http_backend_with_no_url_anywhere_is_PZ0125()
    {
        var yaml = Minimal + "state:\n  backend: http\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateConnectionInvalid);
    }

    [Fact]
    public void A_state_url_that_is_not_absolute_http_is_PZ0125()
    {
        // Caught at load time so a typo is not a PZ0518 after nodes have already run.
        var yaml = Minimal + "state:\n  backend: http\n  url: state.example/state\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateConnectionInvalid);
    }

    [Fact]
    public void Sqlserver_keys_under_the_http_backend_are_PZ0124()
    {
        var yaml = Minimal + "state:\n  backend: http\n  url: https://p.example/state\n  schema: pz\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e =>
            e.Code == PzErrorCode.StateBackendConfigInvalid && e.Message.Contains("schema"));
    }

    [Fact]
    public void A_token_written_into_project_yml_is_PZ0124_and_says_where_it_belongs()
    {
        var yaml = Minimal + "state:\n  backend: http\n  url: https://p.example/state\n  token: s3cret\n";

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(WriteProject(yaml), new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e =>
            e.Code == PzErrorCode.StateBackendConfigInvalid && e.Hint!.Contains("PZ_STATE_TOKEN"));
    }

    [Fact]
    public void Artifacts_true_under_the_http_backend_is_PZ0124()
    {
        // A host-wide PZ_STATE_ARTIFACTS=true must not silently fall through to the SQL artifact store,
        // which has no credentials under this backend.
        var env = new Dictionary<string, string>
        {
            ["PZ_STATE_URL"] = "https://p.example/api/agents/runs/a/state",
            ["PZ_STATE_ARTIFACTS"] = "true",
        };
        var yaml = Minimal + "state:\n  backend: http\n";

        var ex = Assert.Throws<PzValidationException>(() => ProjectLoader.Load(WriteProject(yaml), env));

        Assert.Contains(ex.Errors, e =>
            e.Code == PzErrorCode.StateBackendConfigInvalid && e.Message.Contains("state.artifacts"));
    }

    [Fact]
    public void Connection_naming_a_non_sqlserver_connector_is_PZ0125()
    {
        var yaml = Minimal + "state:\n  backend: sqlserver\n  connection: files\n";
        var dir = WriteProject(yaml);
        File.WriteAllText(Path.Combine(dir, "connections.yml"),
            "files:\n  connector: localfiles\n  root: \"./data\"\n");

        var ex = Assert.Throws<PzValidationException>(
            () => ProjectLoader.Load(dir, new Dictionary<string, string>()));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.StateConnectionInvalid);
    }
}
