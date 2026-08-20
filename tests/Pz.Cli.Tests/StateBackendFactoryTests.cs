using Pz.Cli;
using Pz.Cli.Rendering;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.State;

namespace Pz.Cli.Tests;

/// <summary><see cref="StateBackendFactory"/> composes the
/// three state seams against whichever backend <c>project.State</c> resolved to. Every test here runs
/// without docker -- <see cref="StateBackendFactory.Create"/> must never open a connection (only the
/// stores it returns do, lazily, on first use), which is exactly what makes these tests possible.</summary>
public sealed class StateBackendFactoryTests
{
    private static PzProject Minimal() => new(
        "demo", "1.0", new EngineConfig(), new Dictionary<string, object?>(),
        Connectors: [], Connections: [], Pipelines: []);

    [Fact]
    public void Local_config_yields_the_local_stores()
    {
        var project = Minimal() with { State = StateConfig.Default };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System);

        Assert.IsType<WatermarkStore>(backends.Watermarks);
        Assert.IsType<SyncStateStore>(backends.SyncState);
        Assert.IsType<LocalRunArtifactStore>(backends.Artifacts);
        Assert.Null(backends.EventSink);
        Assert.Equal("local (default)", backends.Description);

        // A no-op for local -- proves EnsureSchema never touches the network for this backend.
        backends.EnsureSchema();
    }

    [Fact]
    public void Http_config_moves_only_the_keyed_state_seams()
    {
        // Watermarks and sync-state go to the server; run
        // artifacts stay local, there is no event sink, and there is no schema to migrate. Constructing
        // this must not touch the network -- the URL below points nowhere and this test still passes.
        var state = new StateConfig(StateConfig.Http, null, null, "pz", Artifacts: false, Events: false,
            BackendSource: "PZ_STATE_BACKEND", Url: "https://p.example/api/agents/runs/a/state",
            Token: "s3cret");
        var project = Minimal() with { State = state };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System, "run-1");

        Assert.IsType<LocalRunArtifactStore>(backends.Artifacts);
        Assert.Null(backends.EventSink);
        Assert.Equal("http (from PZ_STATE_BACKEND)", backends.Description);
        backends.EnsureSchema();
    }

    [Fact]
    public void The_description_names_the_environment_variable_when_that_is_the_source()
    {
        var state = new StateConfig(StateConfig.SqlServer, null, "Server=x;Database=y", "pz",
            Artifacts: true, Events: false, BackendSource: "PZ_STATE_BACKEND");
        var project = Minimal() with { State = state };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System);

        Assert.Equal("sqlserver (from PZ_STATE_BACKEND)", backends.Description);
    }

    [Fact]
    public void The_description_names_project_yml_when_that_is_the_source()
    {
        var state = StateConfig.Default with { BackendSource = "project.yml" };
        var project = Minimal() with { State = state };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System);

        Assert.Equal("local (from project.yml)", backends.Description);
    }

    [Fact]
    public void Events_false_yields_no_event_sink_even_on_a_remote_backend()
    {
        var state = new StateConfig(StateConfig.SqlServer, null, "Server=x;Database=y", "pz",
            Artifacts: true, Events: false, BackendSource: "project.yml");
        var project = Minimal() with { State = state };

        Assert.Null(StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System, "run-1").EventSink);
    }

    [Fact]
    public async Task Events_true_with_a_run_id_yields_a_sql_event_renderer()
    {
        var state = new StateConfig(StateConfig.SqlServer, null, "Server=x;Database=y", "pz",
            Artifacts: true, Events: true, BackendSource: "project.yml");
        var project = Minimal() with { State = state };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System, "run-1");

        Assert.IsType<SqlEventRenderer>(backends.EventSink);
        await ((IAsyncDisposable)backends.EventSink!).DisposeAsync();
    }

    /// <summary>`pz state show` has no run in progress, so omitting <c>runId</c> must suppress the event
    /// sink even when <c>state.events</c> is true -- there is no run for it to attach events to.</summary>
    [Fact]
    public void Events_true_with_no_run_id_still_yields_no_event_sink()
    {
        var state = new StateConfig(StateConfig.SqlServer, null, "Server=x;Database=y", "pz",
            Artifacts: true, Events: true, BackendSource: "project.yml");
        var project = Minimal() with { State = state };

        Assert.Null(StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System).EventSink);
    }

    [Fact]
    public void Artifacts_false_on_a_remote_backend_still_uses_the_local_artifact_store()
    {
        var state = new StateConfig(StateConfig.SqlServer, null, "Server=x;Database=y", "pz",
            Artifacts: false, Events: false, BackendSource: "project.yml");
        var project = Minimal() with { State = state };

        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System);

        Assert.IsType<LocalRunArtifactStore>(backends.Artifacts);
    }

    [Fact]
    public void A_named_connection_resolves_credentials_through_the_sqlserver_connector_builder()
    {
        var connection = new ConnectionDef("ops", "sqlserver",
            new Dictionary<string, object?> { ["host"] = "db.internal", ["database"] = "pz_state" },
            Datasets: [], FilePath: "connections.yml");
        var state = new StateConfig(StateConfig.SqlServer, "ops", null, "pz",
            Artifacts: true, Events: false, BackendSource: "project.yml");
        var project = Minimal() with { State = state, Connections = [connection] };

        // Must not throw, and must not open a connection (no docker, no server at db.internal).
        var backends = StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System);

        Assert.IsType<WatermarkStore>(backends.Watermarks);
    }

    [Fact]
    public void A_named_connection_missing_required_fields_is_PZ0125()
    {
        var connection = new ConnectionDef("ops", "sqlserver",
            new Dictionary<string, object?>(), Datasets: [], FilePath: "connections.yml");
        var state = new StateConfig(StateConfig.SqlServer, "ops", null, "pz",
            Artifacts: true, Events: false, BackendSource: "project.yml");
        var project = Minimal() with { State = state, Connections = [connection] };

        var ex = Assert.Throws<PzConfigException>(
            () => StateBackendFactory.Create(project, "/tmp/p", TimeProvider.System));

        Assert.Equal(PzErrorCode.StateConnectionInvalid, ex.Error.Code);
    }
}
