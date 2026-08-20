using Pz.Cli.Rendering;
using Pz.Connector.SqlServer;
using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.State;
using Pz.State.Http;
using Pz.State.SqlServer;

namespace Pz.Cli;

/// <summary>The three state seams, composed against whichever backend <c>project.State</c> resolved to.
/// The one place a real `pz run` picks local files vs. a SQL Server store.</summary>
/// <summary><see cref="EnsureSchema"/> is a no-op for the local backend and
/// <c>SqlStateSchema.EnsureCurrent</c> (a real network call) for SQL Server -- kept as a delegate rather
/// than exposing the underlying <see cref="Pz.State.SqlServer.SqlStateConnection"/> so
/// <see cref="StateBackendFactory.Create"/> can stay connection-free while still letting
/// <c>RunCommand.ExecuteRun</c> invoke schema migration explicitly, once, during its load phase --
/// PZ0518/PZ0519 surface before any node executes rather than at the first watermark write.</summary>
internal sealed record StateBackends(
    WatermarkStore Watermarks,
    SchemaBaselineStore Schemas,
    SyncStateStore SyncState,
    IRunArtifactStore Artifacts,
    IEventRenderer? EventSink,
    string Description,
    Action EnsureSchema);

/// <summary>Deliberately stateless/static: every construction below is pure/local (a
/// <see cref="SqlStateConnection"/> only stores a connection string; the three SQL stores and the event
/// sink only store their constructor arguments) -- nothing here opens a socket. The first real network
/// call happens lazily, inside whichever store method a caller invokes first (or, for the schema, at the
/// explicit <c>SqlStateSchema.EnsureCurrent</c> call <c>RunCommand.ExecuteRun</c> makes during its load
/// phase). This is what keeps <see cref="Create"/> testable without docker.</summary>
internal static class StateBackendFactory
{
    /// <summary><see cref="SqlEventSink"/> binds a run id at construction, so a caller that has one --
    /// <c>RunCommand.ExecuteRun</c> -- passes <paramref name="runId"/> and
    /// gets a real event sink when <c>state.events</c> is on; a caller with no run in progress --
    /// <c>pz state show</c> -- omits it, which suppresses the event sink regardless of
    /// <c>state.events</c>, since there is no run for it to attach events to.</summary>
    public static StateBackends Create(PzProject project, string projectDir, TimeProvider time, string? runId = null) =>
        Create(project.State, project.Name, project.Connections, projectDir, time, runId);

    /// <summary>The same composition from just the three things it actually needs, for callers that must
    /// NOT load a whole project to reach the state store -- `pz state` and `pz clean`, via
    /// <c>ProjectLoader.LoadStateOnly</c>. Routing those two through the full loader would cost them
    /// their documented no-project-load property, and would let a broken connections.yml block the very
    /// verbs you reach for when config is broken.</summary>
    public static StateBackends Create(StateConfig state, string projectName,
        IReadOnlyList<ConnectionDef> connections, string projectDir, TimeProvider time, string? runId = null)
    {
        var description = Describe(state);

        if (state.IsLocal)
        {
            var stateDir = Path.Combine(projectDir, ".pz", "state");
            return new StateBackends(
                WatermarkStore.Local(stateDir),
                SchemaBaselineStore.Local(stateDir),
                SyncStateStore.Local(stateDir),
                new LocalRunArtifactStore(projectDir),
                EventSink: null,
                description,
                EnsureSchema: static () => { });
        }

        if (state.IsHttp)
        {
            // This backend implements the keyed-state seam only -- watermarks and sync-state move to
            // the server, run artifacts stay local, and there is no
            // event sink (PZ0124 already refused `artifacts: true`/`events: true` at load time). The
            // endpoint is not disposed: it lives exactly as long as the process that built it, the same
            // way SqlStateConnection holds its connection string for the run.
            var endpoint = new HttpStateEndpoint(state.Url!, state.Token);

            return new StateBackends(
                new WatermarkStore(new HttpKeyedStateStore<Watermark>(
                    endpoint, "watermarks", WatermarkStore.ReadEntry, WatermarkStore.WriteEntry)),
                new SchemaBaselineStore(new HttpKeyedStateStore<SchemaBaseline>(
                    endpoint, "schemas", SchemaBaselineStore.ReadEntry, SchemaBaselineStore.WriteEntry)),
                new SyncStateStore(new HttpKeyedStateStore<SyncState>(
                    endpoint, "sync-state", SyncStateStore.ReadEntry, SyncStateStore.WriteEntry)),
                new LocalRunArtifactStore(projectDir),
                EventSink: null,
                description,
                // No schema to migrate: the server owns the storage and its own migrations.
                EnsureSchema: static () => { });
        }

        var connectionString = ResolveConnectionString(state, connections);
        var connection = new SqlStateConnection(connectionString, state.Schema);

        var watermarks = new WatermarkStore(new SqlKeyedStateStore<Watermark>(
            connection, "watermarks", WatermarkStore.ReadEntry, WatermarkStore.WriteEntry));
        var schemas = new SchemaBaselineStore(new SqlKeyedStateStore<SchemaBaseline>(
            connection, "schemas", SchemaBaselineStore.ReadEntry, SchemaBaselineStore.WriteEntry));
        var syncState = new SyncStateStore(new SqlKeyedStateStore<SyncState>(
            connection, "sync-state", SyncStateStore.ReadEntry, SyncStateStore.WriteEntry));

        IRunArtifactStore artifacts = state.Artifacts
            ? new SqlRunArtifactStore(connection, projectName)
            : new LocalRunArtifactStore(projectDir);

        IEventRenderer? eventSink = state.Events && runId is not null
            ? new SqlEventRenderer(new SqlEventSink(connection, runId, time))
            : null;

        return new StateBackends(watermarks, schemas, syncState, artifacts, eventSink, description,
            EnsureSchema: () => SqlStateSchema.EnsureCurrent(connection));
    }

    /// <summary>The value is ambient by design when it comes from the environment, so its provenance is
    /// printed rather than hidden. <see cref="StateConfig.BackendSource"/> is already exactly "default",
    /// "project.yml", or an environment variable's name (see <c>ProjectLoader.ParseStateConfig</c>) --
    /// this only wraps it into a sentence.</summary>
    private static string Describe(StateConfig state) => state.BackendSource switch
    {
        "default" => $"{state.Backend} (default)",
        "project.yml" => $"{state.Backend} (from project.yml)",
        var source => $"{state.Backend} (from {source})",
    };

    /// <summary>Credential resolution: <see cref="StateConfig.Connection"/> (a
    /// connections.yml entry) wins over <see cref="StateConfig.ConnectionString"/>. Both the named
    /// entry's existence and its <c>connector: sqlserver</c> match are already validated at project-load
    /// time (<c>ProjectLoader.ValidateStateConnection</c>, PZ0125) -- so the lookup below never misses,
    /// and this only has to turn the connection's config dict into a connection string, reusing
    /// the exact builder <see cref="SqlServerConnector"/> already uses for ordinary sqlserver
    /// sources/sinks, so a state connection and a data connection resolve credentials identically.</summary>
    private static string ResolveConnectionString(StateConfig state, IReadOnlyList<ConnectionDef> connections)
    {
        if (state.Connection is not { } name)
        {
            return state.ConnectionString!; // Guaranteed by ValidateStateConnection when Connection is null.
        }

        var def = connections.First(c => string.Equals(c.Name, name, StringComparison.Ordinal));
        try
        {
            return SqlServerConnector.BuildConnectionString(new ConnectorConfig(def.Connection));
        }
        catch (PzConnectorException ex)
        {
            throw new PzConfigException(new PzError(PzErrorCode.StateConnectionInvalid,
                $"state.connection '{name}' is missing required sqlserver connection field(s): {ex.Message}",
                "project.yml", null,
                $"add the missing field(s) (host, database) to the '{name}' connection in connections.yml"));
        }
    }
}
