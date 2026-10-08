using System.CommandLine;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Cli.Commands;

/// <summary>`pz cdc status` and `pz cdc drop` -- the only two places any cdc dataset's server-side
/// change-capture state is ever inspected or torn down (the
/// run path never calls <see cref="IChangeCaptureAdmin"/>). Loads the project via the same load phase
/// every other verb uses (no DAG compile needed -- neither verb touches pipelines/sinks), builds
/// <see cref="DatasetSpec"/> the exact same way <see cref="SpecBuilder.ForSourceLoad(SourceDatasetDef)"/>
/// does for a real run, and opens each cdc dataset's source through the connector registry (same
/// builtins + restored-package registry `run`/`plan` build). A connector that does not implement
/// <see cref="IChangeCaptureAdmin"/> is reported, never treated as an error: cdc admin is an optional
/// connector surface.
///
/// Sync-state is read and cleared through whichever backend <c>state:</c> resolved to
/// (<see cref="StateBackendFactory"/>), exactly as the run path does -- a hardcoded
/// <c>SyncStateStore.Local</c> here would make `pz cdc status` report empty state and, far worse, make
/// `pz cdc drop` clear a local file the next run never reads under <c>backend: sqlserver</c>: the
/// operator would expect a re-snapshot while the run resumed from the remote token.
/// Both verbs already do a full project load
/// (they need connections + the connector registry), so unlike `pz state`/`pz clean` there is no
/// no-project-load property to preserve and <see cref="StateBackendFactory.Create(PzProject, string, TimeProvider, string?)"/>
/// composes straight off the loaded project.</summary>
internal static class CdcCommand
{
    public static Command Create()
    {
        var cdc = new Command("cdc", "Inspect and tear down server-side change-capture (cdc) state");
        cdc.Subcommands.Add(CreateStatus());
        cdc.Subcommands.Add(CreateDrop());
        return cdc;
    }

    private static Command CreateStatus()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var command = new Command("status",
            "Report server-side change-capture state for every cdc dataset in the project. " +
            "Exit 0 when every reported dataset is healthy, 1 when any is unhealthy.");
        command.Options.Add(projectOption);
        command.Options.Add(SharedOptions.LogFormat);
        command.Options.Add(SharedOptions.StateUrl);
        command.SetAction((parseResult, ct) => Status(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(SharedOptions.LogFormat),
            parseResult.GetValue(SharedOptions.StateUrl),
            ct));
        return command;
    }

    private static Command CreateDrop()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var targetArgument = new Argument<string[]>("target")
        {
            Description = "The cdc dataset to drop, as <source>.<dataset> (exactly one required -- no bulk drop)",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var command = new Command("drop",
            "Drop server-side change-capture state for ONE cdc dataset and clear pz's sync-state " +
            "entry for it (in whichever store `state:` resolved to), so the next run re-snapshots.");
        command.Options.Add(projectOption);
        command.Options.Add(SharedOptions.LogFormat);
        command.Options.Add(SharedOptions.StateUrl);
        command.Arguments.Add(targetArgument);
        command.SetAction((parseResult, ct) => Drop(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(targetArgument) ?? [],
            parseResult.GetValue(SharedOptions.LogFormat),
            parseResult.GetValue(SharedOptions.StateUrl),
            ct));
        return command;
    }

    // ---- status ----

    internal static async Task<int> Status(string projectDir, string? logFormatRaw, string? stateUrlRaw, CancellationToken ct)
    {
        if (!RunCommand.TryParseLogFormat(logFormatRaw, out var logFormat))
        {
            Console.Error.WriteLine($"error: invalid --log-format value '{logFormatRaw}' (expected 'text' or 'json')");
            return ExitCodes.ConfigError;
        }

        var (loadExit, project, registry, host, backends) = await LoadAsync(projectDir, stateUrlRaw, ct);
        if (loadExit is { } exit)
        {
            return exit;
        }

        await using var connectorHost = host;

        var output = CdcOutput.For(logFormat, TimeProvider.System);
        var cdcDatasets = CdcDatasets(project).ToList();
        if (cdcDatasets.Count == 0)
        {
            output.NoCdcDatasets();

            return ExitCodes.Ok;
        }

        // Same provenance rule as RunCommand.ExecuteRun: an ambient backend is printed,
        // the untouched default stays silent.
        if (project.State.BackendSource != "default")
        {
            output.Note($"note: state backend: {backends.Description}");
        }

        var syncState = backends.SyncState;
        var anyUnhealthy = false;

        output.StatusHeader();

        foreach (var group in cdcDatasets.GroupBy(d => d.Source.Name, StringComparer.Ordinal))
        {
            var source = group.First().Source;
            if (!registry.TryGetSource(source.Connector, out var connector))
            {
                foreach (var (_, dataset) in group)
                {
                    output.Status(Unsupported(source, dataset, StoredToken(syncState, source.Name, dataset.Name)));
                }

                continue;
            }

            var opened = await connector.OpenAsync(registry.ConfigFor(source), ct);
            try
            {
                foreach (var (_, dataset) in group)
                {
                    var key = $"{source.Name}.{dataset.Name}";
                    var storedToken = StoredToken(syncState, source.Name, dataset.Name);
                    if (opened is not IChangeCaptureAdmin admin)
                    {
                        output.Status(Unsupported(source, dataset, storedToken));
                        continue;
                    }

                    // Carry the stored token into the spec: it is what lets the connector tell "never
                    // started, the first run creates the slot" apart from "started, then lost its
                    // server-side state" -- opposite situations with opposite remediations.
                    var spec = SpecBuilder.ForSourceLoad(new SourceDatasetDef(source, dataset))
                        with { PriorSyncState = storedToken };
                    var status = await admin.GetChangeCaptureStatusAsync(spec, ct);
                    output.Status(new CdcStatusRow(key, source.Connector, AdminSupported: true, status.Healthy,
                        status.PositionName, storedToken, status.RetainedBytes, status.Detail));
                    anyUnhealthy |= !status.Healthy;
                }
            }
            finally
            {
                await opened.DisposeAsync();
            }
        }

        return anyUnhealthy ? ExitCodes.NodeFailures : ExitCodes.Ok;
    }

    /// <summary>The load phase both verbs share. `--state-url` is applied before the state backends are
    /// composed, so status reads and drop clears the store a platform's runs use, the same precedence
    /// `pz run` has (it outranks project.yml's state: and PZ_STATE_*).</summary>
    private static async Task<(int? Exit, PzProject Project, ConnectorRegistry Registry,
        Pz.PackageManagement.Hosting.ConnectorHosts? Host, StateBackends Backends)> LoadAsync(
        string projectDir, string? stateUrlRaw, CancellationToken ct)
    {
        try
        {
            var env = SharedInputHelpers.SnapshotEnvironment();
            var project = ProjectLoader.Load(projectDir, env);
            if (!StateUrlOverride.TryApply(project, stateUrlRaw, env, out project, out var stateUrlError))
            {
                Console.Error.WriteLine($"error: {stateUrlError}");
                return (ExitCodes.ConfigError, null!, null!, null, null!);
            }

            var (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck: false, ct);
            var backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System, ct: ct);
            return (null, project, registry, host, backends);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return (ExitCodes.ConfigError, null!, null!, null, null!);
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return (ExitCodes.ConfigError, null!, null!, null, null!);
        }
    }

    private static string? StoredToken(SyncStateStore syncState, string source, string dataset) =>
        syncState.Get(SyncStateStore.Key(source, dataset))?.Token;

    private static CdcStatusRow Unsupported(ConnectionDef source, DatasetDef dataset, string? storedToken) =>
        new($"{source.Name}.{dataset.Name}", source.Connector, AdminSupported: false, Healthy: null,
            PositionName: null, storedToken, RetainedBytes: null, Detail: []);

    // ---- drop ----

    internal static async Task<int> Drop(string projectDir, string[] targets, string? logFormatRaw, string? stateUrlRaw,
        CancellationToken ct)
    {
        if (targets.Length != 1)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CdcTargetInvalid}: usage: pz cdc drop <source>.<dataset> (exactly one target, no bulk drop)");
            return ExitCodes.ConfigError;
        }

        var parts = targets[0].Split('.');
        if (parts is not [{ Length: > 0 } sourceName, { Length: > 0 } datasetName])
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CdcTargetInvalid}: usage: pz cdc drop <source>.<dataset> -- got '{targets[0]}'");
            return ExitCodes.ConfigError;
        }

        if (!RunCommand.TryParseLogFormat(logFormatRaw, out var logFormat))
        {
            Console.Error.WriteLine($"error: invalid --log-format value '{logFormatRaw}' (expected 'text' or 'json')");
            return ExitCodes.ConfigError;
        }

        var (loadExit, project, registry, host, backends) = await LoadAsync(projectDir, stateUrlRaw, ct);
        if (loadExit is { } exit)
        {
            return exit;
        }

        await using var connectorHost = host;

        var source = project.Connections.FirstOrDefault(s => string.Equals(s.Name, sourceName, StringComparison.Ordinal));
        var dataset = source?.Datasets.FirstOrDefault(d => string.Equals(d.Name, datasetName, StringComparison.Ordinal));
        if (source is null || dataset is null || dataset.SyncMode?.Mode != SyncMode.Cdc)
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CdcTargetNotFound}: '{targets[0]}' is not a cdc dataset in this project " +
                "(declare `sync: { mode: cdc }` on it, or check `pz cdc status` for the list)");
            return ExitCodes.ConfigError;
        }

        if (!registry.TryGetSource(source.Connector, out var connector))
        {
            Console.Error.WriteLine(
                $"error {PzErrorCode.CdcTargetNotFound}: source '{sourceName}' connector '{source.Connector}' " +
                "is not registered (admin unsupported)");
            return ExitCodes.ConfigError;
        }

        var spec = SpecBuilder.ForSourceLoad(new SourceDatasetDef(source, dataset));
        var opened = await connector.OpenAsync(registry.ConfigFor(source), ct);
        string? positionName;
        try
        {
            if (opened is not IChangeCaptureAdmin admin)
            {
                Console.Error.WriteLine(
                    $"error {PzErrorCode.CdcTargetNotFound}: connector '{source.Connector}' " +
                    "does not support cdc admin operations (admin unsupported)");
                return ExitCodes.ConfigError;
            }

            // Status is read BEFORE the drop, while the admin connection is still open, so the summary
            // below can print the exact slot/capture-instance name through the public ABI record
            // (ChangeCaptureStatus.PositionName) instead of reaching into connector internals.
            var status = await admin.GetChangeCaptureStatusAsync(spec, ct);
            positionName = status.PositionName;
            await admin.DropChangeCaptureStateAsync(spec, ct);
        }
        finally
        {
            await opened.DisposeAsync();
        }

        // The seam this verb exists for: the entry must vanish from the SAME store the next run reads
        // (backends.SyncState), or the drop is a silent no-op under a remote backend.
        var output = CdcOutput.For(logFormat, TimeProvider.System);
        if (project.State.BackendSource != "default")
        {
            output.Note($"note: state backend: {backends.Description}");
        }

        backends.SyncState.Remove(SyncStateStore.Key(sourceName, datasetName));

        // SQL Server's admin drop is a deliberate no-op (server-side cdc disablement is the DBA's call),
        // so the summary carries the exact `sp_cdc_disable_table` statement instead of pretending anything
        // server-side changed. Schema/table come from the dataset's own entity NAME, split here rather
        // than through a connector-internal helper the CLI cannot reference.
        var isSqlServer = string.Equals(source.Connector, "sqlserver", StringComparison.Ordinal);
        var remediation = new List<string>();
        if (isSqlServer)
        {
            var dot = datasetName.LastIndexOf('.');
            var schema = dot < 0 ? "dbo" : datasetName[..dot];
            var table = dot < 0 ? datasetName : datasetName[(dot + 1)..];
            remediation.Add(
                $"EXEC sys.sp_cdc_disable_table @source_schema = N'{schema}', @source_name = N'{table}', " +
                $"@capture_instance = N'{positionName}';");
        }

        output.Dropped(new CdcDropSummary($"{sourceName}.{datasetName}", source.Connector, positionName,
            ServerSideDropped: !isSqlServer, remediation, project.State.IsLocal));
        return ExitCodes.Ok;
    }

    private static IEnumerable<(ConnectionDef Source, DatasetDef Dataset)> CdcDatasets(PzProject project) =>
        from source in project.Connections
        from dataset in source.Datasets
        where dataset.SyncMode?.Mode == SyncMode.Cdc
        select (source, dataset);
}
