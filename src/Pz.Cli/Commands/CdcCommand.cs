using System.CommandLine;
using System.Globalization;
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
        command.SetAction((parseResult, ct) => Status(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(), ct));
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
        command.Arguments.Add(targetArgument);
        command.SetAction((parseResult, ct) => Drop(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(targetArgument) ?? [],
            ct));
        return command;
    }

    // ---- status ----

    internal static async Task<int> Status(string projectDir, CancellationToken ct)
    {
        PzProject project;
        ConnectorRegistry registry;
        Pz.PackageManagement.Hosting.ConnectorHosts? host;
        StateBackends backends;
        try
        {
            project = ProjectLoader.Load(projectDir, SharedInputHelpers.SnapshotEnvironment());
            (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck: false, ct);
            backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
        }

        await using var connectorHost = host;

        var cdcDatasets = CdcDatasets(project).ToList();
        if (cdcDatasets.Count == 0)
        {
            Console.WriteLine("no cdc datasets in this project");
            return ExitCodes.Ok;
        }

        // Same provenance rule as RunCommand.ExecuteRun: an ambient backend is printed,
        // the untouched default stays silent.
        if (project.State.BackendSource != "default")
        {
            Console.WriteLine($"note: state backend: {backends.Description}");
        }

        var syncState = backends.SyncState;
        var anyUnhealthy = false;

        PrintHeader();

        foreach (var group in cdcDatasets.GroupBy(d => d.Source.Name, StringComparer.Ordinal))
        {
            var source = group.First().Source;
            if (!registry.TryGetSource(source.Connector, out var connector))
            {
                foreach (var (_, dataset) in group)
                {
                    PrintRow($"{source.Name}.{dataset.Name}", null, StoredToken(syncState, source.Name, dataset.Name),
                        null, "admin unsupported");
                }

                continue;
            }

            var opened = await connector.OpenAsync(new ConnectorConfig(source.Connection), ct);
            try
            {
                foreach (var (_, dataset) in group)
                {
                    var key = $"{source.Name}.{dataset.Name}";
                    var storedToken = StoredToken(syncState, source.Name, dataset.Name);
                    if (opened is not IChangeCaptureAdmin admin)
                    {
                        PrintRow(key, null, storedToken, null, "admin unsupported");
                        continue;
                    }

                    // Carry the stored token into the spec: it is what lets the connector tell "never
                    // started, the first run creates the slot" apart from "started, then lost its
                    // server-side state" -- opposite situations with opposite remediations.
                    var spec = SpecBuilder.ForSourceLoad(new SourceDatasetDef(source, dataset))
                        with { PriorSyncState = storedToken };
                    var status = await admin.GetChangeCaptureStatusAsync(spec, ct);
                    PrintRow(key, status.PositionName, storedToken, status.RetainedBytes,
                        status.Healthy ? "healthy" : "unhealthy");
                    if (!status.Healthy)
                    {
                        anyUnhealthy = true;
                        foreach (var line in status.Detail)
                        {
                            Console.WriteLine($"    {line}");
                        }
                    }
                }
            }
            finally
            {
                await opened.DisposeAsync();
            }
        }

        return anyUnhealthy ? ExitCodes.NodeFailures : ExitCodes.Ok;
    }

    private static string? StoredToken(SyncStateStore syncState, string source, string dataset) =>
        syncState.Get(SyncStateStore.Key(source, dataset))?.Token;

    private static void PrintHeader() =>
        Console.WriteLine($"{"dataset",-28} {"position",-20} {"stored token",-20} {"retained",-12} health");

    private static void PrintRow(string key, string? position, string? storedToken, long? retainedBytes, string health) =>
        Console.WriteLine(
            $"{key,-28} {position ?? "-",-20} {storedToken ?? "-",-20} " +
            $"{retainedBytes?.ToString(CultureInfo.InvariantCulture) ?? "-",-12} {health}");

    // ---- drop ----

    internal static async Task<int> Drop(string projectDir, string[] targets, CancellationToken ct)
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

        PzProject project;
        ConnectorRegistry registry;
        Pz.PackageManagement.Hosting.ConnectorHosts? host;
        StateBackends backends;
        try
        {
            project = ProjectLoader.Load(projectDir, SharedInputHelpers.SnapshotEnvironment());
            (registry, host) = await ConnectorRegistryFactory.CreateAsync(project, projectDir, noLockCheck: false, ct);
            backends = StateBackendFactory.Create(project, projectDir, TimeProvider.System);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
            return ExitCodes.ConfigError;
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
        var opened = await connector.OpenAsync(new ConnectorConfig(source.Connection), ct);
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
        if (project.State.BackendSource != "default")
        {
            Console.WriteLine($"note: state backend: {backends.Description}");
        }

        backends.SyncState.Remove(SyncStateStore.Key(sourceName, datasetName));

        PrintDropSummary(source, spec, sourceName, datasetName, positionName, project.State.IsLocal);
        return ExitCodes.Ok;
    }

    /// <summary>Postgres actually drops the replication slot server-side; SQL Server's admin drop is a
    /// deliberate no-op (server-side cdc disablement is the DBA's call) -- print the exact
    /// `sp_cdc_disable_table` statement instead of pretending anything server-side changed.
    /// <paramref name="positionName"/> is the pre-drop <see cref="ChangeCaptureStatus.PositionName"/>
    /// (the slot name for postgres, the capture instance for sqlserver); schema/table for the sqlserver
    /// remediation text come from the dataset's own entity NAME, split here rather than through a
    /// connector-internal helper the CLI cannot reference.</summary>
    private static void PrintDropSummary(ConnectionDef source, DatasetSpec spec, string sourceName, string datasetName,
        string? positionName, bool localState)
    {
        // Under a remote backend the cleared entry lives in the configured store, not .pz/state/, so
        // the summary must not call it "local".
        var entry = localState ? "pz's local sync-state entry" : "pz's sync-state entry in the configured state store";

        if (string.Equals(source.Connector, "sqlserver", StringComparison.Ordinal))
        {
            var dot = datasetName.LastIndexOf('.');
            var schema = dot < 0 ? "dbo" : datasetName[..dot];
            var table = dot < 0 ? datasetName : datasetName[(dot + 1)..];
            Console.WriteLine(
                $"{sourceName}.{datasetName}: cleared {entry} (the next run will re-snapshot).");
            Console.WriteLine(
                "SQL Server cdc was NOT disabled server-side -- pz never runs sp_cdc_disable_table. " +
                "To disable it yourself:");
            Console.WriteLine(
                $"  EXEC sys.sp_cdc_disable_table @source_schema = N'{schema}', @source_name = N'{table}', " +
                $"@capture_instance = N'{positionName}';");
            return;
        }

        if (string.Equals(source.Connector, "postgres", StringComparison.Ordinal))
        {
            Console.WriteLine(
                $"{sourceName}.{datasetName}: dropped replication slot '{positionName}' and cleared {entry} " +
                "(the next run will re-snapshot).");
            return;
        }

        Console.WriteLine(
            $"{sourceName}.{datasetName}: dropped server-side change-capture state and cleared {entry} " +
            "(the next run will re-snapshot).");
    }

    private static IEnumerable<(ConnectionDef Source, DatasetDef Dataset)> CdcDatasets(PzProject project) =>
        from source in project.Connections
        from dataset in source.Datasets
        where dataset.SyncMode?.Mode == SyncMode.Cdc
        select (source, dataset);
}
