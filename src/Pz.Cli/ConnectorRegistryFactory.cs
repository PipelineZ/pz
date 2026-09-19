using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;
using Pz.PackageManagement.Restore;

namespace Pz.Cli;

/// <summary>Builds the connector registry: builtins + (when the project declares non-builtin
/// connectors) a <see cref="ProcessConnectorHost"/> over .pz/packages verified against pz.lock.json.
/// External connectors are hosted out of process only: a package whose manifest declares runtime
/// <c>"dotnet"</c> — or ships no manifest, which means the same — is refused with PZ0360, never
/// ALC-loaded into the engine process. Builtins are the only in-process connectors.</summary>
internal static class ConnectorRegistryFactory
{
    /// <summary>Throws PzValidationException (PZ0321/PZ0322/host PZ03xx mapped) on lock/host problems.
    /// Hosts is null when all declared connectors are builtin. Caller disposes Hosts.
    ///
    /// <para><paramref name="runId"/> scopes an out-of-process connector's sockets to the run directory;
    /// a verb with no run (validate/plan/connectors/mcp) passes none and gets a temp root the returned
    /// <see cref="ConnectorHosts"/> owns and deletes. See <see cref="ProcessSocketRoot"/>.</para>
    ///
    /// <para><paramref name="otelEndpoint"/> is the same OTLP endpoint the engine exports to;
    /// out-of-process connectors receive it in their handshake and export their own spans there. Null
    /// keeps every child telemetry-free.</para>
    ///
    /// <para><paramref name="connectorLog"/> receives every process-hosted connector's <c>ILogger</c>
    /// output as <c>(connection, level, message)</c> -- level already named
    /// (<c>"trace"</c>/.../<c>"critical"</c>), message already redacted (<see cref="MessageRedaction"/>)
    /// and, when the connector logged alongside an exception, folded with the exception's own message.
    /// Null (the default) drops them, same as passing no logSink to <see cref="ProcessConnectorHost"/>
    /// directly. The caller is expected to route this to <c>IRunEvents.SafeConnectorLog</c> once its own
    /// event bus exists -- registry construction itself spawns nothing (see
    /// <see cref="ProcessConnectorHost.LoadFromDirectory"/>'s own doc), so nothing this delegate reports
    /// can fire before the caller is ready for it.</para></summary>
    public static async Task<(ConnectorRegistry Registry, ConnectorHosts? Hosts)> CreateAsync(
        PzProject project, string projectDir, bool noLockCheck, CancellationToken ct, string? runId = null,
        Uri? otelEndpoint = null, Action<string, string, string>? connectorLog = null)
    {
        ct.ThrowIfCancellationRequested();

        var registry = BuiltinConnectors.CreateRegistry();
        var nonBuiltin = project.Connectors
            .Where(c => !BuiltinConnectors.PackageIds.Contains(c.Package))
            .ToArray();

        if (nonBuiltin.Length == 0)
        {
            return (registry, null);
        }

        var lockPath = Path.Combine(projectDir, "pz.lock.json");
        var lockFile = ReadLockOrThrow(lockPath);

        var packagesDir = Path.Combine(projectDir, ".pz", "packages");
        var nonBuiltinRefs = nonBuiltin
            .Select(c => new ConnectorPackageRef(c.Package, c.Version))
            .ToArray();

        if (noLockCheck)
        {
            Console.Error.WriteLine(
                "warning: --no-lock-check set; skipping pz.lock.json drift verification — connectors " +
                "may not match what 'pz restore' last installed");
        }
        else
        {
            var findings = DriftChecker.Verify(
                nonBuiltinRefs, lockFile, packagesDir, System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier);
            if (findings.Count > 0)
            {
                throw new PzValidationException(findings
                    .Select(f => new PzError(f.Code, f.Message, null, null, f.Hint))
                    .ToArray());
            }
        }

        var installRefs = nonBuiltin
            .Select(c =>
            {
                var locked = lockFile.Packages.FirstOrDefault(p =>
                    p.Requested && string.Equals(p.Id, c.Package, StringComparison.OrdinalIgnoreCase));
                return new ConnectorPackageRef(c.Package, locked?.Version ?? c.Version);
            })
            .ToArray();

        // Every external package must declare runtime "process" before any host is constructed —
        // aggregated, so a project with three dotnet-runtime packages hears about all three at once.
        var rejected = new List<PzError>();
        var outOfProcessRefs = new List<ConnectorPackageRef>();
        foreach (var packageRef in installRefs)
        {
            var runtime = DeclaredRuntime(packagesDir, packageRef);
            if (runtime == "process")
            {
                outOfProcessRefs.Add(packageRef);
            }
            else
            {
                var declared = runtime is null
                    ? "ships no pz.connector.json manifest"
                    : $"declares runtime '{runtime}'";
                rejected.Add(new PzError(
                    PzErrorCode.ExternalConnectorNotOutOfProcess,
                    $"connector package '{packageRef.PackageId}' ({packageRef.Version}) {declared} — " +
                    "external connectors are hosted out of process only",
                    null, null,
                    "use a connector published as a runtime: \"process\" (PCP) package, or a builtin connector"));
            }
        }

        if (rejected.Count > 0)
        {
            throw new PzValidationException(rejected.ToArray());
        }

        ProcessConnectorHost? processHost = null;
        string? ownedSocketRoot = null;
        void Warn(string message) => Console.Error.WriteLine($"warning: {message}");
        try
        {
            if (outOfProcessRefs.Count > 0)
            {
                var (socketRoot, owned) = ProcessSocketRoot.Resolve(projectDir, runId);
                ownedSocketRoot = owned ? socketRoot : null;
                processHost = ProcessConnectorHost.LoadFromDirectory(
                    packagesDir, outOfProcessRefs, socketRoot, warn: Warn,
                    logSink: connectorLog is null ? null : (connection, level, message, fields) =>
                        connectorLog(connection, LevelName(level), MessageRedaction.Redact(FoldException(message, fields))),
                    telemetry: new HostTelemetry(runId, otelEndpoint));
            }
        }
        catch (Exception ex)
        {
            // Catch-all, not just ConnectorHostException: the second host failing does not un-load the
            // first, and resolving the socket root touches the filesystem (IOException,
            // UnauthorizedAccessException). Reclaim whatever did get built — and the temp root created
            // for it — through the composite that would otherwise have owned it.
            await new ConnectorHosts(processHost, ownedSocketRoot).DisposeAsync().ConfigureAwait(false);
            if (ex is ConnectorHostException hostFailure)
            {
                throw new PzValidationException([
                    new PzError(hostFailure.Code, hostFailure.Message, null, null, hostFailure.Hint),
                ]);
            }

            // Rethrown as-is: a cancellation is not a config error, and a bare `throw` keeps the
            // original stack for anything that reaches the CLI's fatal handler.
            throw;
        }

        var hosts = new ConnectorHosts(processHost, ownedSocketRoot);
        try
        {
            if (processHost is not null)
            {
                Register(processHost.Installed, processHost.Get, registry, nonBuiltin);
            }
        }
        catch
        {
            // Every throw past this point happens before the caller's `await using` exists, so the hosts
            // (and the socket root created for them) have to be reclaimed here or not at all.
            await hosts.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return (registry, hosts);
    }

    /// <summary>Registers every connector one host reports under both directions it implements. Which
    /// directions a hosted connector actually offers is the caller's question, exactly as it is for a
    /// builtin — a connector implementing only one interface registers only on that side. Registered
    /// as hosted, so the engine threads each connection's name in under
    /// <see cref="ConnectorRegistry.InstanceIdKey"/> for the host to name the instance by.</summary>
    private static void Register(
        IReadOnlyList<ConnectorInfo> installed, Func<string, IConnector> get, ConnectorRegistry registry,
        IReadOnlyList<ConnectorRequirement> nonBuiltin)
    {
        foreach (var info in installed)
        {
            var instance = get(info.Name);
            if (instance is ISourceConnector source)
            {
                RegisterOrThrowCollision(() => registry.AddSource(info.Name, source, hosted: true), info.Name, nonBuiltin);
            }

            if (instance is ISinkConnector sink)
            {
                RegisterOrThrowCollision(() => registry.AddSink(info.Name, sink, hosted: true), info.Name, nonBuiltin);
            }
        }
    }

    /// <summary>A package's manifest <c>runtime</c> is the single source of truth for whether it may be
    /// hosted at all, and reading it is one small JSON read with no assembly load. Missing manifest
    /// (null) and declared runtime are reported separately so the PZ0360 message names what the package
    /// actually said; a malformed manifest surfaces as the reader's own PZ0306 rather than a PZ0360
    /// that would misattribute the problem.</summary>
    private static string? DeclaredRuntime(string packagesDir, ConnectorPackageRef packageRef)
    {
        try
        {
            var packageDir = Path.Combine(packagesDir, packageRef.PackageId, packageRef.Version);
            // The reader defaults an absent runtime field to "dotnet"; only an absent manifest is null.
            return ManifestReader.TryRead(packageDir) is { } manifest ? manifest.Runtime ?? "dotnet" : null;
        }
        catch (ConnectorHostException ex)
        {
            throw new PzValidationException([new PzError(ex.Code, ex.Message, null, null, ex.Hint)]);
        }
    }

    /// <summary><see cref="ConnectorRegistry.AddSource"/>/<c>AddSink</c> throw
    /// <see cref="InvalidOperationException"/> when a hosted connector's name collides with an
    /// already-registered one (always a builtin here: the process host enforces its own PZ0305
    /// cross-package check within its package set before any registration happens). Translate that into
    /// the same PZ0305 error family, naming both
    /// the colliding builtin name and the hosted package(s) that could have produced it.</summary>
    private static void RegisterOrThrowCollision(
        Action register, string connectorName, IReadOnlyList<ConnectorRequirement> nonBuiltin)
    {
        try
        {
            register();
        }
        catch (InvalidOperationException)
        {
            throw new PzValidationException([new PzError(
                PzErrorCode.ConnectorNotInstalled,
                $"hosted connector package {DescribePackages(nonBuiltin)} registers the connector name " +
                $"'{connectorName}', which collides with the builtin '{connectorName}' connector",
                null, null,
                "rename the connector or remove the package")]);
        }
    }

    /// <summary>Names the exact declared non-builtin package when it is the only one restored (the
    /// common case, and the only one this can name with certainty — the host does not expose which of
    /// several hosted packages registered a given connector name); otherwise lists every
    /// declared non-builtin package id as a candidate rather than guessing.</summary>
    private static string DescribePackages(IReadOnlyList<ConnectorRequirement> nonBuiltin) =>
        nonBuiltin.Count == 1
            ? $"'{nonBuiltin[0].Package}'"
            : "one of " + string.Join(", ", nonBuiltin.Select(c => $"'{c.Package}'").OrderBy(p => p, StringComparer.Ordinal));

    private static LockFile ReadLockOrThrow(string lockPath)
    {
        try
        {
            return LockFileWriter.Read(lockPath) ?? throw new PzValidationException([
                new PzError(PzErrorCode.LockMissing,
                    "pz.lock.json is missing but the project declares non-builtin connectors.",
                    null, null, "run 'pz restore'"),
            ]);
        }
        catch (RestoreException ex)
        {
            throw new PzValidationException([new PzError(ex.Code, ex.Message, null, null, ex.Hint)]);
        }
    }

    /// <summary>The wire <c>LogEvent.level</c> int is <c>Microsoft.Extensions.Logging.LogLevel</c>'s own
    /// ordinal (0 trace .. 5 critical -- see <c>pz_connector.proto</c>'s comment on the field); named
    /// here rather than by referencing that package, which no other type in this project needs. An
    /// out-of-range value (a future SDK level pz does not know about yet) names as "unknown" rather than
    /// throwing -- a log line is best-effort observability, never worth failing a run over.</summary>
    internal static string LevelName(int level) => level switch
    {
        0 => "trace",
        1 => "debug",
        2 => "info",
        3 => "warn",
        4 => "error",
        5 => "critical",
        _ => "unknown",
    };

    /// <summary>Closes the gap <see cref="Pz.Connectors.Sdk.HostLoggerProvider"/>'s wire fields open but
    /// nothing downstream reads yet: an exception logged alongside a message carries its own
    /// <c>Message</c> in the <c>exceptionMessage</c> field (the <c>formatter</c> callback that produced
    /// the log's own <c>Message</c> does not include it unless the connector's own log template did).
    /// Folded onto the end so the single <c>connector_log</c> event still carries the operator-actionable
    /// half of an error a bare "unhandled InvalidOperationException" would otherwise discard.</summary>
    internal static string FoldException(string message, IReadOnlyDictionary<string, string> fields) =>
        fields.TryGetValue("exceptionMessage", out var exceptionMessage) && exceptionMessage.Length > 0
            ? $"{message}: {exceptionMessage}"
            : message;
}
