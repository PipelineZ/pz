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
    /// <see cref="ConnectorHosts"/> owns and deletes. See <see cref="ProcessSocketRoot"/>.</para></summary>
    public static async Task<(ConnectorRegistry Registry, ConnectorHosts? Hosts)> CreateAsync(
        PzProject project, string projectDir, bool noLockCheck, CancellationToken ct, string? runId = null)
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
            var findings = DriftChecker.Verify(nonBuiltinRefs, lockFile, packagesDir);
            if (findings.Count > 0)
            {
                throw new PzValidationException(findings
                    .Select(f => new PzError(PzErrorCode.LockDrift, f, null, null, "run 'pz restore'"))
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
                // logSink stays null: pz has no connector-log seam on the in-process side either, so
                // there is nothing for a connector's LogEvent to fan into that would not be a new event
                // contract invented here. HostChannelPump drops them.
                processHost = ProcessConnectorHost.LoadFromDirectory(
                    packagesDir, outOfProcessRefs, socketRoot, warn: Warn);
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
    /// builtin — a connector implementing only one interface registers only on that side.</summary>
    private static void Register(
        IReadOnlyList<ConnectorInfo> installed, Func<string, IConnector> get, ConnectorRegistry registry,
        IReadOnlyList<ConnectorRequirement> nonBuiltin)
    {
        foreach (var info in installed)
        {
            var instance = get(info.Name);
            if (instance is ISourceConnector source)
            {
                RegisterOrThrowCollision(() => registry.AddSource(info.Name, source), info.Name, nonBuiltin);
            }

            if (instance is ISinkConnector sink)
            {
                RegisterOrThrowCollision(() => registry.AddSink(info.Name, sink), info.Name, nonBuiltin);
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
}
