using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;
using Pz.PackageManagement.Restore;

namespace Pz.Cli;

/// <summary>Builds the connector registry: builtins + (when the project declares non-builtin
/// connectors) the hosts over .pz/packages verified against pz.lock.json — a <see cref="ConnectorHost"/>
/// for the packages whose manifest declares no runtime or <c>"dotnet"</c>, and a
/// <see cref="ProcessConnectorHost"/> for those declaring <c>"process"</c>.</summary>
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

        // Partition by declared runtime BEFORE constructing either host: neither can load the other's
        // packages (a process package ships no entry assembly to ALC-load, and ProcessConnectorHost
        // refuses a non-process manifest with PZ0354), and a wrongly-routed package fails with an error
        // about the wrong thing.
        var inProcessRefs = new List<ConnectorPackageRef>();
        var outOfProcessRefs = new List<ConnectorPackageRef>();
        foreach (var packageRef in installRefs)
        {
            (IsProcessRuntime(packagesDir, packageRef) ? outOfProcessRefs : inProcessRefs).Add(packageRef);
        }

        ConnectorHost? host = null;
        ProcessConnectorHost? processHost = null;
        string? ownedSocketRoot = null;
        void Warn(string message) => Console.Error.WriteLine($"warning: {message}");
        try
        {
            if (inProcessRefs.Count > 0)
            {
                host = ConnectorHost.LoadFromDirectory(packagesDir, inProcessRefs, warn: Warn);
            }

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
            await new ConnectorHosts(host, processHost, ownedSocketRoot).DisposeAsync().ConfigureAwait(false);
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

        var hosts = new ConnectorHosts(host, processHost, ownedSocketRoot);
        try
        {
            RefuseCrossHostCollisions(host, processHost);
            if (host is not null)
            {
                Register(host.Installed, host.Get, registry, nonBuiltin);
            }

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

    /// <summary>A package's manifest <c>runtime</c> is the single source of truth for which host owns it,
    /// and reading it is one small JSON read with no assembly load. A manifest that is missing or broken
    /// answers "not process": <see cref="ConnectorHost"/> owns reporting both cases (a missing manifest
    /// is its warn-and-attempt path, a malformed one its PZ0306), and raising them here would replace
    /// its message with one that explains less.</summary>
    private static bool IsProcessRuntime(string packagesDir, ConnectorPackageRef packageRef)
    {
        try
        {
            var packageDir = Path.Combine(packagesDir, packageRef.PackageId, packageRef.Version);
            return ManifestReader.TryRead(packageDir)?.Runtime == "process";
        }
        catch (ConnectorHostException)
        {
            return false;
        }
    }

    /// <summary>Each host rejects duplicate connector names only within its OWN package set, so a name
    /// registered by an in-process package AND an out-of-process one reaches the registry as two
    /// packages both claiming it. Same PZ0305 family and same invariant as every other collision here:
    /// a name is never silently resolved in one host's favour. Aggregated — every colliding name is
    /// reported, not just the first.</summary>
    private static void RefuseCrossHostCollisions(ConnectorHost? host, ProcessConnectorHost? processHost)
    {
        if (host is null || processHost is null)
        {
            return;
        }

        var inProcessNames = host.Installed.Select(i => i.Name).ToHashSet(StringComparer.Ordinal);
        var collisions = processHost.Installed
            .Select(i => i.Name)
            .Where(inProcessNames.Contains)
            .OrderBy(name => name, StringComparer.Ordinal)
            .Select(name => new PzError(
                PzErrorCode.ConnectorNotInstalled,
                $"connector name '{name}' is registered by both an in-process and an out-of-process " +
                "connector package",
                null, null,
                "remove one of the conflicting packages"))
            .ToArray();

        if (collisions.Length > 0)
        {
            throw new PzValidationException(collisions);
        }
    }

    /// <summary><see cref="ConnectorRegistry.AddSource"/>/<c>AddSink</c> throw
    /// <see cref="InvalidOperationException"/> when a hosted connector's name collides with an
    /// already-registered one (always a builtin here: each host enforces its own PZ0305 cross-package
    /// check within its package set, and <see cref="RefuseCrossHostCollisions"/> covers the pair, both
    /// before any registration happens). Translate that into the same PZ0305 error family, naming both
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
    /// common case, and the only one this can name with certainty — a <c>ConnectorHost</c> does not
    /// expose which of several hosted packages registered a given connector name); otherwise lists every
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
