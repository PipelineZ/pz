using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.Cli;

/// <summary>Builds the connector registry: builtins + (when the project declares non-builtin
/// connectors) a ConnectorHost over .pz/packages verified against pz.lock.json.</summary>
internal static class ConnectorRegistryFactory
{
    /// <summary>Throws PzValidationException (PZ0321/PZ0322/host PZ03xx mapped) on lock/host problems.
    /// Host is null when all declared connectors are builtin. Caller disposes Host.</summary>
    public static Task<(ConnectorRegistry Registry, ConnectorHost? Host)> CreateAsync(
        PzProject project, string projectDir, bool noLockCheck, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var registry = BuiltinConnectors.CreateRegistry();
        var nonBuiltin = project.Connectors
            .Where(c => !BuiltinConnectors.PackageIds.Contains(c.Package))
            .ToArray();

        if (nonBuiltin.Length == 0)
        {
            return Task.FromResult<(ConnectorRegistry, ConnectorHost?)>((registry, null));
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

        ConnectorHost host;
        try
        {
            host = ConnectorHost.LoadFromDirectory(
                packagesDir, installRefs, warn: msg => Console.Error.WriteLine($"warning: {msg}"));
        }
        catch (ConnectorHostException ex)
        {
            throw new PzValidationException([new PzError(ex.Code, ex.Message, null, null, ex.Hint)]);
        }

        foreach (var info in host.Installed)
        {
            var instance = host.Get(info.Name);
            if (instance is ISourceConnector source)
            {
                RegisterOrThrowCollision(() => registry.AddSource(info.Name, source), info.Name, nonBuiltin);
            }

            if (instance is ISinkConnector sink)
            {
                RegisterOrThrowCollision(() => registry.AddSink(info.Name, sink), info.Name, nonBuiltin);
            }
        }

        return Task.FromResult<(ConnectorRegistry, ConnectorHost?)>((registry, host));
    }

    /// <summary><see cref="ConnectorRegistry.AddSource"/>/<c>AddSink</c> throw
    /// <see cref="InvalidOperationException"/> when a hosted connector's name collides with an
    /// already-registered one (always a builtin here, since every hosted connector name is unique within
    /// its own <c>ConnectorHost</c> — that host enforces its own PZ0305 cross-package collision check
    /// before <see cref="CreateAsync"/> ever sees it). Translate that into the same PZ0305 error family,
    /// naming both the colliding builtin name and the hosted package(s) that could have produced it.</summary>
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
