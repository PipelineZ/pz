using System.CommandLine;
using System.Runtime.InteropServices;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.Cli.Commands;

/// <summary>`pz restore`: resolves declared non-builtin connector packages (see
/// <see cref="BuiltinConnectors.PackageIds"/>) against the host feeds (<c>--feeds</c>, else
/// <c>PZ_FEEDS</c>, else nuget.org; see <see cref="HostFeeds"/>), materializes them under
/// <c>.pz/packages</c> via the content-addressed cache, and writes <c>pz.lock.json</c>. A project
/// whose connectors are all builtin has nothing to restore: no lock is written, nothing is deleted.</summary>
internal static class RestoreCommand
{
    public static Command Create()
    {
        var projectOption = new Option<string?>("--project") { Description = "Project directory (default: current directory)" };
        var feedsOption = new Option<string[]>("--feeds")
        {
            Description = "NuGet feed URL or local folder path, in probe order; repeatable. " +
                "Overrides PZ_FEEDS; default nuget.org.",
            Arity = ArgumentArity.ZeroOrMore,
        };
        var command = new Command("restore",
            "Resolve declared non-builtin connectors against the host feeds (--feeds, else PZ_FEEDS, " +
            "else nuget.org), materialize them under .pz/packages, and write pz.lock.json.");
        command.Options.Add(projectOption);
        command.Options.Add(feedsOption);
        command.SetAction((parseResult, ct) => Execute(
            parseResult.GetValue(projectOption) ?? Directory.GetCurrentDirectory(),
            parseResult.GetValue(feedsOption), ct));
        return command;
    }

    internal static async Task<int> Execute(string projectDir, IReadOnlyList<string>? feeds, CancellationToken ct)
    {
        var env = SharedInputHelpers.SnapshotEnvironment();

        PzProject project;
        try
        {
            project = ProjectLoader.Load(projectDir, env);
        }
        catch (PzValidationException ex)
        {
            foreach (var error in ex.Errors)
            {
                Console.Error.WriteLine($"error {error}");
            }

            return ExitCodes.ConfigError;
        }

        var builtin = new List<ConnectorRequirement>();
        var nonBuiltin = new List<ConnectorRequirement>();
        foreach (var connector in project.Connectors)
        {
            (BuiltinConnectors.PackageIds.Contains(connector.Package) ? builtin : nonBuiltin).Add(connector);
        }

        foreach (var connector in builtin.OrderBy(c => c.Package, StringComparer.Ordinal))
        {
            // A builtin's version: is accepted (schema-valid) but never
            // consulted — the builtin is whatever ships with this pz build — so say so explicitly rather
            // than implying the declared version was honored.
            Console.WriteLine($"note: {connector.Package} is builtin in this pz version; declared version ignored");
        }

        if (nonBuiltin.Count == 0)
        {
            Console.WriteLine("nothing to restore (all declared connectors are builtin)");
            return ExitCodes.Ok;
        }

        var packagesDir = Path.Combine(projectDir, ".pz", "packages");
        var workDir = Path.Combine(projectDir, ".pz", "tmp", $"restore-{Guid.NewGuid():N}");

        // `pz clean` always sweeps free .pz/tmp workdirs. Holding the
        // lock keeps an in-flight restore's scratch space out of that sweep. Deliberately not a `using`
        // declaration: it must be released BEFORE the workdir cleanup below deletes the directory.
        var workDirLock = Pz.Engine.Execution.RunDirLock.Acquire(workDir);
        try
        {
            var requirements = nonBuiltin
                .Select(c => new ConnectorPackageRef(c.Package, c.Version))
                .ToArray();

            ResolveResult resolved;
            try
            {
                resolved = await NuGetResolver.ResolveAsync(
                    requirements, HostFeeds.Resolve(feeds, env), RuntimeInformation.RuntimeIdentifier, workDir, ct);
            }
            catch (RestoreException ex)
            {
                Console.Error.WriteLine($"error {new PzError(ex.Code, ex.Message, null, null, ex.Hint)}");
                return ExitCodes.ConfigError;
            }

            var hits = PackageMaterializer.Materialize(resolved, CacheRoot(), packagesDir);

            LockFileWriter.Write(resolved.Lock, Path.Combine(projectDir, "pz.lock.json"));

            foreach (var package in resolved.Lock.Packages.OrderBy(p => p.Id, StringComparer.Ordinal))
            {
                var mode = hits.TryGetValue(package.Id, out var wasHit) && wasHit ? "cache hit" : "downloaded";
                Console.WriteLine($"restored {package.Id} {package.Version} ({mode})");
            }

            Console.WriteLine($"wrote pz.lock.json ({resolved.Lock.Packages.Count} packages)");
            return ExitCodes.Ok;
        }
        finally
        {
            // Released before the delete: on Windows the open .lock handle would block removing the dir.
            workDirLock.Dispose();
            try { Directory.Delete(workDir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>PZ_CACHE_DIR is respected here, at the CLI boundary — PackageMaterializer itself always
    /// takes cacheRoot as a plain parameter (Restore/PackageMaterializer.cs doc comment).</summary>
    private static string CacheRoot() =>
        Environment.GetEnvironmentVariable("PZ_CACHE_DIR") is { Length: > 0 } custom
            ? custom
            : Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".pz", "cache");

}
