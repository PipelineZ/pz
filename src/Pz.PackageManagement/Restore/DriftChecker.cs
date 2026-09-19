using NuGet.Versioning;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Restore;

/// <summary>One way the project, its lock and its installed packages disagree. <paramref name="Code"/>
/// is the PZ code the CLI reports it under, <paramref name="Hint"/> the next step: a lock the project
/// has outgrown is re-resolved (<c>pz restore --update</c>), an installed file that no longer matches
/// the lock is reinstalled (<c>pz restore</c>).</summary>
public sealed record DriftFinding(string Code, string Message, string Hint);

/// <summary>Pure, offline check that a project's currently-declared non-builtin connector requirements
/// still match what <c>pz restore</c> last committed to <c>pz.lock.json</c>, and that what is installed
/// under <c>.pz/packages</c> is still what that lock recorded. No feed access, no NuGet resolution —
/// just comparing requirements against <see cref="LockFile.Packages"/>, and the materialized files
/// against the hashes the lock carries.</summary>
public static class DriftChecker
{
    private const string LockDrift = "PZ0321";
    private const string ContentMismatch = "PZ0326";
    private const string ReResolve = "run 'pz restore --update' to re-resolve the lock";
    private const string Reinstall = "run 'pz restore' to reinstall it from the locked package";

    /// <summary>Empty list = no drift. Everything <see cref="VerifyRequirements"/> reports, plus what
    /// only an installed tree can show: a lock restored for another platform than
    /// <paramref name="hostRid"/> (its native assets are the wrong platform's, and its entrypoint would
    /// fail to spawn with a misleading error); a locked package dir missing under
    /// <paramref name="packagesDir"/>; and an installed asset whose content differs from the hash the
    /// lock recorded for it — the check that keeps a modified entrypoint binary from being spawned. An
    /// asset the lock carries no hash for (a lock written before hashes were kept) is not checked.</summary>
    public static IReadOnlyList<DriftFinding> Verify(
        IReadOnlyList<ConnectorPackageRef> nonBuiltinRequirements, LockFile lockFile, string packagesDir, string hostRid)
    {
        var findings = new List<DriftFinding>(VerifyRequirements(nonBuiltinRequirements, lockFile));

        if (!string.Equals(lockFile.Rid, hostRid, StringComparison.OrdinalIgnoreCase))
        {
            findings.Add(new DriftFinding(LockDrift,
                $"pz.lock.json was restored for runtime identifier '{lockFile.Rid}', but this host is '{hostRid}'; " +
                "the installed native assets and entrypoints are the other platform's",
                "run 'pz restore' on this host to install this platform's assets"));
        }

        var requirementIds = new HashSet<string>(
            nonBuiltinRequirements.Select(r => r.PackageId), StringComparer.OrdinalIgnoreCase);
        foreach (var locked in lockFile.Packages.Where(p => p.Requested && requirementIds.Contains(p.Id)))
        {
            var versionDir = Path.Combine(packagesDir, locked.Id, locked.Version);
            if (!Directory.Exists(versionDir))
            {
                findings.Add(new DriftFinding(LockDrift,
                    $"locked package '{locked.Id}' {locked.Version} is missing under {packagesDir}",
                    "run 'pz restore'"));
                continue;
            }

            findings.AddRange(VerifyContent(locked, versionDir));
        }

        return findings;
    }

    /// <summary>The requirement-versus-lock half of <see cref="Verify"/>, for a restore that has no
    /// installed tree to check yet: requirement missing from the lock; locked version outside the
    /// declared range; stale lock entry no requirement explains. Each of these means the lock cannot
    /// pin this project as declared, so the hint is to re-resolve.</summary>
    public static IReadOnlyList<DriftFinding> VerifyRequirements(
        IReadOnlyList<ConnectorPackageRef> nonBuiltinRequirements, LockFile lockFile)
    {
        var findings = new List<DriftFinding>();

        var requestedById = lockFile.Packages
            .Where(p => p.Requested)
            .ToDictionary(p => p.Id, StringComparer.OrdinalIgnoreCase);
        var requirementIds = new HashSet<string>(
            nonBuiltinRequirements.Select(r => r.PackageId), StringComparer.OrdinalIgnoreCase);

        foreach (var requirement in nonBuiltinRequirements)
        {
            if (!requestedById.TryGetValue(requirement.PackageId, out var locked))
            {
                findings.Add(new DriftFinding(LockDrift,
                    $"requirement '{requirement.PackageId}' {requirement.Version} has no entry in pz.lock.json",
                    ReResolve));
                continue;
            }

            if (!ParseRange(requirement.Version).Satisfies(NuGetVersion.Parse(locked.Version)))
            {
                findings.Add(new DriftFinding(LockDrift,
                    $"requirement '{requirement.PackageId}' {requirement.Version} does not admit the " +
                    $"locked version {locked.Version}",
                    ReResolve));
            }
        }

        foreach (var locked in requestedById.Values)
        {
            if (!requirementIds.Contains(locked.Id))
            {
                findings.Add(new DriftFinding(LockDrift,
                    $"pz.lock.json pins '{locked.Id}' {locked.Version} but no requirement declares it anymore",
                    ReResolve));
            }
        }

        return findings;
    }

    /// <summary>Whether every hashed asset of <paramref name="package"/> under <paramref name="versionDir"/>
    /// still has the content the lock recorded. Shared with the materializer, which reinstalls a
    /// directory this reports on rather than trusting that it exists.</summary>
    public static bool ContentMatches(LockedPackage package, string versionDir) =>
        VerifyContent(package, versionDir).Count == 0;

    private static List<DriftFinding> VerifyContent(LockedPackage package, string versionDir)
    {
        var findings = new List<DriftFinding>();
        foreach (var (role, assets) in new[] { ("lib", package.Assets.Lib), ("native", package.Assets.Native) })
        {
            foreach (var asset in assets)
            {
                var path = Path.Combine(versionDir, role, asset.File);

                // Existence is checked even when the lock predates per-file hashes: a torn directory
                // from a crashed materialize must never be trusted just because it exists. Only the
                // content comparison below needs a recorded hash to run at all.
                if (!File.Exists(path))
                {
                    findings.Add(new DriftFinding(ContentMismatch,
                        $"file {role}/{asset.File} of package '{package.Id}' {package.Version} is missing under " +
                        $"{versionDir}",
                        Reinstall));
                    continue;
                }

                if (asset.Sha512 is null)
                {
                    continue;
                }

                var actual = HashFile(path);
                if (!string.Equals(actual, asset.Sha512, StringComparison.OrdinalIgnoreCase))
                {
                    findings.Add(new DriftFinding(ContentMismatch,
                        $"file {role}/{asset.File} of package '{package.Id}' {package.Version} does not have the " +
                        "content pz.lock.json recorded for it (modified, truncated or replaced after restore)",
                        Reinstall));
                }
            }
        }

        return findings;
    }

    private static string? HashFile(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        using var stream = File.OpenRead(path);
        return Convert.ToHexStringLower(System.Security.Cryptography.SHA512.HashData(stream));
    }

    /// <summary>Mirrors NuGetResolver.ParseRequirementRange's exact-pin convention (a bare version like
    /// "1.2.3" means the range [1.2.3]) but does not reject floating ranges — a floating requirement can
    /// never have reached a committed pz.lock.json (restore itself rejects it with PZ0323 before a lock
    /// is ever written), so there is nothing for drift-checking to guard against here.</summary>
    private static VersionRange ParseRange(string versionText) =>
        versionText.Contains('[') || versionText.Contains('(') || versionText.Contains('*')
            ? VersionRange.Parse(versionText)
            : VersionRange.Parse($"[{versionText}]");
}
