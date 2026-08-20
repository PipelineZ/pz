using NuGet.Versioning;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Restore;

/// <summary>Pure, offline check that a project's currently-declared non-builtin connector requirements
/// still match what <c>pz restore</c> last committed to <c>pz.lock.json</c>. No feed access, no NuGet
/// resolution — just comparing <paramref name="nonBuiltinRequirements"/> against
/// <see cref="LockFile.Packages"/> and the materialized <paramref name="packagesDir"/> layout.</summary>
public static class DriftChecker
{
    /// <summary>Empty list = no drift. Each entry is a human-readable description; the CLI maps them
    /// to PZ0321. Checks: requirement missing from lock; locked version outside the declared range;
    /// stale lock entry no requirement explains; locked package dir missing under packagesDir.</summary>
    public static IReadOnlyList<string> Verify(
        IReadOnlyList<ConnectorPackageRef> nonBuiltinRequirements, LockFile lockFile, string packagesDir)
    {
        var findings = new List<string>();

        var requestedById = lockFile.Packages
            .Where(p => p.Requested)
            .ToDictionary(p => p.Id, StringComparer.OrdinalIgnoreCase);
        var requirementIds = new HashSet<string>(
            nonBuiltinRequirements.Select(r => r.PackageId), StringComparer.OrdinalIgnoreCase);

        foreach (var requirement in nonBuiltinRequirements)
        {
            if (!requestedById.TryGetValue(requirement.PackageId, out var locked))
            {
                findings.Add(
                    $"requirement '{requirement.PackageId}' {requirement.Version} has no entry in pz.lock.json");
                continue;
            }

            if (!ParseRange(requirement.Version).Satisfies(NuGetVersion.Parse(locked.Version)))
            {
                findings.Add(
                    $"requirement '{requirement.PackageId}' {requirement.Version} does not admit the " +
                    $"locked version {locked.Version}");
            }

            var versionDir = Path.Combine(packagesDir, locked.Id, locked.Version);
            if (!Directory.Exists(versionDir))
            {
                findings.Add($"locked package '{locked.Id}' {locked.Version} is missing under {packagesDir}");
            }
        }

        foreach (var locked in requestedById.Values)
        {
            if (!requirementIds.Contains(locked.Id))
            {
                findings.Add(
                    $"pz.lock.json pins '{locked.Id}' {locked.Version} but no requirement declares it anymore");
            }
        }

        return findings;
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
