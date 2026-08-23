namespace Pz.PackageManagement.Restore;

/// <summary>Expands a runtime identifier into the ordered list of RIDs a package's assets may be
/// selected from, most specific first — the same ordering NuGet's own RID graph produces, so a package
/// shipping only <c>runtimes/linux-x64/</c> is reachable from a <c>linux-musl-x64</c> host.
///
/// <para>Only the PORTABLE RID shape is expanded (<c>&lt;os&gt;[-&lt;variant&gt;]-&lt;arch&gt;</c>, e.g.
/// <c>linux-musl-x64</c>). That is what <see cref="System.Runtime.InteropServices.RuntimeInformation.RuntimeIdentifier"/>
/// reports on the .NET versions pz targets, and it is the only shape a host RID can take here. A
/// version-qualified legacy RID (<c>ubuntu.20.04-x64</c>, <c>win10-x64</c>) is not recognized and
/// expands to itself alone — degrading to the exact-match behavior rather than guessing an
/// ancestry.</para>
///
/// <para>The whole graph is derived from one OS-ancestry table plus one rule, because the portable RID
/// graph is completely regular: an architecture-qualified RID
/// <c>&lt;os&gt;-&lt;arch&gt;</c> imports its own architecture-less <c>&lt;os&gt;</c> first, then its
/// parent OS carrying the same architecture (<c>linux-musl-x64</c> → <c>linux-musl</c>, then
/// <c>linux-x64</c>). An OS whose parent is the architecture-less root (<c>win</c>, <c>browser</c>,
/// <c>wasi</c>) has no architecture-carrying ancestor, so it imports only itself.</para></summary>
public static class RuntimeIdentifierGraph
{
    /// <summary>The architecture-less OS ancestry: each key's single parent. <c>base</c> is the root and
    /// has none. Keys are exactly the architecture-less RIDs of the portable graph, so an OS key absent
    /// from this table is one this expansion does not recognize.</summary>
    private static readonly Dictionary<string, string?> OsParents = new(StringComparer.Ordinal)
    {
        ["base"] = null,
        ["any"] = "base",
        ["unix"] = "any",
        ["win"] = "any",
        ["browser"] = "any",
        ["wasi"] = "any",
        ["linux"] = "unix",
        ["osx"] = "unix",
        ["freebsd"] = "unix",
        ["illumos"] = "unix",
        ["solaris"] = "unix",
        ["haiku"] = "unix",
        ["ios"] = "unix",
        ["tvos"] = "unix",
        ["linux-musl"] = "linux",
        ["linux-bionic"] = "linux",
        ["android"] = "linux-bionic",
        ["iossimulator"] = "ios",
        ["maccatalyst"] = "ios",
        ["tvossimulator"] = "tvos",
    };

    /// <summary>Architectures that can qualify an OS in a portable RID. Needed to tell an
    /// architecture-qualified RID apart from a multi-segment OS key: <c>linux-musl</c> and
    /// <c>linux-x64</c> split the same way, and only the architecture table separates them.</summary>
    private static readonly HashSet<string> Architectures = new(StringComparer.Ordinal)
    {
        "arm", "arm64", "armel", "armv6", "loongarch64", "mips64", "ppc64le",
        "riscv64", "s390x", "wasm", "x64", "x86",
    };

    /// <summary><paramref name="rid"/> itself first, then every compatible ancestor in breadth-first
    /// import order — the order NuGet resolves assets in, so the first entry a package actually ships
    /// assets for is the one to select. Never empty: an unrecognized RID expands to itself alone.</summary>
    public static IReadOnlyList<string> Expand(string rid)
    {
        var expanded = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<string>();
        queue.Enqueue(rid);

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (!seen.Add(current))
            {
                continue;
            }

            expanded.Add(current);
            foreach (var import in Imports(current))
            {
                queue.Enqueue(import);
            }
        }

        return expanded;
    }

    private static IEnumerable<string> Imports(string rid)
    {
        if (OsParents.TryGetValue(rid, out var parent))
        {
            if (parent is not null)
            {
                yield return parent;
            }

            yield break;
        }

        var lastDash = rid.LastIndexOf('-');
        if (lastDash <= 0)
        {
            yield break; // not architecture-qualified and not a known OS: unrecognized, no ancestry
        }

        var os = rid[..lastDash];
        var architecture = rid[(lastDash + 1)..];
        if (!Architectures.Contains(architecture) || !OsParents.TryGetValue(os, out var osParent))
        {
            yield break;
        }

        yield return os;

        // The architecture-less root ("any") has no architecture-carrying form, so an OS sitting
        // directly under it (win, browser, wasi) contributes no second import.
        if (osParent is not null and not "base" and not "any")
        {
            yield return $"{osParent}-{architecture}";
        }
    }
}
