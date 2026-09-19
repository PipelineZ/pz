using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit;

/// <summary>Guards a relative <c>path:</c>/entity-derived location against escaping a connection's
/// declared root, so a typo'd (or malicious) <c>..</c> segment cannot read or write outside the place
/// the connection names. An ABSOLUTE <c>path:</c> is unaffected — it ignores the connection's location
/// entirely, by design (see e.g. <c>LocalFilesSink.ResolveOutputDir</c>'s doc comment) — callers only
/// ever reach this class with a value already known to be relative. Symlinks are out of scope: this is
/// a purely lexical containment check, the same guarantee a project author already gets from
/// <c>root:</c> itself.
///
/// Two shapes, because "root" means two different things:
///  - <see cref="ResolveWithinRoot"/> — the FILESYSTEM form (localfiles): <paramref name="root"/> is a
///    real directory, so containment is checked with <see cref="Path.GetFullPath(string)"/>.
///  - <see cref="RefuseParentSegment"/> — the OPAQUE-KEY form (s3, gcs): a bucket key is a
///    slash-delimited string with no filesystem resolution behind it — nothing actually "collapses" a
///    <c>..</c> segment the way a filesystem does — so there is no full-path containment to check. A
///    <c>..</c> segment can still never be a legitimate authored key component (pz's own entity-name
///    grammar already forbids one everywhere else — PZ0344), so this refuses the same shape of mistake
///    in the one place a free-form <c>path:</c> override still admits it.</summary>
public static class RootContainment
{
    /// <param name="root">The connection's resolved root directory.</param>
    /// <param name="relative">An already-relative path (the caller checks <see cref="Path.IsPathRooted"/>
    /// first and skips this call entirely for an absolute one).</param>
    /// <param name="connectionName">Named in the refusal; never the attempted value.</param>
    /// <param name="subject">"dataset '&lt;name&gt;'"/"output '&lt;name&gt;'", named in the refusal.</param>
    /// <returns><paramref name="root"/> joined with <paramref name="relative"/>.</returns>
    public static string ResolveWithinRoot(string root, string relative, string connectionName, string subject)
    {
        var combined = Path.Combine(root, relative);

        // Comparing on Path.GetFullPath'd strings (not the raw combined path) is what makes this a real
        // containment check rather than a syntactic one — relative uses ".." itself, and a value like
        // "sub/../../escaped" only reveals where it actually lands once normalized. A trailing separator
        // on the root side is required before the prefix comparison below: without it, a sibling
        // directory that merely SHARES the root's name as a text prefix ("/data/root2" against root
        // "/data/root") would wrongly compare as "inside".
        var fullRoot = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var fullCombined = Path.GetFullPath(combined);
        if (fullCombined != fullRoot &&
            !fullCombined.StartsWith(fullRoot + Path.DirectorySeparatorChar, StringComparison.Ordinal))
        {
            throw Refused(connectionName, subject);
        }

        return combined;
    }

    /// <param name="relativeKey">The `path:` option's value alone (not yet joined to any prefix) —
    /// refusing it before it is joined names the actual offending option, and is equivalent to refusing
    /// after joining since a bucket key never resolves ".." away.</param>
    public static void RefuseParentSegment(string relativeKey, string connectionName, string subject)
    {
        if (Array.Exists(relativeKey.Split('/'), segment => segment == ".."))
        {
            throw Refused(connectionName, subject);
        }
    }

    private static PzConnectorException Refused(string connectionName, string subject) =>
        new($"PZ0365: connection '{connectionName}': {subject} resolves outside 'root:' " +
            "-- use an absolute path, or fix 'root:'", isTransient: false);
}
