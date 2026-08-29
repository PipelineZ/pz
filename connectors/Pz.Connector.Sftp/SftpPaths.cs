using System.Text;
using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;

namespace Pz.Connector.Sftp;

/// <summary>Pure path resolution, glob listing, and watermark-window cover narrowing for the SFTP
/// connector. <see cref="ListMatches"/> is the only member that touches the wire, through
/// <see cref="ISftpFileSystem"/>; everything else is a string computation.</summary>
internal static class SftpPaths
{
    /// <summary>Read location: root + `path:` (default `&lt;entity&gt;.&lt;format&gt;`), '/'-joined, no
    /// leading-slash mangling (an absolute root stays absolute). Pure.</summary>
    public static string ResolveReadPattern(string? root, DatasetSpec spec, string format)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : $"{spec.Dataset}.{format}";
        return Join(root, relative);
    }

    /// <summary>Write directory: root + `path:` (default `&lt;entity&gt;/`). Pure.</summary>
    public static string ResolveOutputDir(string? root, OutputSpec spec)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : spec.Output;
        return Join(root, relative);
    }

    /// <summary>All remote files matching <paramref name="pattern"/>: applies the
    /// <see cref="PathTemplate.WindowCover"/> narrowing when the pattern has date tokens and both
    /// watermark bounds are stamped, then for each cover element lists the static-prefix directory
    /// (recursive iff the wildcard remainder contains '/' or '**') and glob-filters the listed names.
    /// Union across cover elements, distinct, ordinally sorted. Matches only — a no-match ERROR is the
    /// caller's job, which knows the dataset name for the message.</summary>
    public static IReadOnlyList<string> ListMatches(ISftpFileSystem fs, string pattern, DatasetSpec spec)
    {
        var matches = new List<string>();
        foreach (var coverPattern in CoverPatterns(pattern, spec))
        {
            var prefix = PathTemplate.StaticPrefix(coverPattern);
            var lastSlash = prefix.LastIndexOf('/');
            var directory = lastSlash < 0 ? "" : prefix[..lastSlash];
            var remainder = coverPattern[prefix.Length..];
            var recursive = remainder.Contains('/') || remainder.Contains("**");

            var regex = new Regex(GlobToRegexPattern(coverPattern), RegexOptions.None);
            foreach (var file in fs.ListFiles(directory, recursive))
            {
                if (regex.IsMatch(file))
                {
                    matches.Add(file);
                }
            }
        }

        return matches.Distinct().Order(StringComparer.Ordinal).ToArray();
    }

    /// <summary>Joins a root and a relative segment with a single '/'. A null/empty root leaves the
    /// relative segment untouched (so a caller-supplied absolute path is never mangled); a root already
    /// ending in '/' is not doubled. Internal: also used by <c>SftpSink</c> to join the connection root
    /// with a partition_by-rendered folder.</summary>
    internal static string Join(string? root, string relative) =>
        string.IsNullOrEmpty(root) ? relative : root.EndsWith('/') ? root + relative : $"{root}/{relative}";

    /// <summary>Window-cover guard, same shape as S3Source.CoverKeys: narrows to
    /// <see cref="PathTemplate.WindowCover"/>'s members when the pattern is date-templated and both
    /// watermark bounds are stamped, else the single literal pattern (which, if it still carries an
    /// unsubstituted token, glob-matches nothing — that is what "no cover" is supposed to mean here,
    /// not "widen to the whole directory").</summary>
    private static IReadOnlyList<string> CoverPatterns(string pattern, DatasetSpec spec)
    {
        if (!PathTemplate.HasDateTokens(pattern) || spec.WatermarkValue is null || spec.WatermarkUpperBound is null)
        {
            return [pattern];
        }

        var lo = PathTemplate.ParseCanonical(spec.WatermarkValue);
        var hi = PathTemplate.ParseCanonical(spec.WatermarkUpperBound);
        return PathTemplate.WindowCover(pattern, lo, hi);
    }

    // Lockstep copy of AzureSource.GlobToRegexPattern (connectors/Pz.Connector.AzureBlob/AzureSource.cs) —
    // keep the two in sync if glob semantics ever change. '**' crosses a '/' boundary; '*'/'?' do not;
    // everything else is escaped so a literal name (e.g. containing '+') never leaks as a regex metachar.
    internal static string GlobToRegexPattern(string pattern)
    {
        var sb = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            var c = pattern[i];
            if (c == '*')
            {
                if (i + 1 < pattern.Length && pattern[i + 1] == '*')
                {
                    sb.Append(".*");
                    i++;
                }
                else
                {
                    sb.Append("[^/]*");
                }
            }
            else if (c == '?')
            {
                sb.Append("[^/]");
            }
            else
            {
                sb.Append(Regex.Escape(c.ToString()));
            }
        }

        sb.Append('$');
        return sb.ToString();
    }
}
