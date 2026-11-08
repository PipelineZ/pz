namespace Pz.Connector.Http;

/// <summary>Matches a `file.members` glob against a zip entry's full path. `*` and `?` match within one
/// `/`-separated segment, a `**` segment matches zero or more whole segments, and everything else
/// matches itself, ordinally and case-sensitively. A matcher rather than a regex, so no character in
/// the glob has a meaning beyond those three.</summary>
internal static class MemberGlob
{
    public static bool IsMatch(string glob, string entryPath)
        => MatchSegments(glob.Split('/'), 0, entryPath.Split('/'), 0);

    private static bool MatchSegments(string[] glob, int gi, string[] path, int pi)
    {
        while (gi < glob.Length)
        {
            if (glob[gi] == "**")
            {
                while (gi + 1 < glob.Length && glob[gi + 1] == "**")
                {
                    gi++;
                }

                if (gi == glob.Length - 1)
                {
                    return true;
                }

                for (var skip = pi; skip <= path.Length; skip++)
                {
                    if (MatchSegments(glob, gi + 1, path, skip))
                    {
                        return true;
                    }
                }

                return false;
            }

            if (pi >= path.Length || !MatchSegment(glob[gi], path[pi]))
            {
                return false;
            }

            gi++;
            pi++;
        }

        return pi == path.Length;
    }

    /// <summary>Wildcard match of one segment: on a mismatch after a `*`, retry with that `*`
    /// absorbing one more character (linear in practice, no recursion).</summary>
    private static bool MatchSegment(string pattern, string text)
    {
        int p = 0, t = 0, star = -1, starText = 0;
        while (t < text.Length)
        {
            if (p < pattern.Length && (pattern[p] == '?' || pattern[p] == text[t]) && pattern[p] != '*')
            {
                p++;
                t++;
            }
            else if (p < pattern.Length && pattern[p] == '*')
            {
                star = p++;
                starText = t;
            }
            else if (star >= 0)
            {
                p = star + 1;
                t = ++starText;
            }
            else
            {
                return false;
            }
        }

        while (p < pattern.Length && pattern[p] == '*')
        {
            p++;
        }

        return p == pattern.Length;
    }
}
