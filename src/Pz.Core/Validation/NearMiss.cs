namespace Pz.Core.Validation;

/// <summary>Single-edit-distance "did you mean" matching, shared across every layer that refuses an
/// unrecognized key and wants to name the likely typo rather than just listing every accepted one --
/// <c>Pz.Core.Templating.ScriptKwargs.NearMiss</c> (kept as a thin forwarder for its existing call
/// sites) and <c>Pz.Engine.Validation.ConnectorConfigValidator</c>'s unknown-output-option message both
/// go through this one implementation.</summary>
public static class NearMiss
{
    /// <summary>The name in <paramref name="known"/> that <paramref name="candidate"/> is one edit (or
    /// a case difference) away from, or null when it is plainly not a near miss of anything known.</summary>
    public static string? Find(IEnumerable<string> known, string candidate) =>
        known.FirstOrDefault(k =>
            !string.Equals(candidate, k, StringComparison.Ordinal)
            && (string.Equals(candidate, k, StringComparison.OrdinalIgnoreCase) || IsWithinOneEdit(candidate, k)));

    /// <summary>True when one insertion, deletion, or substitution turns <paramref name="a"/> into
    /// <paramref name="b"/>. Short-circuits on a length gap of 2+, so it never scans a long option name
    /// against a short keyword.</summary>
    private static bool IsWithinOneEdit(string a, string b)
    {
        if (Math.Abs(a.Length - b.Length) > 1)
        {
            return false;
        }

        int i = 0, j = 0, edits = 0;
        while (i < a.Length && j < b.Length)
        {
            if (a[i] == b[j])
            {
                i++;
                j++;
                continue;
            }

            if (++edits > 1)
            {
                return false;
            }

            if (a.Length > b.Length) { i++; }
            else if (a.Length < b.Length) { j++; }
            else { i++; j++; }
        }

        return edits + (a.Length - i) + (b.Length - j) <= 1;
    }
}
