using System.Text.RegularExpressions;

namespace Pz.Core.Model;

/// <summary>Validates names that end up interpolated, unquoted, into DuckDB identifiers -- a pipeline's
/// file stem (<c>staging.&lt;name&gt;</c>) and a connection's name (the <c>&lt;connection&gt;</c> half of
/// <c>src_&lt;connection&gt;__&lt;entity&gt;</c>, itself never folded -- see
/// <see cref="Pz.Core.Dag.StagingName"/>). Both must be a legal unquoted SQL identifier on their own,
/// independent of DuckDB's reserved-word list: every keyword probed (order, select, table, group, ...)
/// parses fine once schema-qualified as <c>staging.&lt;word&gt;</c>, so nothing here refuses on
/// reservedness -- only shape. The MCP authoring surface (<c>pz_write_pipeline</c>) enforces the same
/// predicate before ever writing a file, so an agent-authored name cannot reach disk only to fail this
/// check at the next load.</summary>
public static partial class PzIdentifier
{
    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Pattern();

    public static bool IsValid(string? name) => name is { Length: > 0 } && Pattern().IsMatch(name);

    /// <summary>The reason <paramref name="name"/> is not a legal unquoted identifier, or null when it
    /// is. The returned clause completes "... '&lt;name&gt;' ..." in a PZ0136 message.</summary>
    public static string? Problem(string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return "is empty";
        }

        return IsValid(name)
            ? null
            : "is not a valid identifier -- only ASCII letters, digits, and underscores are allowed, and it cannot start with a digit";
    }

    /// <summary>A concrete rename that IS a valid identifier: every run of characters outside
    /// <c>[A-Za-z0-9_]</c> becomes one underscore, then -- because a leading digit is the one thing
    /// folding alone cannot fix -- a leading run of digits/underscores is moved to the end
    /// (<c>01_load</c> -&gt; <c>load_01</c>, matching how authors already read numeric file-name
    /// prefixes as ordering, not identity).</summary>
    public static string Suggest(string name)
    {
        var folded = NonIdentifierRun().Replace(name, "_");
        if (folded.Length == 0 || !char.IsAsciiDigit(folded[0]))
        {
            var trimmed = folded.Trim('_');
            return trimmed.Length > 0 ? trimmed : "_";
        }

        var i = 0;
        while (i < folded.Length && (char.IsAsciiDigit(folded[i]) || folded[i] == '_')) { i++; }
        var prefix = folded[..i].Trim('_');
        var rest = folded[i..].Trim('_');
        // A name of nothing but digits/underscores has no non-numeric remainder to lead with --
        // an underscore prefix is the only way left to make it start legally.
        return rest.Length > 0 ? $"{rest}_{prefix}" : $"_{prefix}";
    }

    [GeneratedRegex("[^A-Za-z0-9_]+")]
    private static partial Regex NonIdentifierRun();
}
