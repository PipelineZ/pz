using System.Globalization;
using System.Text;

namespace Pz.Mcp.Editing;

/// <summary>Renders the handful of block shapes the authoring tools produce — NOT a general YAML
/// writer. Deterministic: fixed key order (the order the caller's Dictionary/List already
/// carries — no re-sorting), 2-space indent per level, LF line endings, no trailing whitespace; string
/// scalars are quoted only when YAML would otherwise mangle them on re-read.
///
/// Accepted value shapes are exactly the ones <see cref="Pz.Core.Loading.YamlMapper"/> itself produces
/// when it loads a file: <see cref="Dictionary{TKey,TValue}"/> (mapping), <see cref="List{T}"/>
/// (sequence), and scalars <see langword="long"/>/<see langword="int"/>/<see langword="double"/>/
/// <see langword="bool"/>/<see langword="string"/>/<see langword="null"/>. Anything else throws
/// <see cref="NotSupportedException"/> — this type is deliberately narrow rather than a general-purpose
/// YAML emitter.</summary>
public static class CanonicalYaml
{
    private const int SpacesPerLevel = 2;

    /// <summary>Renders a mapping entry `key:` plus its nested value tree, with the entry's own `key:`
    /// line indented by <paramref name="indentLevels"/> * 2 spaces and nested content one level deeper
    /// per level of nesting. The result always ends with a trailing newline, and every line is LF-only
    /// with no trailing whitespace.</summary>
    public static string MappingEntry(string key, object? value, int indentLevels)
    {
        var sb = new StringBuilder();
        WriteEntry(sb, key, value, indentLevels);
        return sb.ToString();
    }

    private static void WriteEntry(StringBuilder sb, string key, object? value, int indentLevels)
    {
        var indent = new string(' ', indentLevels * SpacesPerLevel);
        var renderedKey = RenderScalarText(key);

        switch (value)
        {
            case Dictionary<string, object?> map:
                if (map.Count == 0)
                {
                    sb.Append(indent).Append(renderedKey).Append(": {}\n");
                    return;
                }

                sb.Append(indent).Append(renderedKey).Append(":\n");
                foreach (var (childKey, childValue) in map)
                {
                    WriteEntry(sb, childKey, childValue, indentLevels + 1);
                }

                return;

            case List<object?> list:
                if (list.Count == 0)
                {
                    sb.Append(indent).Append(renderedKey).Append(": []\n");
                    return;
                }

                sb.Append(indent).Append(renderedKey).Append(":\n");
                foreach (var item in list)
                {
                    WriteListItem(sb, item, indentLevels + 1);
                }

                return;

            case null:
            case bool:
            case long:
            case int:
            case double:
            case string:
                sb.Append(indent).Append(renderedKey).Append(": ").Append(RenderScalarValue(value)).Append('\n');
                return;

            default:
                throw NotSupported(value);
        }
    }

    private static void WriteListItem(StringBuilder sb, object? item, int indentLevels)
    {
        var dashIndent = new string(' ', indentLevels * SpacesPerLevel);

        if (item is Dictionary<string, object?> map && map.Count > 0)
        {
            // Render the mapping one level deeper, then fold its first line's indent into "- " so the
            // dash and the first key share a line, with the remaining keys aligned under it — standard
            // YAML block-sequence-of-mappings style.
            var inner = new StringBuilder();
            var first = true;
            foreach (var (childKey, childValue) in map)
            {
                WriteEntry(inner, childKey, childValue, indentLevels + (first ? 0 : 1));
                first = false;
            }

            var innerText = inner.ToString();
            var firstLineIndentLength = indentLevels * SpacesPerLevel;
            // The first key was rendered at indentLevels (not +1); replace its leading indent with
            // "- " so the dash and first key share a line. Subsequent lines already carry their own
            // (indentLevels+1) indent from WriteEntry, aligning under the first key.
            sb.Append(dashIndent).Append("- ").Append(innerText.AsSpan(firstLineIndentLength));
            return;
        }

        switch (item)
        {
            case null:
            case bool:
            case long:
            case int:
            case double:
            case string:
                sb.Append(dashIndent).Append("- ").Append(RenderScalarValue(item)).Append('\n');
                return;
            case Dictionary<string, object?>:
                // Empty map list item.
                sb.Append(dashIndent).Append("- {}\n");
                return;
            case List<object?>:
                throw NotSupported(item); // nested sequences-in-sequences: no authoring tool needs this shape yet.
            default:
                throw NotSupported(item);
        }
    }

    private static string RenderScalarValue(object? value) => value switch
    {
        null => "null",
        bool b => b ? "true" : "false",
        long l => l.ToString(CultureInfo.InvariantCulture),
        int i => i.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString(CultureInfo.InvariantCulture),
        string s => RenderScalarText(s),
        _ => throw NotSupported(value),
    };

    /// <summary>Quoting rule (deliberately small): a plain scalar is left bare unless leaving it bare
    /// would change its meaning on re-read — empty, leading/trailing whitespace, a leading YAML
    /// indicator character, an embedded "`: `"/trailing `:`/embedded `" #"` (would be read as
    /// structure/a comment), or text that a YAML 1.1 loader would coerce to bool/null/number instead of
    /// leaving as a string.</summary>
    private static string RenderScalarText(string s)
    {
        return NeedsQuoting(s) ? Quote(s) : s;
    }

    private static bool NeedsQuoting(string s)
    {
        if (s.Length == 0)
        {
            return true;
        }

        if (char.IsWhiteSpace(s[0]) || char.IsWhiteSpace(s[^1]))
        {
            return true;
        }

        // '\r' counts on its own: a lone CR -- no LF after it -- is not caught by the
        // '\n' test, and splicing a raw CR into connections.yml would corrupt the line it lands on.
        if (s.Contains('\n') || s.Contains('\r') || s.Contains('\t'))
        {
            return true;
        }

        if ("-?:,[]{}#&*!|>'\"%@`".IndexOf(s[0]) >= 0)
        {
            return true;
        }

        if (s.Contains(": ", StringComparison.Ordinal) || s.EndsWith(':') || s.Contains(" #", StringComparison.Ordinal))
        {
            return true;
        }

        return LooksLikeNonStringScalar(s);
    }

    private static bool LooksLikeNonStringScalar(string s)
    {
        if (s.Equals("null", StringComparison.OrdinalIgnoreCase) || s == "~")
        {
            return true;
        }

        if (s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("false", StringComparison.OrdinalIgnoreCase)
            || s.Equals("yes", StringComparison.OrdinalIgnoreCase) || s.Equals("no", StringComparison.OrdinalIgnoreCase)
            || s.Equals("on", StringComparison.OrdinalIgnoreCase) || s.Equals("off", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            return true;
        }

        if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
        {
            return true;
        }

        return false;
    }

    private static string Quote(string s)
    {
        // Order matters: backslash-escaping must run first, or the backslashes this method itself
        // introduces below (for \n, \r and \t) would get doubled by a later pass.
        var escaped = s
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("\"", "\\\"", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\t", "\\t", StringComparison.Ordinal);
        return "\"" + escaped + "\"";
    }

    private static NotSupportedException NotSupported(object? value) => new(
        $"CanonicalYaml renders only Dictionary<string,object?>/List<object?> and long/int/double/bool/" +
        $"string/null scalars — got {value?.GetType().FullName ?? "null"}.");
}
