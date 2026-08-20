using Scriban;
using Scriban.Runtime;
using Scriban.Syntax;

namespace Pz.Core.Templating;

/// <summary>The keyword arguments of one Scriban call, read off the caller's syntax node rather than
/// from the bound argument list.
///
/// This is the whole reason <c>source()</c>/<c>sink()</c> are <see cref="IScriptCustomFunction"/>s:
/// Scriban 7.2.5 binds an unrecognized named argument into the next free POSITIONAL slot instead of
/// raising, so <c>sink('m', 'x', keyz: ['id'])</c> would silently set the write strategy to a list of
/// column names. Reading <see cref="ScriptFunctionCall.Arguments"/> directly gives pz the real names,
/// values, and source spans, so every rejection is its own.
///
/// Shared by both call surfaces so the mechanism has exactly one implementation — the two functions
/// differ in their rule tables, not in how they see their arguments.</summary>
internal static class ScriptKwargs
{
    /// <summary>Every named argument of the call, keyed by the name the author typed, with the line it
    /// appeared on. <paramref name="onDuplicate"/> is invoked for a name passed more than once; the
    /// first occurrence wins so parsing can continue and report the rest of the call.</summary>
    public static Dictionary<string, (object? Value, int Line)> Read(
        TemplateContext context, ScriptNode? callerContext, Action<string, int> onDuplicate)
    {
        var kwargs = new Dictionary<string, (object? Value, int Line)>(StringComparer.Ordinal);
        if (callerContext is not ScriptFunctionCall call)
        {
            return kwargs;
        }

        foreach (var arg in call.Arguments)
        {
            if (arg is not ScriptNamedArgument named || named.Name?.Name is not { } name || named.Value is null)
            {
                continue;
            }

            var line = named.Span.Start.Line + 1;
            if (!kwargs.TryAdd(name, (Convert(context.Evaluate(named.Value)), line)))
            {
                onDuplicate(name, line);
            }
        }

        return kwargs;
    }

    /// <summary>Scriban's own containers never escape a call surface: every value is converted to the
    /// plain CLR shapes the YAML loader produces, so <c>CanonicalJson.Serialize</c> (which feeds the
    /// NodeId), the loader's own sub-parsers, and every connector see identical values whichever surface
    /// declared them.</summary>
    public static object? Convert(object? value) => value switch
    {
        ScriptArray array => array.Select(Convert).ToList(),
        ScriptObject obj => obj.ToDictionary(kv => kv.Key, kv => Convert(kv.Value), StringComparer.Ordinal),
        _ => value,
    };

    /// <summary>The name in <paramref name="known"/> that <paramref name="option"/> is one edit (or a
    /// case difference) away from, or null when it is plainly a connector option. pz cannot REFUSE an
    /// unrecognized kwarg — no connector publishes an option vocabulary to check against — but a name
    /// one character from a pz-owned key is worth saying out loud.</summary>
    public static string? NearMiss(IEnumerable<string> known, string option) =>
        known.FirstOrDefault(k =>
            !string.Equals(option, k, StringComparison.Ordinal)
            && (string.Equals(option, k, StringComparison.OrdinalIgnoreCase) || IsWithinOneEdit(option, k)));

    /// <summary>True when one insertion, deletion, or substitution turns <paramref name="a"/> into
    /// <paramref name="b"/>. Short-circuits on a length gap of 2+, so it never scans a long
    /// connector-option name against a short keyword.</summary>
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
