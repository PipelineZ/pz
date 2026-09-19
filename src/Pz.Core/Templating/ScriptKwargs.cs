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
    /// one character from a pz-owned key is worth saying out loud. Thin forwarder: the match itself is
    /// <see cref="Pz.Core.Validation.NearMiss"/>, shared with tier 3's unknown-output-option message.</summary>
    public static string? NearMiss(IEnumerable<string> known, string option) =>
        Pz.Core.Validation.NearMiss.Find(known, option);
}
