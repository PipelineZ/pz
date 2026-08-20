using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Bindings;

/// <summary>One value the engine supplies to a connector recipe. <see cref="TypeName"/> is the
/// contract-vocabulary type when known (drives per-context formatting, e.g. timestamp → ISO-8601).</summary>
public sealed record BindingValue(string? Value, string? TypeName)
{
    public bool IsNull => Value is null;
}

/// <summary>Expands the engine-binding vocabulary (`{{ watermark }}`, closed whitelist) inside
/// connector option strings. The CALLER formats/escapes per context via the format delegate; a
/// referenced null binding yields a null result so the caller can omit the whole parameter (first
/// run / --full-refresh). Rendered values must never be logged.</summary>
public static partial class BindingExpander
{
    [GeneratedRegex(@"\{\{\s*(?<name>[a-z_]+)\s*\}\}")]
    private static partial Regex Reference();

    public static IReadOnlyDictionary<string, BindingValue> FromSpec(DatasetSpec spec)
    {
        ArgumentNullException.ThrowIfNull(spec);
        return new Dictionary<string, BindingValue>
        {
            ["watermark"] = new(spec.WatermarkValue, null),
            ["window_upper"] = new(spec.WatermarkUpperBound, null),
        };
    }

    public static IReadOnlyList<string> ReferencedBindings(string template)
    {
        ArgumentNullException.ThrowIfNull(template);
        var names = Reference().Matches(template).Select(m => m.Groups["name"].Value).Distinct().ToList();
        if (Reference().Replace(template, "").Contains("{{"))
        {
            throw new FormatException($"malformed binding reference in template '{template}'");
        }

        return names;
    }

    public static bool TryExpand(string template, IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, out string? result, out string? error)
    {
        ArgumentNullException.ThrowIfNull(template);
        ArgumentNullException.ThrowIfNull(bindings);
        ArgumentNullException.ThrowIfNull(format);

        var referencedNames = ReferencedBindings(template);

        foreach (var name in referencedNames)
        {
            if (!bindings.TryGetValue(name, out _))
            {
                result = null;
                error = $"unknown binding '{name}' (known: {string.Join(", ", bindings.Keys.Order())})";
                return false;
            }
        }

        // A null-valued binding signals the caller should omit the whole parameter.
        foreach (var name in referencedNames)
        {
            if (bindings[name].IsNull)
            {
                result = null;
                error = null;
                return true;
            }
        }

        result = Reference().Replace(template, m => format(m.Groups["name"].Value, bindings[m.Groups["name"].Value]));
        error = null;
        return true;
    }
}
