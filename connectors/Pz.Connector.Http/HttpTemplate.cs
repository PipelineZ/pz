using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>Expands templates that reference `{{ item.&lt;name&gt; }}` alongside the engine bindings, in
/// one pass: every reference is matched in the template text only, and a substituted value is never
/// scanned again, so an API value that looks like a reference stays literal. Templates without item
/// references never come here; they keep going through <see cref="BindingExpander"/>.</summary>
internal static partial class HttpTemplate
{
    // Item names are matched loosely so a badly named reference is reported by name rather than as a
    // malformed template; engine names match exactly what BindingExpander accepts.
    [GeneratedRegex(@"\{\{\s*(?:item\.(?<item>[A-Za-z0-9_]+)|(?<name>[a-z_]+))\s*\}\}")]
    private static partial Regex Reference();

    [GeneratedRegex(@"\{\{\s*item\.")]
    private static partial Regex ItemPrefix();

    [GeneratedRegex(@"^\s*\{\{\s*item\.(?<item>[A-Za-z0-9_]+)\s*\}\}\s*$")]
    private static partial Regex WholeItem();

    [GeneratedRegex(@"^[a-z_][a-z0-9_]*$")]
    private static partial Regex FieldName();

    public static bool HasItemReferences(string template) => ItemPrefix().IsMatch(template);

    public static IReadOnlyList<string> ItemReferences(string template)
        => Matches(template, "item");

    public static IReadOnlyList<string> EngineReferences(string template)
        => Matches(template, "name");

    /// <summary>The field name when the whole template (surrounding blanks aside) is one item
    /// reference, the shape that keeps the field's JSON type in a body; otherwise null.</summary>
    public static string? WholeItemReference(string template)
        => WholeItem().Match(template) is { Success: true } m ? m.Groups["item"].Value : null;

    /// <summary>A field's value as request text: a string as itself, a number or bool as its JSON
    /// text, so `1e3` and `12.50` are sent exactly as the API wrote them.</summary>
    public static string ItemText(JsonNode node)
        => node is JsonValue v && v.GetValueKind() == JsonValueKind.String ? v.GetValue<string>() : node.ToJsonString();

    /// <summary>Offline check of a template: well-formed, engine names known, and item references
    /// allowed here (<paramref name="itemFields"/> non-null), well named and declared.</summary>
    public static bool TryValidate(string template, IReadOnlyCollection<string> knownBindings,
        IReadOnlyCollection<string>? itemFields, out string? error)
    {
        if (IsMalformed(template))
        {
            error = "malformed binding template (expected '{{ name }}' or '{{ item.name }}' with a lowercase name)";
            return false;
        }

        var errors = new List<string>();
        foreach (var name in EngineReferences(template).Where(n => !knownBindings.Contains(n)))
        {
            errors.Add($"unknown binding '{name}' — accepted: {string.Join(", ", knownBindings)}");
        }

        foreach (var name in ItemReferences(template))
        {
            if (itemFields is null)
            {
                errors.Add($"'{{{{ item.{name} }}}}' is only valid in the for_each item request's path, query and body");
            }
            else if (!IsFieldName(name))
            {
                errors.Add($"item field '{name}' is not a valid name (lowercase letters, digits and '_', " +
                    "not starting with a digit or 'pz_')");
            }
            else if (!itemFields.Contains(name))
            {
                errors.Add($"unknown item field '{name}' — declared: {string.Join(", ", itemFields)}");
            }
        }

        error = errors.Count == 0 ? null : string.Join("; ", errors);
        return error is null;
    }

    /// <summary>Expands engine and item references in one pass. A referenced engine binding that is
    /// null yields a null <paramref name="result"/> with no error, as <see cref="BindingExpander"/>
    /// does, so the caller can drop a query parameter. A referenced item field that is missing, null,
    /// an object or an array is an error naming the field.</summary>
    public static bool TryExpand(string template, IReadOnlyDictionary<string, BindingValue> bindings,
        ItemValues? item, Func<string, BindingValue, string> formatEngine, Func<string, JsonNode, string> formatItem,
        out string? result, out string? error)
    {
        result = null;
        if (IsMalformed(template))
        {
            error = "malformed binding template";
            return false;
        }

        foreach (var name in EngineReferences(template))
        {
            if (!bindings.ContainsKey(name))
            {
                error = $"unknown binding '{name}' (known: {string.Join(", ", bindings.Keys.Order())})";
                return false;
            }
        }

        foreach (var name in ItemReferences(template))
        {
            if (!TryGetField(item, name, out _, out error))
            {
                return false;
            }
        }

        error = null;
        if (EngineReferences(template).Any(n => bindings[n].IsNull))
        {
            return true;
        }

        result = Reference().Replace(template, m => m.Groups["item"].Success
            ? formatItem(m.Groups["item"].Value, item!.Fields[m.Groups["item"].Value]!)
            : formatEngine(m.Groups["name"].Value, bindings[m.Groups["name"].Value]));
        return true;
    }

    /// <summary>A referenced field's value, which must be a JSON string, number or bool.</summary>
    public static bool TryGetField(ItemValues? item, string name, out JsonNode? value, out string? error)
    {
        value = null;
        if (item is null)
        {
            error = $"'{{{{ item.{name} }}}}' has no item here (item references are only valid in the for_each item request)";
            return false;
        }

        if (!item.Fields.TryGetValue(name, out var node))
        {
            error = $"item field '{name}' is missing from the record";
            return false;
        }

        var kind = node?.GetValueKind();
        if (kind is not (JsonValueKind.String or JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False))
        {
            error = $"item field '{name}' is {Describe(kind)}; a referenced field must be a string, number or boolean";
            return false;
        }

        value = node;
        error = null;
        return true;
    }

    private static string Describe(JsonValueKind? kind) => kind switch
    {
        JsonValueKind.Object => "an object",
        JsonValueKind.Array => "an array",
        _ => "null",
    };

    private static bool IsFieldName(string name)
        => FieldName().IsMatch(name) && !name.StartsWith("pz_", StringComparison.Ordinal);

    private static bool IsMalformed(string template) => Reference().Replace(template, "").Contains("{{");

    private static List<string> Matches(string template, string group)
        => [.. Reference().Matches(template).Where(m => m.Groups[group].Success)
            .Select(m => m.Groups[group].Value).Distinct()];
}
