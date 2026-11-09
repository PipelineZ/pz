using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>The `for_each:` dataset option: a list request whose records each drive one item request
/// (the dataset's own request). <see cref="List"/> is parsed by the dataset parser itself, with the
/// keys a list request cannot have refused; <see cref="Fields"/> are the values picked from each list
/// record, in declaration order, for `{{ item.&lt;name&gt; }}` references.</summary>
internal sealed partial record HttpForEachConfig(HttpDatasetConfig List,
    IReadOnlyList<(string Name, string Pointer)> Fields, int? MaxItems, bool OnMissingSkip, int Concurrency = 1)
{
    /// <summary>Keys that describe the item request or the dataset as a whole, never the list request.</summary>
    internal static readonly string[] ItemOnlyKeys =
        ["file", "export", "columns", "cursor", "cursor_type", "cursor_pointer", "cursor_order", "delta_pointer",
         "for_each", "run_date", "watermark_format", "epoch_unit", "column_pointers"];

    [GeneratedRegex(@"^[a-z_][a-z0-9_]*$")]
    private static partial Regex FieldName();

    public static HttpForEachConfig? Parse(object? raw, DatasetSpec spec, List<string> errors)
        => Parse(raw, spec, errors, out _);

    /// <summary>As <see cref="Parse(object?, DatasetSpec, List{string})"/>; <paramref name="fieldNames"/>
    /// is the well-named declared fields whenever `for_each` is present, even when the block has
    /// other errors, so item references in the item request are not reported a second time.</summary>
    public static HttpForEachConfig? Parse(object? raw, DatasetSpec spec, List<string> errors,
        out IReadOnlyCollection<string>? fieldNames)
    {
        fieldNames = null;
        if (raw is null)
        {
            return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'for_each' must be a map (path, items, fields, ...)");
            fieldNames = [];
            return null;
        }

        var before = errors.Count;
        var fields = ParseFields(block, errors);
        fieldNames = [.. fields.Select(f => f.Name)];

        var list = HttpDatasetConfig.ParseCore(block, spec, errors, listRequest: true);

        int? maxItems = null;
        if (block.TryGetValue("max_items", out var mi) && mi is not null)
        {
            if (YamlScalarText.NonScalarKind(mi) is null && long.TryParse(YamlScalarText.Of(mi), out var n) && n > 0)
            {
                maxItems = (int)Math.Min(n, int.MaxValue);
            }
            else
            {
                errors.Add($"for_each.max_items must be a positive integer, got '{YamlScalarText.Of(mi)}'");
            }
        }

        var concurrency = HttpDatasetConfig.ParseConcurrency(block, "for_each.concurrency", errors);

        var onMissing = block.TryGetValue("on_missing", out var om) ? YamlScalarText.Of(om) : null;
        if (onMissing is not (null or "error" or "skip"))
        {
            errors.Add($"for_each.on_missing must be 'error' or 'skip', got '{onMissing}'");
        }

        if (errors.Count > before)
        {
            return null;
        }

        return new HttpForEachConfig(list, fields, maxItems, onMissing == "skip", concurrency);
    }

    private static List<(string Name, string Pointer)> ParseFields(IReadOnlyDictionary<string, object?> block,
        List<string> errors)
    {
        var fields = new List<(string, string)>();
        if (!block.TryGetValue("fields", out var raw) || raw is null)
        {
            errors.Add("for_each.fields is required (a map of name to JSON pointer into each list record)");
            return fields;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("for_each.fields must be a map of name to JSON pointer");
            return fields;
        }

        if (map.Count == 0)
        {
            errors.Add("for_each.fields must declare at least one field");
            return fields;
        }

        foreach (var (name, value) in map)
        {
            if (!FieldName().IsMatch(name) || name.StartsWith("pz_", StringComparison.Ordinal))
            {
                errors.Add($"for_each.fields name '{name}' is not valid (lowercase letters, digits and '_', " +
                    "not starting with a digit or 'pz_')");
                continue;
            }

            if (YamlScalarText.NonScalarKind(value) is not null || YamlScalarText.Of(value) is not { } pointer)
            {
                errors.Add($"for_each.fields.{name} must be a JSON pointer string");
                continue;
            }

            HttpDatasetConfig.ValidatePointer(pointer, $"for_each.fields.{name}", errors);
            fields.Add((name, pointer));
        }

        return fields;
    }
}
