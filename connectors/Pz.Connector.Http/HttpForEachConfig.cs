using System.Globalization;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>The `for_each:` dataset option: a list request whose records each drive one item request
/// (the dataset's own request). <see cref="List"/> is parsed by the dataset parser itself, with the
/// keys a list request cannot have refused; <see cref="Fields"/> are the values picked from each list
/// record, in declaration order, for `{{ item.&lt;name&gt; }}` references. With `values:` there is no list
/// request (<see cref="List"/> is null): <see cref="Values"/> are the records, and their keys are the
/// fields.</summary>
internal sealed partial record HttpForEachConfig(HttpDatasetConfig? List,
    IReadOnlyList<(string Name, string Pointer)> Fields, int? MaxItems, bool OnMissingSkip, int Concurrency = 1)
{
    /// <summary>`for_each.values`: the literal records, one per item, each a map of every field. Null when
    /// the items come from a list request.</summary>
    public IReadOnlyList<JsonObject>? Values { get; init; }

    /// <summary>`for_each.dedupe`: skip a record whose fields tuple an earlier record of this read had.</summary>
    public bool Dedupe { get; init; }

    /// <summary>The list token a `values` read's checkpoints carry: the literal list has one "page".</summary>
    internal const string ValuesToken = "values";

    /// <summary>The keys of a list request, which `values` replaces.</summary>
    private static readonly string[] ListRequestKeys =
        ["path", "query", "method", "body", "body_format", "content_type", "items", "pagination", "error_pointer",
         "max_pages", "fields", "max_items"];

    /// <summary>Keys that describe the item request or the dataset as a whole, never the list request.</summary>
    internal static readonly string[] ItemOnlyKeys =
        ["file", "export", "columns", "cursor", "cursor_type", "cursor_pointer", "cursor_order", "delta_pointer",
         "for_each", "run_date", "watermark_format", "watermark_time_zone", "epoch_unit", "column_pointers",
         "stop_at_watermark"];

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
        var hasValues = block.TryGetValue("values", out var rawValues);
        List<(string Name, string Pointer)> fields;
        HttpDatasetConfig? list = null;
        List<JsonObject>? values = null;
        if (hasValues)
        {
            foreach (var key in ListRequestKeys.Where(block.ContainsKey))
            {
                errors.Add($"for_each.{key} cannot be combined with for_each.values (the values are the list)");
            }

            values = ParseValues(rawValues, errors, out var keys);
            fields = [.. keys.Select(k => (k, "/" + k))];
            fieldNames = keys;
        }
        else
        {
            fields = ParseFields(block, errors);
            fieldNames = [.. fields.Select(f => f.Name)];
            list = HttpDatasetConfig.ParseCore(block, spec, errors, listRequest: true);
        }

        var dedupe = false;
        if (block.TryGetValue("dedupe", out var rawDedupe) && rawDedupe is not null)
        {
            if (rawDedupe is bool flag)
            {
                dedupe = flag;
            }
            else
            {
                errors.Add($"for_each.dedupe must be a boolean, got '{YamlScalarText.Of(rawDedupe)}'");
            }
        }

        int? maxItems = null;
        if (!hasValues && block.TryGetValue("max_items", out var mi) && mi is not null)
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

        return new HttpForEachConfig(list, fields, maxItems, onMissing == "skip", concurrency)
        {
            Values = values,
            Dedupe = dedupe,
        };
    }

    /// <summary>Reads `for_each.values`: a non-empty list of maps with the same keys (the first entry's, in
    /// its order, are <paramref name="keys"/>), each value a scalar or null. Every problem is reported.</summary>
    private static List<JsonObject>? ParseValues(object? raw, List<string> errors, out List<string> keys)
    {
        keys = [];
        if (raw is string || YamlScalarText.NonScalarKind(raw) != "list"
            || raw is not System.Collections.IEnumerable entries || !entries.Cast<object?>().Any())
        {
            errors.Add("for_each.values must be a non-empty list of maps (one map of item fields per item)");
            return null;
        }

        var records = new List<JsonObject>();
        HashSet<string>? firstKeys = null;
        var index = -1;
        foreach (var entry in entries)
        {
            index++;
            if (entry is not IReadOnlyDictionary<string, object?> map)
            {
                errors.Add($"for_each.values[{index}] must be a map of field name to value");
                continue;
            }

            if (map.Count == 0)
            {
                errors.Add($"for_each.values[{index}] must declare at least one field");
                continue;
            }

            if (firstKeys is null)
            {
                firstKeys = [.. map.Keys];
                keys = [.. map.Keys.Where(IsFieldName)];
            }
            else if (!firstKeys.SetEquals(map.Keys))
            {
                errors.Add($"for_each.values[{index}] has keys {string.Join(", ", map.Keys.Order(StringComparer.Ordinal))}; " +
                    $"every entry must have the keys of the first ({string.Join(", ", firstKeys.Order(StringComparer.Ordinal))})");
            }

            var record = new JsonObject();
            foreach (var (name, value) in map)
            {
                if (!IsFieldName(name))
                {
                    errors.Add($"for_each.values[{index}] key '{name}' is not valid (lowercase letters, digits and " +
                        "'_', not starting with a digit or 'pz_')");
                    continue;
                }

                if (!TryScalar(value, out var node))
                {
                    errors.Add($"for_each.values[{index}].{name} must be a string, number, boolean or null, " +
                        $"got a {YamlScalarText.NonScalarKind(value)}");
                    continue;
                }

                record[name] = node;
            }

            records.Add(record);
        }

        return records;
    }

    /// <summary>A YAML scalar as the JSON value a list record would carry: numbers stay numbers, booleans
    /// booleans; false for a map or list.</summary>
    private static bool TryScalar(object? value, out JsonNode? node)
    {
        node = value switch
        {
            null => null,
            string s => JsonValue.Create(s),
            bool b => JsonValue.Create(b),
            int i => JsonValue.Create(i),
            long l => JsonValue.Create(l),
            double d => JsonValue.Create(d),
            float f => JsonValue.Create(f),
            decimal m => JsonValue.Create(m),
            _ when YamlScalarText.NonScalarKind(value) is not null => null,
            IConvertible c when c.GetTypeCode() is >= TypeCode.SByte and <= TypeCode.UInt64
                => JsonValue.Create(c.ToDecimal(CultureInfo.InvariantCulture)),
            _ => JsonValue.Create(YamlScalarText.Of(value)),
        };
        return value is null || YamlScalarText.NonScalarKind(value) is null;
    }

    private static bool IsFieldName(string name)
        => FieldName().IsMatch(name) && !name.StartsWith("pz_", StringComparison.Ordinal);

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
            if (!IsFieldName(name))
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
