using System.Net.Http.Headers;
using System.Text;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>The request body of a POST read: a map (sent as JSON, or form-encoded when
/// `body_format: form`) or a string sent as-is. Binding references are expanded in string values
/// only, with the same vocabulary as `query:`. Nothing here puts body content in a message.</summary>
internal sealed class HttpBody
{
    private static readonly HashSet<string> NumericTypes = ["int", "bigint", "double", "decimal"];

    private readonly JsonNode? template;
    private readonly string? text;
    private readonly bool form;

    private HttpBody(JsonNode? template, string? text, bool form, string mediaType)
    {
        this.template = template;
        this.text = text;
        this.form = form;
        MediaType = mediaType;
    }

    public string MediaType { get; }

    /// <summary>True when the body is a JSON map (the shape body pagination can write into).</summary>
    public bool IsJson => template is not null && !form;

    /// <summary>True when the body is a string sent as-is.</summary>
    public bool IsText => text is not null;

    /// <summary>The unexpanded JSON template of a map body; null for form and string bodies.</summary>
    public JsonNode? Template => IsJson ? template : null;

    public static HttpBody? Parse(object? raw, string? bodyFormat, string? contentType,
        IReadOnlyCollection<string> knownBindings, List<string> errors,
        IReadOnlyCollection<string>? itemFields = null)
    {
        if (raw is null)
        {
            return null;
        }

        if (bodyFormat is not (null or "json" or "form"))
        {
            errors.Add($"'body_format' must be 'json' or 'form', got '{bodyFormat}'");
            bodyFormat = null;
        }

        var isForm = bodyFormat == "form";
        var defaultType = isForm ? "application/x-www-form-urlencoded" : "application/json";
        var mediaType = string.IsNullOrWhiteSpace(contentType) ? defaultType : contentType;
        if (!MediaTypeHeaderValue.TryParse(mediaType, out _))
        {
            errors.Add($"'content_type' is not a valid media type: '{mediaType}'");
            mediaType = defaultType;
        }

        if (raw is string s)
        {
            if (isForm)
            {
                errors.Add("'body_format: form' requires 'body' to be a flat map of scalars, not a string");
                return null;
            }

            CheckTemplate(s, "body", knownBindings, itemFields, errors);
            return new HttpBody(null, s, false, mediaType);
        }

        if (YamlScalarText.NonScalarKind(raw) != "map")
        {
            errors.Add("'body' must be a map or a string");
            return null;
        }

        var before = errors.Count;
        var node = ToJson(raw, "", knownBindings, itemFields, errors);
        if (isForm)
        {
            if (node is JsonObject obj && obj.All(kv => kv.Value is null or JsonValue))
            {
                return errors.Count == before ? new HttpBody(node, null, true, mediaType) : null;
            }

            errors.Add("'body_format: form' requires 'body' to be a flat map of scalars " +
                "(no nested maps or lists)");
            return null;
        }

        return errors.Count == before ? new HttpBody(node, null, false, mediaType) : null;
    }

    /// <summary>Expands a map body to a fresh JSON tree. A string value that references a null binding
    /// becomes JSON null; a string that is exactly one binding takes a JSON number when the binding's
    /// cursor type is numeric; a string that is exactly one item reference takes the field's JSON value
    /// with its type.</summary>
    public JsonNode? Render(IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, out string? error, ItemValues? item = null)
    {
        error = null;
        if (template is null)
        {
            error = "the body is a string; use RenderText";
            return null;
        }

        return Expand(template.DeepClone(), bindings, format, item, ref error);
    }

    /// <summary>Renders a string body (or a form body as `k=v&amp;...`). A null binding in a string
    /// body is refused: sending a half-rendered body would be a silent wrong result.</summary>
    public string RenderText(IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, ItemValues? item = null)
    {
        if (form)
        {
            var pairs = new List<string>();
            foreach (var (key, value) in (JsonObject)template!)
            {
                var raw = value is JsonValue v && v.TryGetValue<string>(out var str) ? str : value?.ToJsonString();
                if (raw is null)
                {
                    continue;
                }

                var expanded = ExpandString(raw, bindings, format, out var err, item);
                if (err is not null)
                {
                    throw new PzConnectorException($"body field '{key}': {err}", isTransient: false);
                }

                if (expanded is not null)
                {
                    pairs.Add($"{Uri.EscapeDataString(key)}={Uri.EscapeDataString(expanded)}");
                }
            }

            return string.Join('&', pairs);
        }

        var result = ExpandString(text!, bindings, format, out var error, item);
        if (error is not null)
        {
            throw new PzConnectorException($"body: {error}", isTransient: false);
        }

        if (result is null)
        {
            var referenced = HttpTemplate.HasItemReferences(text!)
                ? HttpTemplate.EngineReferences(text!)
                : BindingExpander.ReferencedBindings(text!);
            var nulls = referenced.Where(n => bindings[n].IsNull);
            throw new PzConnectorException(
                $"body references binding(s) {string.Join(", ", nulls.Select(n => $"'{n}'"))} with no value " +
                "(first run or --full-refresh); a string body cannot omit a fragment — set an " +
                "'initial:' bound on the incremental so the watermark is never null", isTransient: false);
        }

        return result;
    }

    public static HttpContent ToContent(JsonNode? json, string? text, HttpBody body)
    {
        var payload = text ?? json?.ToJsonString() ?? "null";
        // StringContent's media-type argument refuses parameters, but 'content_type' may carry them
        // (Parse accepted it with TryParse). The payload is UTF-8, so a declared charset stays as
        // written and a missing one is filled in.
        var content = new StringContent(payload, Encoding.UTF8);
        var header = MediaTypeHeaderValue.Parse(body.MediaType);
        header.CharSet ??= "utf-8";
        content.Headers.ContentType = header;
        return content;
    }

    private static JsonNode? Expand(JsonNode? node, IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, ItemValues? item, ref string? error)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(kv => kv.Key).ToList())
                {
                    var child = obj[key];
                    var expanded = Expand(child, bindings, format, item, ref error);
                    if (!ReferenceEquals(expanded, child))
                    {
                        obj[key] = expanded;
                    }
                }

                return obj;
            case JsonArray arr:
                for (var i = 0; i < arr.Count; i++)
                {
                    var child = arr[i];
                    var expanded = Expand(child, bindings, format, item, ref error);
                    if (!ReferenceEquals(expanded, child))
                    {
                        arr[i] = expanded;
                    }
                }

                return arr;
            case JsonValue v when v.TryGetValue<string>(out var s):
                return HttpTemplate.HasItemReferences(s)
                    ? ExpandItemValue(s, bindings, format, item, ref error)
                    : ExpandValue(s, bindings, format, ref error);
            default:
                return node;
        }
    }

    private static JsonNode? ExpandValue(string s, IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, ref string? error)
    {
        var text = ExpandString(s, bindings, format, out var err);
        if (err is not null)
        {
            error ??= err;
            return null;
        }

        if (text is null)
        {
            return null;
        }

        var names = BindingExpander.ReferencedBindings(s);
        if (names.Count == 1 && IsWholeValue(s, names[0])
            && bindings[names[0]].TypeName is { } type && NumericTypes.Contains(type))
        {
            // Parsed as a JSON literal so big integers and exponents keep their digits; a value that
            // is not a number is refused rather than sent as a string the API would misread.
            if (TryParseNumber(text, out var number))
            {
                return number;
            }

            error ??= $"binding '{names[0]}' is not a number for cursor_type '{type}'";
            return null;
        }

        return JsonValue.Create(text);
    }

    private static JsonNode? ExpandItemValue(string s, IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, ItemValues? item, ref string? error)
    {
        if (HttpTemplate.WholeItemReference(s) is { } name)
        {
            if (HttpTemplate.TryGetField(item, name, out var value, out var fieldError))
            {
                return value!.DeepClone();
            }

            error ??= fieldError;
            return null;
        }

        var text = ExpandString(s, bindings, format, out var err, item);
        if (err is not null)
        {
            error ??= err;
            return null;
        }

        return text is null ? null : JsonValue.Create(text);
    }

    private static bool TryParseNumber(string text, out JsonNode? number)
    {
        number = null;
        try
        {
            var node = JsonNode.Parse(text.Trim());
            if (node is JsonValue v && v.GetValueKind() == System.Text.Json.JsonValueKind.Number)
            {
                number = node;
                return true;
            }
        }
        catch (System.Text.Json.JsonException)
        {
        }

        return false;
    }

    private static bool IsWholeValue(string s, string name)
    {
        var t = s.Trim();
        return t.StartsWith("{{", StringComparison.Ordinal) && t.EndsWith("}}", StringComparison.Ordinal)
            && t[2..^2].Trim() == name;
    }

    internal static string? ExpandString(string s, IReadOnlyDictionary<string, BindingValue> bindings,
        Func<string, BindingValue, string> format, out string? error, ItemValues? item = null)
    {
        if (HttpTemplate.HasItemReferences(s))
        {
            return HttpTemplate.TryExpand(s, bindings, item, format, (_, node) => HttpTemplate.ItemText(node),
                out var expanded, out error) ? expanded : null;
        }

        try
        {
            return BindingExpander.TryExpand(s, bindings, format, out var result, out error) ? result : null;
        }
        catch (FormatException)
        {
            error = "malformed binding template";
            return null;
        }
    }

    private static JsonNode? ToJson(object? value, string path, IReadOnlyCollection<string> knownBindings,
        IReadOnlyCollection<string>? itemFields, List<string> errors)
    {
        switch (value)
        {
            case null:
                return null;
            case string s:
                CheckTemplate(s, $"body value at '{(path.Length == 0 ? "/" : path)}'", knownBindings, itemFields, errors);
                return JsonValue.Create(s);
            case bool b:
                return JsonValue.Create(b);
            case int or long or short or byte or sbyte or ushort or uint:
                return JsonValue.Create(Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
            case ulong ul:
                return JsonValue.Create(ul);
            case float or double:
                return JsonValue.Create(Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture));
            case decimal d:
                return JsonValue.Create(d);
        }

        if (YamlScalarText.NonScalarKind(value) == "map")
        {
            var obj = new JsonObject();
            foreach (var (k, v) in Entries(value))
            {
                obj[k] = ToJson(v, $"{path}/{k}", knownBindings, itemFields, errors);
            }

            return obj;
        }

        if (value is System.Collections.IEnumerable items)
        {
            var arr = new JsonArray();
            var i = 0;
            foreach (var item in items)
            {
                arr.Add(ToJson(item, $"{path}/{i++}", knownBindings, itemFields, errors));
            }

            return arr;
        }

        return JsonValue.Create(YamlScalarText.Of(value));
    }

    private static IEnumerable<(string Key, object? Value)> Entries(object map)
    {
        if (map is IReadOnlyDictionary<string, object?> ro)
        {
            return ro.Select(kv => (kv.Key, kv.Value));
        }

        return ((System.Collections.IDictionary)map).Cast<System.Collections.DictionaryEntry>()
            .Select(e => (e.Key.ToString() ?? "", e.Value));
    }

    private static void CheckTemplate(string template, string label,
        IReadOnlyCollection<string> knownBindings, IReadOnlyCollection<string>? itemFields, List<string> errors)
        => HttpDatasetConfig.ValidateBindingTemplate(template, label, knownBindings, errors,
            echoTemplate: false, itemFields: itemFields);
}
