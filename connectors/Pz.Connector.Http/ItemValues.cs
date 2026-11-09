using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>The declared fields of one list record, in declaration order. A field whose pointer does
/// not resolve is absent from <see cref="Fields"/>; one that resolves to JSON null maps to null. Types
/// are not checked here: an unreferenced field may be anything, and a referenced one is checked where
/// it is expanded.</summary>
internal sealed record ItemValues(IReadOnlyList<string> Order, IReadOnlyDictionary<string, JsonNode?> Fields)
{
    private const int MaxIdentityFields = 3;
    private const int MaxIdentityValue = 40;

    public static ItemValues From(JsonNode? record, IReadOnlyList<(string Name, string Pointer)> fields)
    {
        var order = new List<string>(fields.Count);
        var values = new Dictionary<string, JsonNode?>(fields.Count);
        foreach (var (name, pointer) in fields)
        {
            order.Add(name);
            if (JsonPointer.TryResolve(record, pointer, out var node))
            {
                values[name] = node;
            }
        }

        return new ItemValues(order, values);
    }

    /// <summary>Whether <paramref name="name"/> is a declared field, resolved or not.</summary>
    public bool Declares(string name) => Order.Contains(name);

    /// <summary>The field as a value of contract type <paramref name="typeName"/>: a JSON string follows the
    /// text rules (<see cref="TextValueParser"/>, so "" is NULL), any other value converts as a JSON
    /// page's column does (under <paramref name="options"/>' epoch rule), and a missing or null field is NULL.</summary>
    public object? ValueAs(string name, string typeName, string context, ContractReadOptions? options = null)
    {
        if (!Fields.TryGetValue(name, out var node) || node is null)
        {
            return null;
        }

        return node is JsonValue value && value.GetValueKind() == JsonValueKind.String
            ? TextValueParser.Parse(value.GetValue<string>(), typeName, $"{context}: column '{name}'")
            : ContractProjector.ConvertScalar(node, typeName, name, context, options ?? ContractReadOptions.Default);
    }

    /// <summary>A JSON object of every declared field in declaration order, so downstream SQL sees one key
    /// set on every row: a field the list record lacks is JSON null.</summary>
    public string ToJsonString()
    {
        var obj = new JsonObject();
        foreach (var name in Order)
        {
            // A node has one parent: the list record keeps its own.
            obj[name] = Fields.TryGetValue(name, out var node) ? node?.DeepClone() : null;
        }

        return obj.ToJsonString();
    }

    /// <summary>`item name=value[, …]` for messages: the first three fields, each value with every control
    /// character as a space and at most 40 characters, so an API value cannot flood or forge a log line.</summary>
    public string Identity()
    {
        var parts = Order.Take(MaxIdentityFields).Select(name =>
        {
            var text = !Fields.TryGetValue(name, out var node) ? "(missing)"
                : node is null ? "null"
                : HttpTemplate.ItemText(node);
            text = new string([.. text.Select(c => char.IsControl(c) ? ' ' : c)]);
            if (text.Length > MaxIdentityValue)
            {
                // Never keep half of a surrogate pair.
                text = text[..(char.IsHighSurrogate(text[MaxIdentityValue - 1]) ? MaxIdentityValue - 1 : MaxIdentityValue)];
            }

            return $"{name}={text}";
        });
        return "item " + string.Join(", ", parts);
    }
}
