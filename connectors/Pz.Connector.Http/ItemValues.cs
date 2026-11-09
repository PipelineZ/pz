using System.Text.Json.Nodes;
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

    /// <summary>`item name=value[, …]` for messages: the first three fields, each value on one line and
    /// at most 40 characters, so an API value cannot flood or forge a log line.</summary>
    public string Identity()
    {
        var parts = Order.Take(MaxIdentityFields).Select(name =>
        {
            var text = !Fields.TryGetValue(name, out var node) ? "(missing)"
                : node is null ? "null"
                : HttpTemplate.ItemText(node);
            text = text.Replace('\r', ' ').Replace('\n', ' ');
            return $"{name}={(text.Length > MaxIdentityValue ? text[..MaxIdentityValue] : text)}";
        });
        return "item " + string.Join(", ", parts);
    }
}
