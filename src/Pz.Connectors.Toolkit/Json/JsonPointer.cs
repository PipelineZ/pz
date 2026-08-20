using System.Text.Json.Nodes;

namespace Pz.Connectors.Toolkit.Json;

/// <summary>Minimal RFC 6901 JSON Pointer over <see cref="JsonNode"/> (System.Text.Json has no
/// pointer API). "" is the whole document. Missing data returns false; a syntactically malformed
/// pointer throws <see cref="ArgumentException"/> (callers validate pointers offline first).</summary>
public static class JsonPointer
{
    public static bool TryResolve(JsonNode? root, string pointer, out JsonNode? result)
    {
        ArgumentNullException.ThrowIfNull(pointer);
        if (pointer.Length == 0)
        {
            result = root;
            return true;
        }

        if (pointer[0] != '/')
        {
            throw new ArgumentException($"JSON pointer must be empty or start with '/': '{pointer}'", nameof(pointer));
        }

        var current = root;
        foreach (var raw in pointer[1..].Split('/'))
        {
            var segment = raw.Replace("~1", "/").Replace("~0", "~");
            switch (current)
            {
                case JsonObject obj when obj.TryGetPropertyValue(segment, out var child):
                    current = child;
                    break;
                case JsonArray arr when int.TryParse(segment, out var index) && index >= 0 && index < arr.Count:
                    current = arr[index];
                    break;
                default:
                    result = null;
                    return false;
            }
        }

        result = current;
        return true;
    }
}
