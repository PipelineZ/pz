using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Validation;

namespace Pz.Mcp;

/// <summary>The JSON fragments more than one tool result (or the envelope itself) writes. Each shape is
/// part of the append-only envelope contract, so there is exactly one writer per shape.</summary>
internal static class EnvelopeJson
{
    /// <summary>The <c>errors</c> array: code/message/file?/line?/next_step.</summary>
    internal static void WriteErrors(Utf8JsonWriter json, IReadOnlyList<PzError> errors)
    {
        json.WriteStartArray("errors");
        foreach (var e in errors)
        {
            json.WriteStartObject();
            json.WriteString("code", e.Code);
            json.WriteString("message", e.Message);
            if (e.File is { } file) { json.WriteString("file", file); }
            if (e.Line is { } line) { json.WriteNumber("line", line); }
            json.WriteString("next_step", e.Hint);
            json.WriteEndObject();
        }

        json.WriteEndArray();
    }

    /// <summary>The <c>warnings</c> array: code/message/file?/line?/hint?.</summary>
    internal static void WriteWarnings(Utf8JsonWriter json, IReadOnlyList<PzWarning> warnings)
    {
        json.WriteStartArray("warnings");
        foreach (var w in warnings)
        {
            json.WriteStartObject();
            json.WriteString("code", w.Code);
            json.WriteString("message", w.Message);
            if (w.File is { } file)
            {
                json.WriteString("file", file);
            }

            if (w.Line is { } line)
            {
                json.WriteNumber("line", line);
            }

            if (w.Hint is { } hint)
            {
                json.WriteString("hint", hint);
            }

            json.WriteEndObject();
        }

        json.WriteEndArray();
    }

    internal static void WriteStringArray(Utf8JsonWriter json, string propertyName, IReadOnlyList<string> values)
    {
        json.WriteStartArray(propertyName);
        foreach (var value in values)
        {
            json.WriteStringValue(value);
        }

        json.WriteEndArray();
    }

    /// <summary>The compiled DAG's <c>nodes</c> array: id/name/kind/dependsOn.</summary>
    internal static void WriteDagNodes(Utf8JsonWriter json, IReadOnlyList<DagNode> nodes)
    {
        json.WriteStartArray("nodes");
        foreach (var node in nodes)
        {
            json.WriteStartObject();
            json.WriteString("id", node.Id.Value);
            json.WriteString("name", node.Name);
            json.WriteString("kind", node.Kind.ToString());
            json.WriteStartArray("dependsOn");
            foreach (var dep in node.DependsOn)
            {
                json.WriteStringValue(dep.Value);
            }

            json.WriteEndArray();
            json.WriteEndObject();
        }

        json.WriteEndArray();
    }
}
