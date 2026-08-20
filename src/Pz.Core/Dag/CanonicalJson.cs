using System.Buffers;
using System.Text;
using System.Text.Json;

namespace Pz.Core.Dag;

/// <summary>
/// Deterministic JSON serialization used as hashing input: object keys sorted ordinal,
/// no whitespace, invariant number formatting, UTF-8. Used for the dictionary-shaped
/// config values (connection/options/etc.) that feed <see cref="NodeId.Compute"/>.
/// </summary>
public static class CanonicalJson
{
    public static string Serialize(object? value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            Write(writer, value);
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static void Write(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case string s:
                writer.WriteStringValue(s);
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case IReadOnlyDictionary<string, object?> objDict:
                WriteObject(writer, objDict.Select(kv => (kv.Key, (object?)kv.Value)));
                break;
            case IReadOnlyDictionary<string, string> strDict:
                WriteObject(writer, strDict.Select(kv => (kv.Key, (object?)kv.Value)));
                break;
            case IEnumerable<object?> list:
                writer.WriteStartArray();
                foreach (var item in list)
                {
                    Write(writer, item);
                }

                writer.WriteEndArray();
                break;
            default:
                throw new NotSupportedException($"CanonicalJson cannot serialize value of type {value.GetType()}.");
        }
    }

    private static void WriteObject(Utf8JsonWriter writer, IEnumerable<(string Key, object? Value)> entries)
    {
        writer.WriteStartObject();
        foreach (var (key, entryValue) in entries.OrderBy(e => e.Key, StringComparer.Ordinal))
        {
            writer.WritePropertyName(key);
            Write(writer, entryValue);
        }

        writer.WriteEndObject();
    }
}
