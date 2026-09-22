using System.Buffers;
using System.Globalization;
using System.Numerics;
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
            // NaN and the infinities have no JSON number form; refused like any other value without a
            // canonical one, so the compiler reports it as a coded error.
            case double d when !double.IsFinite(d):
                throw new NotSupportedException("CanonicalJson cannot serialize a non-finite double.");
            case double d:
                writer.WriteNumberValue(d);
                break;
            // A Scriban kwarg literal too big for long (e.g. an author-typed huge id) evaluates to
            // BigInteger, not a parse error -- JSON numbers are arbitrary precision, so the decimal
            // digits ARE the canonical, lossless form. Utf8JsonWriter has no BigInteger overload, so
            // this writes the digits as a raw (unquoted) JSON number rather than routing through
            // WriteNumberValue.
            case BigInteger bi:
                writer.WriteRawValue(bi.ToString(CultureInfo.InvariantCulture), skipInputValidation: true);
                break;
            // A `1.5m`-suffixed Scriban literal evaluates to decimal, not double -- Utf8JsonWriter has a
            // direct decimal overload, so this is lossless without any string round trip.
            case decimal m:
                writer.WriteNumberValue(m);
                break;
            // Unreachable through today's sandboxed kwarg surface (the `date` builtin is stripped
            // along with every other Scriban builtin object), but CanonicalJson is a shared utility,
            // not kwarg-specific -- ISO-8601 round-trip ("O") is lossless and deterministic for a
            // given DateTime.Kind.
            case DateTime dt:
                writer.WriteStringValue(dt);
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
