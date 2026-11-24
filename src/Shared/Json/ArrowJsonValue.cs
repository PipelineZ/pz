using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Arrow;

/// <summary>One Arrow cell as a JSON value, at any nesting depth: numbers as numbers (a non-finite double as
/// <c>null</c>, a decimal beyond <see cref="decimal"/> as its exact digit string), dates as
/// <c>yyyy-MM-dd</c>, timestamps as ISO-8601 UTC with microseconds and <c>Z</c>, a struct as an object, a
/// list or fixed-size list as an array, a map as an object whose keys are the map keys' text (DuckDB's own
/// <c>::JSON</c> renders a map the same way). Child indexing follows Apache.Arrow's accessors: a struct's
/// children are sliced with it, a list's values are not (its value offsets are), and a fixed-size list's
/// values start at <c>(offset + row) * size</c>.</summary>
internal static class ArrowJsonValue
{
    public static void Write(Utf8JsonWriter json, IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            json.WriteNullValue();
            return;
        }

        switch (array)
        {
            case Int32Array a:
                json.WriteNumberValue(a.GetValue(index)!.Value);
                break;
            case Int64Array a:
                json.WriteNumberValue(a.GetValue(index)!.Value);
                break;
            case DoubleArray a:
                // JSON has no literal for NaN or ±Infinity.
                var d = a.GetValue(index)!.Value;
                if (double.IsFinite(d))
                {
                    json.WriteNumberValue(d);
                }
                else
                {
                    json.WriteNullValue();
                }

                break;
            case Decimal128Array a when Decimal128Cells.FitsSystemDecimal(a, index):
                json.WriteNumberValue(a.GetValue(index)!.Value);
                break;
            case Decimal128Array a:
                json.WriteRawValue(a.GetString(index));
                break;
            case BooleanArray a:
                json.WriteBooleanValue(a.GetValue(index)!.Value);
                break;
            case StringArray a:
                json.WriteStringValue(a.GetBytes(index));
                break;
            case Date32Array a:
                json.WriteStringValue(FormatDate(a, index));
                break;
            case TimestampArray a:
                json.WriteStringValue(FormatTimestamp(a.GetTimestamp(index)!.Value));
                break;
            case MapArray a:
            {
                // A map is a list of key/value structs; a key is never null.
                var entries = a.KeyValues;
                json.WriteStartObject();
                for (var i = a.ValueOffsets[index]; i < a.ValueOffsets[index + 1]; i++)
                {
                    json.WritePropertyName(KeyText(entries.Fields[0], i));
                    Write(json, entries.Fields[1], i);
                }

                json.WriteEndObject();
                break;
            }

            case ListArray a:
                json.WriteStartArray();
                for (var i = a.ValueOffsets[index]; i < a.ValueOffsets[index + 1]; i++)
                {
                    Write(json, a.Values, i);
                }

                json.WriteEndArray();
                break;
            case FixedSizeListArray a:
            {
                var size = ((FixedSizeListType)a.Data.DataType).ListSize;
                var start = (a.Offset + index) * size;
                json.WriteStartArray();
                for (var i = start; i < start + size; i++)
                {
                    Write(json, a.Values, i);
                }

                json.WriteEndArray();
                break;
            }

            case StructArray a:
            {
                var fields = ((StructType)a.Data.DataType).Fields;
                json.WriteStartObject();
                for (var k = 0; k < fields.Count; k++)
                {
                    json.WritePropertyName(fields[k].Name);
                    Write(json, a.Fields[k], index);
                }

                json.WriteEndObject();
                break;
            }

            default:
                throw new NotSupportedException($"Arrow type '{array.Data.DataType}' has no JSON encoding");
        }
    }

    /// <summary>The cell as compact JSON text.</summary>
    public static string ToJson(IArrowArray array, int index)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true }))
        {
            Write(json, array, index);
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    public static string FormatDate(Date32Array array, int index) =>
        array.GetDateTime(index)!.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    /// <summary>ISO-8601 UTC, microsecond precision, 'Z' designator -- e.g. "2026-07-13T10:30:15.000000Z".</summary>
    public static string FormatTimestamp(DateTimeOffset value) =>
        value.UtcDateTime.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "Z";

    /// <summary>A map key as an object property name: a string as itself, a scalar as its text, anything
    /// else as its JSON.</summary>
    private static string KeyText(IArrowArray keys, int index) => keys switch
    {
        StringArray a => a.GetString(index),
        Int32Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
        Int64Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
        DoubleArray a => a.GetValue(index)!.Value.ToString("R", CultureInfo.InvariantCulture),
        Decimal128Array a => a.GetString(index),
        BooleanArray a => a.GetValue(index)!.Value ? "true" : "false",
        Date32Array a => FormatDate(a, index),
        TimestampArray a => FormatTimestamp(a.GetTimestamp(index)!.Value),
        _ => ToJson(keys, index),
    };
}
