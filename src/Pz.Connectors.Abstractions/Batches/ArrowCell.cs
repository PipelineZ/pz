using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Arrow;

namespace Pz.Connectors.Abstractions.Batches;

/// <summary>One cell of a batch a sink receives, read at any nesting depth: as JSON (for a destination that
/// stores documents or JSON text) or as a boxed .NET value (for one that builds its own values). The batch is
/// engine-owned only until the write call returns, so read what you need inside it.</summary>
public static class ArrowCell
{
    /// <summary>Writes the cell as a JSON value: numbers as numbers (a non-finite double as <c>null</c>, a decimal
    /// beyond <see cref="decimal"/> as its exact digits), dates as <c>yyyy-MM-dd</c>, timestamps as ISO-8601 UTC
    /// with microseconds and <c>Z</c>, a struct as an object, a list as an array, a map as an object keyed by the
    /// keys' text.</summary>
    public static void WriteJson(Utf8JsonWriter writer, IArrowArray array, int index)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(array);
        ArrowJsonValue.Write(writer, array, index);
    }

    /// <summary>The cell as compact JSON text (<see cref="WriteJson"/>).</summary>
    public static string ToJson(IArrowArray array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);
        return ArrowJsonValue.ToJson(array, index);
    }

    /// <summary>The cell as a boxed value, null for a null cell: <see cref="int"/>, <see cref="long"/>,
    /// <see cref="double"/>, <see cref="decimal"/> (or <see cref="System.Data.SqlTypes.SqlDecimal"/> beyond its
    /// range), <see cref="bool"/>, <see cref="string"/>, <see cref="DateOnly"/>, a UTC <see cref="DateTimeOffset"/>;
    /// a list or fixed-size list as <c>List&lt;object?&gt;</c>; a struct as <c>Dictionary&lt;string, object?&gt;</c>
    /// in field order; a map as <c>List&lt;KeyValuePair&lt;object, object?&gt;&gt;</c> in entry order (map keys need
    /// not be strings).</summary>
    public static object? GetValue(IArrowArray array, int index)
    {
        ArgumentNullException.ThrowIfNull(array);
        if (array.IsNull(index))
        {
            return null;
        }

        switch (array)
        {
            case Int32Array a: return a.GetValue(index)!.Value;
            case Int64Array a: return a.GetValue(index)!.Value;
            case DoubleArray a: return a.GetValue(index)!.Value;
            case Decimal128Array a when Decimal128Cells.FitsSystemDecimal(a, index): return a.GetValue(index)!.Value;
            case Decimal128Array a: return a.GetSqlDecimal(index)!.Value;
            case BooleanArray a: return a.GetValue(index)!.Value;
            case StringArray a: return a.GetString(index);
            case Date32Array a: return a.GetDateOnly(index)!.Value;
            case TimestampArray a: return a.GetTimestamp(index)!.Value.ToUniversalTime();
            case MapArray a:
            {
                var entries = a.KeyValues;
                var map = new List<KeyValuePair<object, object?>>(a.ValueOffsets[index + 1] - a.ValueOffsets[index]);
                for (var i = a.ValueOffsets[index]; i < a.ValueOffsets[index + 1]; i++)
                {
                    map.Add(new(GetValue(entries.Fields[0], i)!, GetValue(entries.Fields[1], i)));
                }

                return map;
            }

            case ListArray a:
            {
                var items = new List<object?>(a.ValueOffsets[index + 1] - a.ValueOffsets[index]);
                for (var i = a.ValueOffsets[index]; i < a.ValueOffsets[index + 1]; i++)
                {
                    items.Add(GetValue(a.Values, i));
                }

                return items;
            }

            case FixedSizeListArray a:
            {
                var size = ((FixedSizeListType)a.Data.DataType).ListSize;
                var start = (a.Offset + index) * size;
                var items = new List<object?>(size);
                for (var i = start; i < start + size; i++)
                {
                    items.Add(GetValue(a.Values, i));
                }

                return items;
            }

            case StructArray a:
            {
                var fields = ((StructType)a.Data.DataType).Fields;
                var values = new Dictionary<string, object?>(fields.Count, StringComparer.Ordinal);
                for (var k = 0; k < fields.Count; k++)
                {
                    values[fields[k].Name] = GetValue(a.Fields[k], index);
                }

                return values;
            }

            default:
                throw new NotSupportedException($"Arrow type '{array.Data.DataType}' is not a carried type");
        }
    }
}
