using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Arrow;

/// <summary>Positional shape comparison for the two places a batch meets a schema it must line up
/// with: the connector SDK's data-plane stream (whose header is the planned read's schema) and the
/// engine's Arrow ingest (whose target is the staged table). Both bind columns by position, so the
/// comparison is field count and, per position, the structural type: nested child types, decimal
/// precision and scale, timestamp unit and timezone, fixed widths, dictionary and union parameters,
/// an extension type's storage. Names, nullability and metadata are reported but never make a
/// mismatch — a renamed column still binds to the right slot.
///
/// <para>Compiled into <c>Pz.Connectors.Sdk</c> and <c>Pz.DuckDb</c> from this one source file: the
/// two assemblies may not reference each other, and the ABI package between them references
/// Apache.Arrow alone and grows only by capability.</para></summary>
internal static class ArrowSchemaShape
{
    public static bool Same(Schema expected, Schema actual)
    {
        if (expected.FieldsList.Count != actual.FieldsList.Count)
        {
            return false;
        }

        for (var i = 0; i < expected.FieldsList.Count; i++)
        {
            if (!SameType(expected.FieldsList[i].DataType, actual.FieldsList[i].DataType))
            {
                return false;
            }
        }

        return true;
    }

    public static bool SameType(IArrowType expected, IArrowType actual)
    {
        // An extension type reports its storage's TypeId, so the two must be told apart first.
        if (expected is ExtensionType e || actual is ExtensionType)
        {
            return expected is ExtensionType left && actual is ExtensionType right
                && string.Equals(left.Name, right.Name, StringComparison.Ordinal)
                && SameType(left.StorageType, right.StorageType);
        }

        if (expected.TypeId != actual.TypeId)
        {
            return false;
        }

        return expected switch
        {
            Decimal32Type d => actual is Decimal32Type a && d.Precision == a.Precision && d.Scale == a.Scale,
            Decimal64Type d => actual is Decimal64Type a && d.Precision == a.Precision && d.Scale == a.Scale,
            Decimal128Type d => actual is Decimal128Type a && d.Precision == a.Precision && d.Scale == a.Scale,
            Decimal256Type d => actual is Decimal256Type a && d.Precision == a.Precision && d.Scale == a.Scale,
            FixedSizeBinaryType f => actual is FixedSizeBinaryType a && f.ByteWidth == a.ByteWidth,
            TimestampType t => actual is TimestampType a && t.Unit == a.Unit && SameTimezone(t.Timezone, a.Timezone),
            TimeBasedType t => actual is TimeBasedType a && t.Unit == a.Unit,
            IntervalType t => actual is IntervalType a && t.Unit == a.Unit,
            DictionaryType d => actual is DictionaryType a && d.Ordered == a.Ordered
                && SameType(d.IndexType, a.IndexType) && SameType(d.ValueType, a.ValueType),
            FixedSizeListType l => actual is FixedSizeListType a && l.ListSize == a.ListSize && SameChildren(l, a),
            UnionType u => actual is UnionType a && u.Mode == a.Mode && u.TypeIds.AsSpan().SequenceEqual(a.TypeIds) && SameChildren(u, a),
            NestedType n => actual is NestedType a && SameChildren(n, a),
            _ => true,
        };
    }

    private static bool SameChildren(NestedType expected, NestedType actual)
    {
        if (expected.Fields.Count != actual.Fields.Count)
        {
            return false;
        }

        for (var i = 0; i < expected.Fields.Count; i++)
        {
            if (!SameType(expected.Fields[i].DataType, actual.Fields[i].DataType))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>No timezone and an empty one both mean a naive timestamp.</summary>
    private static bool SameTimezone(string? expected, string? actual) =>
        string.IsNullOrEmpty(expected) ? string.IsNullOrEmpty(actual) : string.Equals(expected, actual, StringComparison.Ordinal);

    public static string Describe(Schema schema) =>
        string.Join(", ", schema.FieldsList.Select(f => $"{f.Name}:{Describe(f.DataType)}"));

    public static string Describe(IArrowType type)
    {
        var text = new StringBuilder();
        Append(text, type);
        return text.ToString();
    }

    private static void Append(StringBuilder text, IArrowType type)
    {
        switch (type)
        {
            case ExtensionType e:
                text.Append(e.Name).Append('<');
                Append(text, e.StorageType);
                text.Append('>');
                return;
            case Decimal32Type d:
                text.Append("decimal32(").Append(d.Precision).Append(", ").Append(d.Scale).Append(')');
                return;
            case Decimal64Type d:
                text.Append("decimal64(").Append(d.Precision).Append(", ").Append(d.Scale).Append(')');
                return;
            case Decimal128Type d:
                text.Append("decimal128(").Append(d.Precision).Append(", ").Append(d.Scale).Append(')');
                return;
            case Decimal256Type d:
                text.Append("decimal256(").Append(d.Precision).Append(", ").Append(d.Scale).Append(')');
                return;
            case FixedSizeBinaryType f:
                text.Append("fixed_size_binary[").Append(f.ByteWidth).Append(']');
                return;
            case TimestampType t:
                text.Append("timestamp[").Append(Unit(t.Unit));
                if (!string.IsNullOrEmpty(t.Timezone))
                {
                    text.Append(", tz=").Append(t.Timezone);
                }

                text.Append(']');
                return;
            case TimeBasedType t:
                text.Append(t.Name).Append('[').Append(Unit(t.Unit)).Append(']');
                return;
            case DictionaryType d:
                text.Append("dictionary<");
                Append(text, d.IndexType);
                text.Append(", ");
                Append(text, d.ValueType);
                text.Append('>');
                return;
            case FixedSizeListType l:
                text.Append("fixed_size_list<");
                Append(text, l.ValueDataType);
                text.Append(">[").Append(l.ListSize).Append(']');
                return;
            case MapType m:
                text.Append("map<");
                Append(text, m.KeyField.DataType);
                text.Append(", ");
                Append(text, m.ValueField.DataType);
                text.Append('>');
                return;
            case StructType or UnionType:
                text.Append(type.Name).Append('<');
                var children = ((NestedType)type).Fields;
                for (var i = 0; i < children.Count; i++)
                {
                    if (i > 0)
                    {
                        text.Append(", ");
                    }

                    text.Append(children[i].Name).Append(':');
                    Append(text, children[i].DataType);
                }

                text.Append('>');
                return;
            case NestedType n:
                // list, large_list, list_view, large_list_view, run_end_encoded: one child type carries
                // the structure, its field name does not.
                text.Append(n.Name).Append('<');
                for (var i = 0; i < n.Fields.Count; i++)
                {
                    if (i > 0)
                    {
                        text.Append(", ");
                    }

                    Append(text, n.Fields[i].DataType);
                }

                text.Append('>');
                return;
            default:
                text.Append(type.Name);
                return;
        }
    }

    private static string Unit(TimeUnit unit) => unit switch
    {
        TimeUnit.Second => "s",
        TimeUnit.Millisecond => "ms",
        TimeUnit.Microsecond => "us",
        TimeUnit.Nanosecond => "ns",
        _ => unit.ToString(),
    };
}
