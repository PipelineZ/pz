using System.Data.SqlTypes;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connectors.Abstractions.Formats;

/// <summary>The <c>columns:</c> contract type grammar: the eight flat types (<c>int</c>, <c>bigint</c>,
/// <c>double</c>, <c>decimal</c>, <c>varchar</c>, <c>boolean</c>, <c>date</c>, <c>timestamp</c>) and, built from
/// them, <c>&lt;type&gt;[]</c> (a list), <c>&lt;type&gt;[n]</c> (a fixed-size array),
/// <c>struct(&lt;name&gt; &lt;type&gt;, …)</c> and <c>map(&lt;flat type&gt;, &lt;type&gt;)</c>, nested to any depth —
/// DuckDB's own spelling with pz's type names. A struct field name is a bare identifier or a double-quoted one
/// (<c>""</c> for a quote). Every connector and the engine read a contract through this one parser.</summary>
public static class ColumnContractType
{
    /// <summary>The flat type names, in the order messages list them.</summary>
    public static IReadOnlyList<string> FlatTypes { get; } =
        ["int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp"];

    /// <summary>How a type is spelled, for messages.</summary>
    public const string Syntax =
        "int, bigint, double, decimal, varchar, boolean, date, timestamp, or nested: varchar[], int[3], " +
        "struct(name varchar, tags varchar[]), map(varchar, bigint)";

    /// <summary>Parses <paramref name="text"/>; on failure <paramref name="error"/> says what is wrong and
    /// where.</summary>
    public static bool TryParse(string text, out IArrowType? type, out string? error)
    {
        var parser = new Parser(text);
        try
        {
            type = parser.ParseType();
            parser.SkipSpace();
            if (!parser.AtEnd)
            {
                throw parser.Fail("unexpected text");
            }

            error = null;
            return true;
        }
        catch (FormatException ex)
        {
            type = null;
            error = ex.Message;
            return false;
        }
    }

    /// <summary>The Arrow type of contract type <paramref name="text"/> for column <paramref name="column"/>.
    /// Throws a permanent config <see cref="PzConnectorException"/> naming the column when it does not
    /// parse.</summary>
    public static IArrowType ToArrowType(string text, string column) =>
        TryParse(text, out var type, out var error)
            ? type!
            : throw new PzConnectorException(
                $"column '{column}': columns: contract type '{text}' is not valid ({error}) -- types are {Syntax}",
                isTransient: false, code: PzConnectorErrorCodes.Config);

    /// <summary>True when <paramref name="text"/> is a nested type (list, array, struct or map).</summary>
    public static bool IsNested(string text) =>
        TryParse(text, out var type, out _) && type is NestedType;

    /// <summary>The DuckDB type name for an Arrow type this grammar produces: <c>INTEGER</c>, <c>VARCHAR[]</c>,
    /// <c>INTEGER[3]</c>, <c>STRUCT("name" VARCHAR)</c>, <c>MAP(VARCHAR, BIGINT)</c>.</summary>
    public static string ToDuckDbName(IArrowType type) => type switch
    {
        Int32Type => "INTEGER",
        Int64Type => "BIGINT",
        DoubleType => "DOUBLE",
        Decimal128Type d => $"DECIMAL({d.Precision},{d.Scale})",
        StringType => "VARCHAR",
        BooleanType => "BOOLEAN",
        Date32Type => "DATE",
        TimestampType => "TIMESTAMP",
        MapType m => $"MAP({ToDuckDbName(m.KeyField.DataType)}, {ToDuckDbName(m.ValueField.DataType)})",
        ListType l => ToDuckDbName(l.ValueDataType) + "[]",
        FixedSizeListType f => $"{ToDuckDbName(f.ValueDataType)}[{f.ListSize.ToString(CultureInfo.InvariantCulture)}]",
        StructType s => $"STRUCT({string.Join(", ", s.Fields.Select(f => $"\"{f.Name.Replace("\"", "\"\"", StringComparison.Ordinal)}\" {ToDuckDbName(f.DataType)}"))})",
        _ => throw new NotSupportedException($"no contract type maps to Arrow type '{type}'"),
    };

    /// <summary>A JSON value as the .NET value <see cref="Batches.ArrowBatchBuilder.AppendRow"/> takes for a
    /// column of Arrow <paramref name="type"/>: JSON null is null; a number for int/bigint/double/decimal; a
    /// string for varchar, for date (<c>yyyy-MM-dd</c>) and for timestamp (ISO-8601, any precision, <c>Z</c> or
    /// an offset, else UTC); true/false; an array for a list or fixed-size array; an object for a struct (a
    /// missing field is null, an undeclared one ignored) and for a map (each key read as the map's key type).
    /// A value of the wrong JSON kind throws <see cref="InvalidOperationException"/> or
    /// <see cref="FormatException"/>; callers name the column and line, never the value.</summary>
    public static object? ValueFromJson(JsonElement element, IArrowType type)
    {
        if (element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        switch (type)
        {
            case Int32Type:
                return element.GetInt32();
            case Int64Type:
                return element.GetInt64();
            case DoubleType:
                return element.GetDouble();
            case Decimal128Type:
                return Decimal(element);
            case StringType:
                return element.GetString();
            case BooleanType:
                return element.GetBoolean();
            case Date32Type:
                return DateOnly.ParseExact(element.GetString()!, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            case TimestampType:
                return Timestamp(element.GetString()!);
            case MapType map:
            {
                Expect(element, JsonValueKind.Object, "an object");
                var entries = new Dictionary<object, object?>();
                foreach (var property in element.EnumerateObject())
                {
                    entries[Key(property.Name, map.KeyField.DataType)] = ValueFromJson(property.Value, map.ValueField.DataType);
                }

                return entries;
            }

            case ListType list:
                Expect(element, JsonValueKind.Array, "an array");
                return element.EnumerateArray().Select(item => ValueFromJson(item, list.ValueDataType)).ToList();
            case FixedSizeListType list:
                Expect(element, JsonValueKind.Array, "an array");
                return element.EnumerateArray().Select(item => ValueFromJson(item, list.ValueDataType)).ToList();
            case StructType @struct:
            {
                Expect(element, JsonValueKind.Object, "an object");
                var fields = new Dictionary<string, object?>(StringComparer.Ordinal);
                foreach (var field in @struct.Fields)
                {
                    fields[field.Name] = element.TryGetProperty(field.Name, out var value)
                        ? ValueFromJson(value, field.DataType)
                        : null;
                }

                return fields;
            }

            default:
                throw new NotSupportedException($"no contract type maps to Arrow type '{type}'");
        }
    }

    /// <summary><see cref="ValueFromJson(JsonElement, IArrowType)"/> for JSON text (a csv cell holding a nested
    /// value). Malformed JSON throws <see cref="FormatException"/>.</summary>
    public static object? ValueFromJson(string json, IArrowType type)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            return ValueFromJson(document.RootElement, type);
        }
        catch (JsonException ex)
        {
            throw new FormatException("not valid JSON", ex);
        }
    }

    private static void Expect(JsonElement element, JsonValueKind kind, string what)
    {
        if (element.ValueKind != kind)
        {
            throw new InvalidOperationException($"expected {what}, got a JSON {element.ValueKind.ToString().ToLowerInvariant()}");
        }
    }

    /// <summary>A JSON number as a decimal(38,9) cell. <see cref="JsonElement.GetDecimal"/> rounds a literal
    /// with more than 28 significant digits instead of failing, so a plain (exponent-free) literal that long
    /// is parsed as a <see cref="SqlDecimal"/>, which carries all 38 digits or throws.</summary>
    private static object Decimal(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Number)
        {
            var raw = element.GetRawText();
            if (raw.AsSpan().IndexOfAny('e', 'E') < 0 && raw.Count(char.IsAsciiDigit) > 28)
            {
                return SqlDecimal.Parse(raw);
            }
        }

        return element.GetDecimal();
    }

    private static DateTimeOffset Timestamp(string text) =>
        DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var value)
            ? value
            : throw new FormatException("not a timestamp");

    /// <summary>A map key (always JSON object property text) as the map's flat key type.</summary>
    private static object Key(string text, IArrowType type) => type switch
    {
        Int32Type => int.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture),
        Int64Type => long.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture),
        DoubleType => double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture),
        Decimal128Type => decimal.Parse(text, NumberStyles.Number, CultureInfo.InvariantCulture),
        BooleanType => bool.Parse(text),
        Date32Type => DateOnly.ParseExact(text, "yyyy-MM-dd", CultureInfo.InvariantCulture),
        TimestampType => Timestamp(text),
        _ => text,
    };

    private static IArrowType? Flat(string name) => name switch
    {
        "int" => Int32Type.Default,
        "bigint" => Int64Type.Default,
        "double" => DoubleType.Default,
        "decimal" => new Decimal128Type(38, 9),
        "varchar" => StringType.Default,
        "boolean" => BooleanType.Default,
        "date" => Date32Type.Default,
        "timestamp" => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => null,
    };

    private sealed class Parser(string text)
    {
        private int _at;

        public bool AtEnd => _at >= text.Length;

        public FormatException Fail(string what) =>
            new(AtEnd ? $"{what} at the end" : $"{what} at position {_at + 1} ('{text[_at]}')");

        public void SkipSpace()
        {
            while (!AtEnd && char.IsWhiteSpace(text[_at]))
            {
                _at++;
            }
        }

        public IArrowType ParseType()
        {
            SkipSpace();
            var word = Word();
            IArrowType type = word switch
            {
                "struct" => Struct(),
                "map" => Map(),
                "" => throw Fail("expected a type"),
                _ => Flat(word) ?? throw new FormatException($"unknown type '{word}'"),
            };

            while (true)
            {
                SkipSpace();
                if (AtEnd || text[_at] != '[')
                {
                    return type;
                }

                _at++;
                SkipSpace();
                var digits = _at;
                while (!AtEnd && char.IsAsciiDigit(text[_at]))
                {
                    _at++;
                }

                var size = text[digits.._at];
                SkipSpace();
                Expect(']');
                var item = new Field("item", type, nullable: true);
                type = size.Length == 0
                    ? new ListType(item)
                    : int.TryParse(size, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n > 0
                        ? new FixedSizeListType(item, n)
                        : throw new FormatException($"array size '{size}' must be a positive whole number");
            }
        }

        private StructType Struct()
        {
            Open();
            var fields = new List<Field>();
            var names = new HashSet<string>(StringComparer.Ordinal);
            do
            {
                SkipSpace();
                var name = Name();
                if (!names.Add(name))
                {
                    throw new FormatException($"struct field '{name}' appears twice");
                }

                fields.Add(new Field(name, ParseType(), nullable: true));
                SkipSpace();
            }
            while (TryConsume(','));

            Expect(')');
            return new StructType(fields);
        }

        private MapType Map()
        {
            Open();
            var key = ParseType();
            if (key is NestedType)
            {
                throw new FormatException("a map key must be a flat type");
            }

            SkipSpace();
            Expect(',');
            var value = ParseType();
            SkipSpace();
            Expect(')');
            return new MapType(new Field("key", key, nullable: false), new Field("value", value, nullable: true));
        }

        private void Open()
        {
            SkipSpace();
            Expect('(');
        }

        private string Word()
        {
            var start = _at;
            while (!AtEnd && (char.IsAsciiLetterOrDigit(text[_at]) || text[_at] == '_'))
            {
                _at++;
            }

            return text[start.._at];
        }

        private string Name()
        {
            if (AtEnd || text[_at] != '"')
            {
                var start = _at;
                var bare = Word();
                if (bare.Length > 0 && !char.IsAsciiDigit(bare[0]))
                {
                    return bare;
                }

                _at = start;
                throw Fail("expected a field name");
            }

            var name = new StringBuilder();
            _at++;
            while (true)
            {
                if (AtEnd)
                {
                    throw Fail("unterminated quoted field name");
                }

                if (text[_at] == '"')
                {
                    if (_at + 1 < text.Length && text[_at + 1] == '"')
                    {
                        name.Append('"');
                        _at += 2;
                        continue;
                    }

                    _at++;
                    return name.Length > 0 ? name.ToString() : throw Fail("empty field name");
                }

                name.Append(text[_at++]);
            }
        }

        private bool TryConsume(char c)
        {
            if (!AtEnd && text[_at] == c)
            {
                _at++;
                return true;
            }

            return false;
        }

        private void Expect(char c)
        {
            if (!TryConsume(c))
            {
                throw Fail($"expected '{c}'");
            }
        }
    }
}
