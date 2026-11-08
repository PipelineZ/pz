using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Http.Tests;

public class TextValueParserTests
{
    private static readonly string[] Types =
        ["int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp"];

    public static TheoryData<string, string> Cases()
    {
        var data = new TheoryData<string, string>
        {
            { "1,000.5", "double" },
            { "1e3", "double" },
            { "true", "boolean" },
            { "True", "boolean" },
            { "false", "boolean" },
            { "yes", "boolean" },
            { "2026-11-07T10:00:00+02:00", "timestamp" },
            { "2026-11-07 10:00:00", "timestamp" },
            { "2026-11-07", "date" },
            { "1,234.5", "decimal" },
            { "42", "int" },
            { " 42 ", "int" },
            { "-9000000000", "bigint" },
            { "x", "int" },
            { "1.5", "int" },
            { "naïve", "varchar" },
        };
        foreach (var type in Types)
        {
            data.Add("", type);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Matches_csv_column_writer(string text, string typeName)
    {
        var (expected, writerError) = ViaCsvColumnWriter(text, typeName);

        if (writerError is not null)
        {
            var ex = Assert.Throws<PzConnectorException>(() => TextValueParser.Parse(text, typeName, "ctx"));
            Assert.False(ex.IsTransient);
            Assert.Equal($"ctx: value '{text}' is not a valid {typeName}", ex.Message);
        }
        else
        {
            Assert.Equal(expected, TextValueParser.Parse(text, typeName, "ctx"));
        }
    }

    [Fact]
    public void Known_values()
    {
        Assert.Equal(1000.5d, TextValueParser.Parse("1,000.5", "double", "ctx"));
        Assert.Equal(true, TextValueParser.Parse("true", "boolean", "ctx"));
        Assert.Equal(true, TextValueParser.Parse("True", "boolean", "ctx"));
        var ts = Assert.IsType<DateTimeOffset>(TextValueParser.Parse("2026-11-07T10:00:00+02:00", "timestamp", "ctx"));
        Assert.Equal(new DateTimeOffset(2026, 11, 7, 8, 0, 0, TimeSpan.Zero), ts);
        Assert.Equal(TimeSpan.Zero, ts.Offset);
        Assert.Equal(new DateOnly(2026, 11, 7), TextValueParser.Parse("2026-11-07", "date", "ctx"));
        Assert.Equal(12.5m, TextValueParser.Parse("12.5", "decimal", "ctx"));
        Assert.Equal(42, TextValueParser.Parse("42", "int", "ctx"));
        Assert.Equal(42L, TextValueParser.Parse("42", "bigint", "ctx"));
        Assert.Equal("a", TextValueParser.Parse("a", "varchar", "ctx"));
    }

    [Fact]
    public void Empty_is_null_for_every_type()
    {
        foreach (var type in Types)
        {
            Assert.Null(TextValueParser.Parse("", type, "ctx"));
        }
    }

    [Fact]
    public void Invalid_int_is_permanent()
    {
        var ex = Assert.Throws<PzConnectorException>(() => TextValueParser.Parse("x", "int", "s.d: file 'a.csv'"));
        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'a.csv': value 'x' is not a valid int", ex.Message);
    }

    /// <summary>What the CSV contract path stores for one cell: the writer's array read back as the CLR
    /// value <see cref="ArrowBatchBuilder"/> would have been given.</summary>
    private static (object? Value, PzConnectorException? Error) ViaCsvColumnWriter(string text, string typeName)
    {
        var field = ContractProjector.BuildSchema(new Dictionary<string, string> { ["c"] = typeName }).FieldsList[0];
        var writer = CsvColumnWriter.Create(field, typeName, "f.csv", new PooledNativeAllocator());
        try
        {
            writer.Append(text.AsSpan(), 1);
        }
        catch (PzConnectorException ex)
        {
            return (null, ex);
        }

        using var array = (IDisposable)writer.BuildAndReset();
        object? value = array switch
        {
            Int32Array a => a.GetValue(0),
            Int64Array a => a.GetValue(0),
            DoubleArray a => a.GetValue(0),
            Decimal128Array a => a.GetValue(0),
            BooleanArray a => a.GetValue(0),
            StringArray a => a.GetString(0),
            Date32Array a => a.GetDateOnly(0),
            TimestampArray a => a.GetTimestamp(0),
            _ => throw new InvalidOperationException(array.GetType().Name),
        };
        return (value, null);
    }
}
