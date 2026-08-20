using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

public class ArrowBatchDataReaderTests
{
    private static RecordBatch BuildBatch()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("name", StringType.Default, nullable: true),
            new Field("d", Date32Type.Default, nullable: true),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        ], null);
        var id = new Int64Array.Builder().Append(1).AppendNull().Build();
        var name = new StringArray.Builder().Append("a").AppendNull().Build();
        var d = new Date32Array.Builder().Append(new DateOnly(2026, 1, 2)).AppendNull().Build();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "+00:00"))
            .Append(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero)).AppendNull().Build();
        return new RecordBatch(schema, [id, name, d, ts], 2);
    }

    [Fact]
    public void Reads_values_then_dbnulls_then_ends()
    {
        using var batch = BuildBatch();
        using var reader = new ArrowBatchDataReader(batch);
        Assert.Equal(4, reader.FieldCount);

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetValue(0));
        Assert.Equal("a", reader.GetValue(1));
        Assert.Equal(new DateTime(2026, 1, 2), reader.GetValue(2));            // date -> DateTime for TDS
        Assert.Equal(new DateTime(2026, 1, 2, 3, 4, 5), reader.GetValue(3));   // ts -> naive UTC DateTime

        Assert.True(reader.Read());
        for (var i = 0; i < 4; i++)
        {
            Assert.True(reader.IsDBNull(i));
            Assert.Equal(DBNull.Value, reader.GetValue(i));
        }

        Assert.False(reader.Read());
    }

    [Fact]
    public void GetOrdinal_and_names_follow_the_schema()
    {
        using var batch = BuildBatch();
        using var reader = new ArrowBatchDataReader(batch);
        Assert.Equal("name", reader.GetName(1));
        Assert.Equal(1, reader.GetOrdinal("name"));
    }

    [Fact]
    public void GetFieldType_is_schema_derived_and_null_independent()
    {
        using var batch = BuildBatch();
        using var reader = new ArrowBatchDataReader(batch);
        Assert.Equal(typeof(long), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));
        Assert.Equal(typeof(DateTime), reader.GetFieldType(2));
        Assert.Equal(typeof(DateTime), reader.GetFieldType(3));
        Assert.True(reader.Read());
        Assert.True(reader.Read()); // now on the all-null row
        Assert.Equal(typeof(long), reader.GetFieldType(0)); // not DBNull
    }

    [Fact]
    public void GetTextReader_streams_string_cells()
    {
        using var batch = BuildBatch();
        using var reader = new ArrowBatchDataReader(batch);
        Assert.True(reader.Read());
        using var text = reader.GetTextReader(1);
        Assert.Equal("a", text.ReadToEnd());
    }

    [Fact]
    public void Int32_double_bool_decimal_columns_convert_directly()
    {
        var schema = new Schema(
        [
            new Field("i", Int32Type.Default, nullable: true),
            new Field("d", DoubleType.Default, nullable: true),
            new Field("b", BooleanType.Default, nullable: true),
            new Field("m", new Decimal128Type(38, 9), nullable: true),
        ], null);
        var i = new Int32Array.Builder().Append(7).Build();
        var d = new DoubleArray.Builder().Append(1.5).Build();
        var b = new BooleanArray.Builder().Append(true).Build();
        var m = new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(12.5m).Build();
        using var batch = new RecordBatch(schema, [i, d, b, m], 1);
        using var reader = new ArrowBatchDataReader(batch);
        Assert.True(reader.Read());
        Assert.Equal(7, reader.GetValue(0));
        Assert.Equal(1.5, reader.GetValue(1));
        Assert.Equal(true, reader.GetValue(2));
        Assert.Equal(12.5m, reader.GetValue(3));
    }

    // The source-side analog (SqlServerArrowReaderTests.Decimal_scale_overflow_names_the_column) covers a
    // decimal that doesn't survive being written INTO an Arrow array; this covers the write path's own
    // catch (ReadDecimal) -- a value that DOES fit decimal128(38,9) but not .NET's 96-bit decimal, which
    // only GetValue's conversion (called from Cell, the bulk-write path) can surface.
    [Fact]
    public void Decimal_value_too_large_for_net_decimal_names_the_column_during_bulk_write()
    {
        var schema = new Schema(
        [
            new Field("amount", new Decimal128Type(38, 9), nullable: true),
        ], null);
        // 29-digit integer part -- decimal128(38,9) represents it fine, but it exceeds
        // decimal.MaxValue's ~7.9228e28. Decimal128Array.Builder.Append(string) parses the digits
        // directly into the 128-bit mantissa, bypassing .NET decimal parsing at append time -- the
        // overflow only surfaces later, when GetValue converts to decimal.
        var amount = new Decimal128Array.Builder(new Decimal128Type(38, 9))
            .Append("99999999999999999999999999999.000000000")
            .Build();
        using var batch = new RecordBatch(schema, [amount], 1);
        using var reader = new ArrowBatchDataReader(batch);
        Assert.True(reader.Read());

        var ex = Assert.Throws<PzConnectorException>(() => reader.GetValue(0));
        Assert.False(ex.IsTransient);
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
        Assert.Contains("exceeds .NET decimal range", ex.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(ex.InnerException);
    }
}
