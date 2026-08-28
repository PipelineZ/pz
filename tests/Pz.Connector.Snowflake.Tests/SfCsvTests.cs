using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake.Tests;

public class SfCsvTests
{
    private static RecordBatch OneRow()
    {
        var schema = new Schema([
            new Field("i", Int64Type.Default, nullable: true),
            new Field("s", StringType.Default, nullable: true),
            new Field("b", BooleanType.Default, nullable: true),
            new Field("d", Date32Type.Default, nullable: true),
            new Field("t", new TimestampType(TimeUnit.Microsecond, (string?)null), nullable: true)], null);
        return new RecordBatch(schema, [
            new Int64Array.Builder().Append(7).AppendNull().Build(),
            new StringArray.Builder().Append("he\"llo").AppendNull().Build(),
            new BooleanArray.Builder().Append(true).AppendNull().Build(),
            new Date32Array.Builder().Append(new DateOnly(2026, 3, 27)).AppendNull().Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, (string?)null))
                .Append(new DateTimeOffset(2026, 3, 27, 10, 30, 0, 123, TimeSpan.Zero)).AppendNull().Build(),
        ], 2);
    }

    [Fact]
    public void Encodes_values_nulls_quoting_and_formats()
    {
        using var batch = OneRow();
        var sw = new StringWriter();
        SfCsv.WriteBatch(batch, sw);
        var lines = sw.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal("7,\"he\"\"llo\",TRUE,2026-03-27,2026-03-27 10:30:00.123000", lines[0]);
        Assert.Equal("\\N,\\N,\\N,\\N,\\N", lines[1]);
    }

    [Fact]
    public void Lines_end_with_lf_only()
    {
        using var batch = OneRow();
        var sw = new StringWriter();
        SfCsv.WriteBatch(batch, sw);
        Assert.DoesNotContain("\r", sw.ToString());
    }

    [Fact]
    public void FileFormatClause_is_pinned()
    {
        Assert.Equal(
            "file_format = (type = csv field_optionally_enclosed_by = '\"' null_if = ('\\\\N') escape_unenclosed_field = none)",
            SfCsv.FileFormatClause);
    }

    [Fact]
    public void String_values_containing_backslash_or_literal_null_token_round_trip_as_quoted_data()
    {
        var schema = new Schema([new Field("s", StringType.Default, nullable: true)], null);
        using var batch = new RecordBatch(schema, [
            new StringArray.Builder().Append("C:\\temp\\file").Append("\\N").Build(),
        ], 2);
        var sw = new StringWriter();
        SfCsv.WriteBatch(batch, sw);
        var lines = sw.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);

        // Always quoted, whatever the content: a backslash is not an escape character in this format
        // (only the enclosing `"` is, doubled), and the literal two-character string "\N" as DATA is
        // unambiguous only because it is quoted -- the unquoted null marker never is.
        Assert.Equal("\"C:\\temp\\file\"", lines[0]);
        Assert.Equal("\"\\N\"", lines[1]);
    }

    [Fact]
    public void Merge_sequence_column_appends_session_monotonic_counter()
    {
        using var batch = OneRow();
        var sw = new StringWriter();
        var next = SfCsv.WriteBatch(batch, sw, sequenceStart: 5);
        var lines = sw.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal("7,\"he\"\"llo\",TRUE,2026-03-27,2026-03-27 10:30:00.123000,5", lines[0]);
        Assert.Equal("\\N,\\N,\\N,\\N,\\N,6", lines[1]);
        Assert.Equal(7, next);
    }

    // double.ToString(InvariantCulture) spells these "NaN"/"Infinity"/"-Infinity", none of which
    // Snowflake's CSV COPY parses for a FLOAT column -- it wants "nan"/"inf"/"-inf". With
    // on_error = abort_statement (SfDdl.BuildCopyIntoStagingSql), an un-parseable field is a hard
    // commit failure, not a dropped row.
    [Theory]
    [InlineData(double.NaN, "nan")]
    [InlineData(double.PositiveInfinity, "inf")]
    [InlineData(double.NegativeInfinity, "-inf")]
    [InlineData(3.14, "3.14")]
    public void Double_special_values_render_as_snowflake_understands_them(double value, string expected)
    {
        var schema = new Schema([new Field("d", DoubleType.Default, nullable: true)], null);
        using var batch = new RecordBatch(schema, [new DoubleArray.Builder().Append(value).Build()], 1);
        var sw = new StringWriter();
        SfCsv.WriteBatch(batch, sw);
        Assert.Equal(expected, sw.ToString().TrimEnd('\n'));
    }

    [Fact]
    public void Decimal128_value_outside_clr_decimal_range_is_a_named_write_error()
    {
        // Decimal128(38, 0) can hold a 38-digit integer; System.Decimal's range tops out around
        // 7.9e28 (~29 digits) -- Decimal128Array.GetValue throws a raw OverflowException widening a
        // value at the top of the v0 matrix's precision, which the sink must catch and name.
        var type = new Decimal128Type(38, 0);
        var schema = new Schema([new Field("amount", type, nullable: true)], null);
        using var batch = new RecordBatch(schema, [
            new Decimal128Array.Builder(type).Append("99999999999999999999999999999999999999").Build(),
        ], 1);
        var sw = new StringWriter();
        var ex = Assert.Throws<PzConnectorException>(() => SfCsv.WriteBatch(batch, sw));
        Assert.False(ex.IsTransient);
        Assert.Contains("amount", ex.Message);
    }
}
