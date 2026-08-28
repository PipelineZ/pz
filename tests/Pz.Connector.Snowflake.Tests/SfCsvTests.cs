using Apache.Arrow;
using Apache.Arrow.Types;

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
}
