using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

public class SqlServerArrowReaderTests
{
    private static readonly (string, string, Type)[] AllKindsColumns =
    [
        ("c_int", "int", typeof(int)), ("c_tiny", "tinyint", typeof(byte)),
        ("c_small", "smallint", typeof(short)), ("c_big", "bigint", typeof(long)),
        ("c_float", "float", typeof(double)), ("c_real", "real", typeof(float)),
        ("c_dec", "decimal", typeof(decimal)), ("c_str", "nvarchar", typeof(string)),
        ("c_guid", "uniqueidentifier", typeof(Guid)), ("c_bit", "bit", typeof(bool)),
        ("c_date", "date", typeof(DateTime)), ("c_dt2", "datetime2", typeof(DateTime)),
        ("c_dto", "datetimeoffset", typeof(DateTimeOffset)),
    ];

    private static readonly Guid G = Guid.Parse("11111111-2222-3333-4444-555555555555");

    private static object?[] Row() =>
    [
        42, (byte)7, (short)-3, 9_000_000_000L, 1.5d, 2.5f, 12345.123456789m, "hello",
        G, true, new DateOnly(2026, 3, 27),
        new DateTime(2026, 3, 27, 10, 30, 0, DateTimeKind.Unspecified),
        new DateTimeOffset(2026, 3, 27, 12, 30, 0, TimeSpan.FromHours(2)),
    ];

    [Fact]
    public async Task Reads_every_kind_with_widening_null_row_and_exact_schema()
    {
        var reader = new FakeDbDataReader(AllKindsColumns, [Row(), new object?[13]]);
        var schema = SqlServerArrowReader.BuildSchema(reader, "ds");
        var batches = new List<RecordBatch>();
        await foreach (var b in SqlServerArrowReader.ReadBatchesAsync(reader, BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal(2, batch.Length);
        Assert.True(schema.FieldsList.Select(f => f.Name).SequenceEqual(
            batch.Schema.FieldsList.Select(f => f.Name)));

        Assert.Equal(42, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(7, ((Int32Array)batch.Column(1)).GetValue(0));          // tinyint widened
        Assert.Equal(-3, ((Int32Array)batch.Column(2)).GetValue(0));         // smallint widened
        Assert.Equal(9_000_000_000L, ((Int64Array)batch.Column(3)).GetValue(0));
        Assert.Equal(1.5d, ((DoubleArray)batch.Column(4)).GetValue(0));
        Assert.Equal(2.5d, ((DoubleArray)batch.Column(5)).GetValue(0));      // real widened
        Assert.Equal(12345.123456789m, ((Decimal128Array)batch.Column(6)).GetValue(0));
        Assert.Equal("hello", ((StringArray)batch.Column(7)).GetString(0));
        Assert.Equal(G.ToString("D"), ((StringArray)batch.Column(8)).GetString(0));
        Assert.True(((BooleanArray)batch.Column(9)).GetValue(0));
        Assert.Equal(new DateOnly(2026, 3, 27), ((Date32Array)batch.Column(10)).GetDateOnly(0));
        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero),
            ((TimestampArray)batch.Column(11)).GetTimestamp(0));             // trusted-UTC
        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero),
            ((TimestampArray)batch.Column(12)).GetTimestamp(0));             // dto normalized to UTC

        for (var i = 0; i < 13; i++)
        {
            Assert.True(batch.Column(i).IsNull(1), $"column {i} row 1 should be NULL");
        }
    }

    [Fact]
    public async Task Emits_multiple_batches_under_a_tiny_byte_target()
    {
        var rows = Enumerable.Range(0, 500).Select(i => new object?[] { i, $"row-{i}" }).ToArray();
        var reader = new FakeDbDataReader([("id", "int", typeof(int)), ("name", "nvarchar", typeof(string))], rows);
        var count = 0;
        var total = 0;
        await foreach (var b in SqlServerArrowReader.ReadBatchesAsync(reader, new BatchOptions(TargetBatchBytes: 1024), CancellationToken.None))
        {
            count++;
            total += b.Length;
        }

        Assert.True(count >= 2, $"expected >= 2 batches, got {count}");
        Assert.Equal(500, total);
    }

    [Fact]
    public void Unsupported_column_fails_fast_naming_column_and_fix()
    {
        var reader = new FakeDbDataReader([("payload", "varbinary", typeof(byte[]))], []);
        var ex = Assert.Throws<PzConnectorException>(() => SqlServerArrowReader.BuildSchema(reader, "ds"));
        Assert.False(ex.IsTransient);
        Assert.Contains("payload", ex.Message, StringComparison.Ordinal);
        Assert.Contains("varbinary", ex.Message, StringComparison.Ordinal);
        Assert.Contains("query:", ex.Message, StringComparison.Ordinal); // actionable hint
    }

    [Fact]
    public async Task Decimal_scale_overflow_names_the_column()
    {
        var reader = new FakeDbDataReader([("amount", "decimal", typeof(decimal))],
            [[0.1234567891234m]]); // scale 13 > 9
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var _ in SqlServerArrowReader.ReadBatchesAsync(reader, BatchOptions.Default, CancellationToken.None)) { }
        });
        Assert.Contains("amount", ex.Message, StringComparison.Ordinal);
    }
}
