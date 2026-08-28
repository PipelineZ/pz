using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeArrowReaderTests
{
    // FakeDbDataReader columns: (name, dataTypeName, clrType) — Snowflake.Data reports logical
    // names (FIXED/TEXT/REAL) and hands FIXED scale-0 back as long.
    private static readonly (string, string, Type)[] AllKinds =
    [
        ("c_int", "FIXED", typeof(long)), ("c_big", "FIXED", typeof(long)),
        ("c_dbl", "REAL", typeof(double)), ("c_dec", "FIXED", typeof(decimal)),
        ("c_str", "TEXT", typeof(string)), ("c_bool", "BOOLEAN", typeof(bool)),
        ("c_date", "DATE", typeof(DateTime)), ("c_ts", "TIMESTAMP_NTZ", typeof(DateTime)),
    ];

    private static object?[] Row() =>
    [
        42L, 9_000_000_000L, 1.5d, 12345.12m, "hello", true,
        new DateTime(2026, 3, 27), new DateTime(2026, 3, 27, 10, 30, 0),
    ];

    [Fact]
    public async Task Reads_every_kind_plus_null_row_with_exact_schema()
    {
        var reader = new FakeDbDataReader(AllKinds, [Row(), new object?[8]]);
        var batches = new List<RecordBatch>();
        await foreach (var b in SnowflakeArrowReader.ReadBatchesAsync(reader, BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal(2, batch.Length);
        Assert.Equal(8, batch.Schema.FieldsList.Count);
        Assert.IsType<StringArray>(batch.Column("c_str"));
        Assert.True(batch.Column("c_ts").IsNull(1));
        batch.Dispose();
    }

    [Fact]
    public void Unmapped_type_names_the_column_with_a_hint()
    {
        var reader = new FakeDbDataReader([("payload", "VARIANT", typeof(string))], []);
        var ex = Assert.Throws<PzConnectorException>(() => SnowflakeArrowReader.BuildSchema(reader, "dataset 'x'"));
        Assert.False(ex.IsTransient);
        Assert.Contains("payload", ex.Message);
        Assert.Contains("cast", ex.Message);
    }

    [Fact]
    public async Task MaxRowsPerBatch_splits_batches()
    {
        var rows = Enumerable.Range(0, 5).Select(i => new object?[] { (long)i }).ToArray();
        var reader = new FakeDbDataReader([("id", "FIXED", typeof(long))], rows);
        var count = 0;
        await foreach (var b in SnowflakeArrowReader.ReadBatchesAsync(
            reader, new BatchOptions(MaxRowsPerBatch: 2), CancellationToken.None))
        {
            count++;
            b.Dispose();
        }

        Assert.Equal(3, count);
    }
}
