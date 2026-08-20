using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connectors.Abstractions.Tests.Formats;

/// <summary>Offline determinism tests for <see cref="NdjsonCodec.ReadAsync"/>: projected/typed rows with
/// unknown-key-ignored and missing-declared-key-null, top-level-array rejection, batching at
/// <see cref="BatchOptions.MaxRowsPerBatch"/>, and a <see cref="NdjsonCodec.WriteAsync"/> round-trip over
/// the full eight-type "columns:" contract matrix.</summary>
public class NdjsonCodecReadTests
{
    private static Stream ToStream(string s) => new MemoryStream(Encoding.UTF8.GetBytes(s));

    private static async Task<CollectedRows> Collect(IAsyncEnumerable<RecordBatch> batches)
    {
        var list = new List<RecordBatch>();
        await foreach (var batch in batches)
        {
            list.Add(batch);
        }

        return new CollectedRows(list);
    }

    [Fact]
    public async Task Reads_projected_typed_rows_ignoring_unknown_keys()
    {
        var ndjson = "{\"id\":1,\"level\":\"info\",\"extra\":9}\n{\"id\":2}\n";  // 2nd row missing level
        var contract = new Dictionary<string, string> { ["id"] = "bigint", ["level"] = "varchar" };
        var rows = await Collect(NdjsonCodec.ReadAsync(ToStream(ndjson), contract, null, BatchOptions.Default, default));
        Assert.Equal(2, rows.RowCount);
        Assert.Equal(1L, rows.GetInt64("id", 0));
        Assert.Equal("info", rows.GetString("level", 0));
        Assert.True(rows.IsNull("level", 1));   // missing declared key -> null
    }

    [Fact]
    public async Task Top_level_array_is_permanent_error()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Collect(NdjsonCodec.ReadAsync(ToStream("  [1,2,3]"), new Dictionary<string, string> { ["id"] = "bigint" }, null, BatchOptions.Default, default)));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Explicit_json_null_is_arrow_null()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint", ["level"] = "varchar" };
        var rows = await Collect(NdjsonCodec.ReadAsync(
            ToStream("{\"id\":1,\"level\":null}\n"), contract, null, BatchOptions.Default, default));
        Assert.Equal(1, rows.RowCount);
        Assert.True(rows.IsNull("level", 0));
    }

    [Fact]
    public async Task Projection_selects_a_subset_of_declared_columns_in_projection_order()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint", ["level"] = "varchar", ["extra"] = "int" };
        var ndjson = "{\"id\":1,\"level\":\"info\",\"extra\":9}\n";
        var rows = await Collect(NdjsonCodec.ReadAsync(
            ToStream(ndjson), contract, ["level", "id"], BatchOptions.Default, default));
        Assert.Equal(2, rows.ColumnCount);
        Assert.Equal("info", rows.GetString("level", 0));
        Assert.Equal(1L, rows.GetInt64("id", 0));
    }

    [Fact]
    public async Task Rows_spanning_max_rows_per_batch_emit_multiple_batches()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint" };
        var sb = new StringBuilder();
        for (var i = 0; i < 5; i++)
        {
            sb.Append("{\"id\":").Append(i).Append("}\n");
        }

        var options = new BatchOptions(TargetBatchBytes: 32 * 1024 * 1024, MaxRowsPerBatch: 2);
        var rows = await Collect(NdjsonCodec.ReadAsync(ToStream(sb.ToString()), contract, null, options, default));

        Assert.Equal(5, rows.RowCount);
        Assert.Equal(3, rows.BatchCount);          // 2 + 2 + 1
        Assert.Equal([2, 2, 1], rows.BatchLengths);
        for (var i = 0; i < 5; i++)
        {
            Assert.Equal((long)i, rows.GetInt64("id", i));
        }
    }

    [Fact]
    public async Task Malformed_json_line_is_permanent_error()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint" };
        var ndjson = "{\"id\":1}\n{not json at all\n";
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Collect(NdjsonCodec.ReadAsync(ToStream(ndjson), contract, null, BatchOptions.Default, default)));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Type_mismatched_value_is_permanent_error()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Collect(NdjsonCodec.ReadAsync(ToStream("{\"id\":\"abc\"}\n"), contract, null, BatchOptions.Default, default)));
        Assert.False(ex.IsTransient);
    }

    [Theory]
    [InlineData("2026-07-13T10:00:00Z", 0)]                    // no fractional seconds
    [InlineData("2026-07-13T10:00:00.123Z", 123000)]            // 3-digit fractional
    [InlineData("2026-07-13T10:00:00.123456Z", 123456)]         // 6-digit fractional (matches WriteAsync's own output)
    [InlineData("2026-07-13T10:00:00+00:00", 0)]                // numeric UTC offset instead of 'Z'
    public async Task Reads_loosened_iso8601_timestamp_variants(string wire, int microsecondFraction)
    {
        var contract = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var ndjson = $"{{\"ts\":\"{wire}\"}}\n";
        var rows = await Collect(NdjsonCodec.ReadAsync(ToStream(ndjson), contract, null, BatchOptions.Default, default));

        Assert.Equal(1, rows.RowCount);
        var expected = new DateTimeOffset(2026, 7, 13, 10, 0, 0, TimeSpan.Zero).AddTicks(microsecondFraction * 10);
        Assert.Equal(expected, rows.GetTimestamp("ts", 0));
    }

    [Fact]
    public async Task Invalid_timestamp_error_is_pii_safe()
    {
        var contract = new Dictionary<string, string> { ["ts"] = "timestamp" };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Collect(NdjsonCodec.ReadAsync(ToStream("{\"ts\":\"not-a-time\"}\n"), contract, null, BatchOptions.Default, default)));

        Assert.False(ex.IsTransient);
        Assert.DoesNotContain("not-a-time", ex.Message);
        Assert.Contains("'ts'", ex.Message);
    }

    [Fact]
    public async Task Whitespace_only_line_is_skipped()
    {
        var contract = new Dictionary<string, string> { ["id"] = "bigint" };
        var ndjson = "{\"id\":1}\n   \n{\"id\":2}\n";
        var rows = await Collect(NdjsonCodec.ReadAsync(ToStream(ndjson), contract, null, BatchOptions.Default, default));
        Assert.Equal(2, rows.RowCount);
        Assert.Equal(1L, rows.GetInt64("id", 0));
        Assert.Equal(2L, rows.GetInt64("id", 1));
    }

    [Fact]
    public async Task Empty_stream_yields_no_batches()
    {
        var rows = await Collect(NdjsonCodec.ReadAsync(
            ToStream(""), new Dictionary<string, string> { ["id"] = "bigint" }, null, BatchOptions.Default, default));
        Assert.Equal(0, rows.RowCount);
        Assert.Equal(0, rows.BatchCount);
    }

    [Fact]
    public async Task Round_trips_every_contract_type_through_write_then_read()
    {
        var schema = new Schema(
        [
            new Field("n", Int32Type.Default, nullable: true),
            new Field("big", Int64Type.Default, nullable: true),
            new Field("amt", new Decimal128Type(38, 9), nullable: true),
            new Field("price", DoubleType.Default, nullable: true),
            new Field("active", BooleanType.Default, nullable: true),
            new Field("day", Date32Type.Default, nullable: true),
            new Field("created", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true),
            new Field("name", StringType.Default, nullable: true),
        ], null);

        var n = new Int32Array.Builder();
        var big = new Int64Array.Builder();
        var amt = new Decimal128Array.Builder(new Decimal128Type(38, 9));
        var price = new DoubleArray.Builder();
        var active = new BooleanArray.Builder();
        var day = new Date32Array.Builder();
        var created = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var name = new StringArray.Builder();

        n.Append(7);
        big.Append(123456789012345L);
        amt.Append(12345.678901234m);
        price.Append(3.14);
        active.Append(true);
        day.Append(new DateOnly(2026, 7, 13));
        created.Append(new DateTimeOffset(2026, 7, 13, 10, 30, 15, TimeSpan.Zero));
        name.Append("widget");

        using var written = new RecordBatch(schema,
        [
            n.Build(), big.Build(), amt.Build(), price.Build(),
            active.Build(), day.Build(), created.Build(), name.Build(),
        ], 1);

        using var ms = new MemoryStream();
        await NdjsonCodec.WriteAsync(written, ms, default);
        ms.Position = 0;

        var contract = new Dictionary<string, string>
        {
            ["n"] = "int",
            ["big"] = "bigint",
            ["amt"] = "decimal",
            ["price"] = "double",
            ["active"] = "boolean",
            ["day"] = "date",
            ["created"] = "timestamp",
            ["name"] = "varchar",
        };

        var rows = await Collect(NdjsonCodec.ReadAsync(ms, contract, null, BatchOptions.Default, default));

        Assert.Equal(1, rows.RowCount);
        Assert.Equal(7, rows.GetInt32("n", 0));
        Assert.Equal(123456789012345L, rows.GetInt64("big", 0));
        Assert.Equal(12345.678901234m, rows.GetDecimal("amt", 0));
        Assert.Equal(3.14, rows.GetDouble("price", 0));
        Assert.True(rows.GetBool("active", 0));
        Assert.Equal(new DateOnly(2026, 7, 13), rows.GetDate("day", 0));
        Assert.Equal(new DateTimeOffset(2026, 7, 13, 10, 30, 15, TimeSpan.Zero), rows.GetTimestamp("created", 0));
        Assert.Equal("widget", rows.GetString("name", 0));
    }

    /// <summary>Flattens the batches an <c>await foreach</c> collected into simple by-name, by-row-index
    /// accessors so tests read declaratively instead of re-deriving Arrow column/row arithmetic each time.</summary>
    private sealed class CollectedRows(IReadOnlyList<RecordBatch> batches)
    {
        public int BatchCount => batches.Count;

        public int RowCount => batches.Sum(b => b.Length);

        public int ColumnCount => batches.Count > 0 ? batches[0].ColumnCount : 0;

        public IReadOnlyList<int> BatchLengths => batches.Select(b => b.Length).ToList();

        public bool IsNull(string column, int row) => Locate(column, row, out var array, out var i) && array.IsNull(i);

        public long GetInt64(string column, int row) => ((Int64Array)Array(column, row, out var i)).GetValue(i)!.Value;

        public int GetInt32(string column, int row) => ((Int32Array)Array(column, row, out var i)).GetValue(i)!.Value;

        public double GetDouble(string column, int row) => ((DoubleArray)Array(column, row, out var i)).GetValue(i)!.Value;

        public decimal GetDecimal(string column, int row) => ((Decimal128Array)Array(column, row, out var i)).GetValue(i)!.Value;

        public bool GetBool(string column, int row) => ((BooleanArray)Array(column, row, out var i)).GetValue(i)!.Value;

        public string? GetString(string column, int row) => ((StringArray)Array(column, row, out var i)).GetString(i);

        public DateOnly GetDate(string column, int row) =>
            DateOnly.FromDateTime(((Date32Array)Array(column, row, out var i)).GetDateTime(i)!.Value);

        public DateTimeOffset GetTimestamp(string column, int row) => ((TimestampArray)Array(column, row, out var i)).GetTimestamp(i)!.Value;

        private IArrowArray Array(string column, int row, out int indexInBatch) =>
            Locate(column, row, out var array, out indexInBatch)
                ? array
                : throw new IndexOutOfRangeException($"row {row} out of range ({RowCount} total)");

        private bool Locate(string column, int row, out IArrowArray array, out int indexInBatch)
        {
            foreach (var batch in batches)
            {
                if (row < batch.Length)
                {
                    array = batch.Column(batch.Schema.GetFieldIndex(column));
                    indexInBatch = row;
                    return true;
                }

                row -= batch.Length;
            }

            array = null!;
            indexInBatch = -1;
            return false;
        }
    }
}
