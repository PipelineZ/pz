using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

public class ArrowBatchBuilderTests
{
    private static Schema WideSchema() => new([
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: true),
        new Field("amount", DoubleType.Default, nullable: true),
        new Field("flag", BooleanType.Default, nullable: true),
        new Field("day", Date32Type.Default, nullable: true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        new Field("price", new Decimal128Type(38, 9), nullable: true),
        new Field("count", Int32Type.Default, nullable: true),
    ], null);

    private static object?[] Row(long i) =>
    [
        i, $"row-{i}", i * 1.5, i % 2 == 0,
        new DateOnly(2026, 1, 1).AddDays((int)(i % 365)),
        new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddSeconds(i),
        (decimal)i + 0.123456789m, (int)i,
    ];

    [Fact]
    public void Builder_emits_batch_at_target_bytes()
    {
        var builder = new ArrowBatchBuilder(WideSchema(), targetBatchBytes: 4096);
        var taken = 0;
        for (long i = 0; i < 1000; i++)
        {
            builder.AppendRow(Row(i));
            if (builder.TryTakeBatch(out var batch))
            {
                taken++;
                Assert.NotNull(batch);
                Assert.True(batch!.Length > 0);
                batch.Dispose();
            }
        }
        Assert.True(taken >= 2, $"expected multiple batches at a 4KB target, got {taken}");
    }

    [Fact]
    public void Builder_flush_emits_remainder_and_then_null()
    {
        var builder = new ArrowBatchBuilder(WideSchema());
        builder.AppendRow(Row(1));
        builder.AppendRow(Row(2));
        Assert.False(builder.TryTakeBatch(out _));
        var remainder = builder.Flush();
        Assert.NotNull(remainder);
        Assert.Equal(2, remainder!.Length);
        remainder.Dispose();
        Assert.Null(builder.Flush());
    }

    [Fact]
    public void Builder_roundtrips_values_and_nulls()
    {
        var builder = new ArrowBatchBuilder(WideSchema());
        builder.AppendRow(Row(7));
        builder.AppendRow([8L, null, null, null, null, null, null, null]);
        var batch = builder.Flush()!;
        Assert.Equal(2, batch.Length);
        var ids = (Int64Array)batch.Column(0);
        var names = (StringArray)batch.Column(1);
        Assert.Equal(7L, ids.GetValue(0));
        Assert.Equal(8L, ids.GetValue(1));
        Assert.Equal("row-7", names.GetString(0));
        Assert.True(names.IsNull(1));
        batch.Dispose();
    }

    [Fact]
    public void Unsupported_schema_type_fails_fast()
    {
        var schema = new Schema([new Field("blob", BinaryType.Default, nullable: true)], null);
        Assert.Throws<NotSupportedException>(() => new ArrowBatchBuilder(schema));
    }

    [Fact]
    public void Wrong_value_count_throws()
    {
        var builder = new ArrowBatchBuilder(WideSchema());
        Assert.Throws<ArgumentException>(() => builder.AppendRow([1L, "only-two"]));
    }

    /// <summary><see cref="ArrowBatchBuilder.AppendFrom"/> is the
    /// typed, non-boxing bulk-copy analogue of <see cref="ArrowBatchBuilder.AppendRow"/> — reading each
    /// cell directly out of an already-built Arrow batch's columns instead of a boxed
    /// <c>object?[]</c> row. Builds a source batch via the row-oriented API (standing in for a
    /// DuckDB-native Arrow batch), re-batches it into a fresh builder via <c>AppendFrom</c>, and checks
    /// the result against the same values <c>AppendRow</c> would have produced, across the whole v0 type
    /// matrix plus an all-null row: a null cell must go through <c>AppendNull()</c> rather than being
    /// dropped, or the columns' row counts desync.</summary>
    [Fact]
    public void AppendFrom_matches_AppendRow_including_nulls()
    {
        var sourceBuilder = new ArrowBatchBuilder(WideSchema());
        sourceBuilder.AppendRow(Row(3));
        sourceBuilder.AppendRow([8L, null, null, null, null, null, null, null]);
        var source = sourceBuilder.Flush()!;

        var sourceColumns = new IArrowArray[source.ColumnCount];
        for (var i = 0; i < sourceColumns.Length; i++)
        {
            sourceColumns[i] = source.Column(i);
        }

        var target = new ArrowBatchBuilder(WideSchema());
        target.AppendFrom(sourceColumns, 0);
        target.AppendFrom(sourceColumns, 1);
        var batch = target.Flush()!;

        Assert.Equal(2, batch.Length);
        var ids = (Int64Array)batch.Column(0);
        var names = (StringArray)batch.Column(1);
        var amounts = (DoubleArray)batch.Column(2);
        var flags = (BooleanArray)batch.Column(3);
        var days = (Date32Array)batch.Column(4);
        var timestamps = (TimestampArray)batch.Column(5);
        var prices = (Decimal128Array)batch.Column(6);
        var counts = (Int32Array)batch.Column(7);

        Assert.Equal(3L, ids.GetValue(0));
        Assert.Equal("row-3", names.GetString(0));
        Assert.Equal(4.5, amounts.GetValue(0));
        Assert.False(flags.GetValue(0)); // Row(3): 3 % 2 == 0 is false
        Assert.Equal(new DateOnly(2026, 1, 4), days.GetDateOnly(0));
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 3, TimeSpan.Zero), timestamps.GetTimestamp(0));
        Assert.Equal(3m + 0.123456789m, prices.GetValue(0));
        Assert.Equal(3, counts.GetValue(0));

        Assert.Equal(8L, ids.GetValue(1));
        Assert.True(names.IsNull(1));
        Assert.True(amounts.IsNull(1));
        Assert.True(flags.IsNull(1));
        Assert.True(days.IsNull(1));
        Assert.True(timestamps.IsNull(1));
        Assert.True(prices.IsNull(1));
        Assert.True(counts.IsNull(1));

        source.Dispose();
        batch.Dispose();
    }

    [Fact]
    public void AppendFrom_wrong_column_count_throws()
    {
        var builder = new ArrowBatchBuilder(WideSchema());
        var oneColumn = new IArrowArray[] { new Int64Array.Builder().Append(1L).Build() };
        Assert.Throws<ArgumentException>(() => builder.AppendFrom(oneColumn, 0));
    }

    /// <summary>Steady-state managed allocation per batch must
    /// stay below a small fixed ceiling regardless of batch byte size — not proportional to it — proving
    /// final batch buffers are actually going through pooled native memory rather than the managed heap.
    /// Every row's boxed <c>object?[]</c> values are built ONCE, outside the measured loop, and reused
    /// across every simulated batch: boxing scalars for the row-based API is a fixed per-row cost
    /// unrelated to batch *memory* pooling, and would otherwise swamp the signal this test targets (the
    /// final-buffer allocation path: <c>Build(allocator)</c> plus small constant per-batch wrapper
    /// overhead). Uses the DEFAULT allocator (<see cref="Pz.Connectors.Abstractions.Memory.PooledNativeAllocator.Shared"/>)
    /// since that is what every real caller gets.</summary>
    [Fact]
    public void Builder_steady_state_allocations_below_ceiling()
    {
        const int rowsPerBatch = 500;
        const int warmupBatches = 5;
        const int measuredBatches = 50;
        // Fixed, not proportional to batch size: the whole point is that growing a batch must not grow
        // managed allocation. Generous enough to absorb wrapper overhead, tight enough that a single
        // batch-sized managed buffer would blow it.
        const long ceilingBytesPerBatch = 64 * 1024;

        var rows = new object?[rowsPerBatch][];
        for (var i = 0; i < rowsPerBatch; i++)
        {
            rows[i] = Row(i);
        }

        var builder = new ArrowBatchBuilder(WideSchema(), targetBatchBytes: int.MaxValue);

        void RunOneBatch()
        {
            foreach (var row in rows)
            {
                builder.AppendRow(row);
            }

            builder.Flush()!.Dispose();
        }

        for (var i = 0; i < warmupBatches; i++)
        {
            RunOneBatch();
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < measuredBatches; i++)
        {
            RunOneBatch();
        }

        var delta = GC.GetAllocatedBytesForCurrentThread() - before;
        var perBatch = delta / measuredBatches;

        Assert.True(perBatch <= ceilingBytesPerBatch,
            $"expected <= {ceilingBytesPerBatch} bytes managed allocation per batch in steady state, " +
            $"got {perBatch} bytes/batch ({delta} bytes over {measuredBatches} batches)");
    }
}
