using System.Data.SqlTypes;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Memory;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

public sealed class ArrowChunkCoalescerTests
{
    private static readonly Schema Schema = new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(true))
        .Field(f => f.Name("s").DataType(StringType.Default).Nullable(true))
        .Build();

    private const int LargeTarget = 32 * 1024 * 1024;
    private const int LargeCap = 1_000_000;

    // Row `id`'s string: null every 7th row, otherwise `pad` followed by the id, so a slice whose
    // offset is not byte-aligned in the validity bitmap still has nulls to get wrong.
    private static string? ExpectedString(long id, string pad) =>
        id % 7 == 0 ? null : pad + id.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private static RecordBatch Chunk(PooledNativeAllocator allocator, long firstId, int rows, string pad = "v")
    {
        var builder = new ArrowBatchBuilder(Schema, int.MaxValue, allocator, maxRowsPerBatch: int.MaxValue);
        for (var id = firstId; id < firstId + rows; id++)
        {
            builder.AppendRow([id, ExpectedString(id, pad)]);
        }

        return builder.Flush()!;
    }

    private static void AssertRows(RecordBatch batch, long firstId, int rows, string pad = "v")
    {
        Assert.Equal(rows, batch.Length);
        var ids = (Int64Array)batch.Column(0);
        var strings = (StringArray)batch.Column(1);
        for (var i = 0; i < rows; i++)
        {
            var id = firstId + i;
            Assert.Equal(id, ids.GetValue(i));
            var expected = ExpectedString(id, pad);
            Assert.Equal(expected is null, strings.IsNull(i));
            if (expected is not null)
            {
                Assert.Equal(expected, strings.GetString(i));
            }
        }
    }

    [Fact]
    public void Held_chunks_are_emitted_as_one_concatenated_batch_below_target()
    {
        var allocator = new PooledNativeAllocator();
        using (var coalescer = new ArrowChunkCoalescer(Schema, LargeTarget, LargeCap, allocator))
        {
            Assert.Empty(coalescer.Add(Chunk(allocator, 0, 1000)));
            Assert.Empty(coalescer.Add(Chunk(allocator, 1000, 1000)));
            Assert.Empty(coalescer.Add(Chunk(allocator, 2000, 1000)));

            using var batch = coalescer.Flush();
            Assert.NotNull(batch);
            // Every input chunk is disposed by now; the output must hold its own copy.
            AssertRows(batch, 0, 3000);
            Assert.Null(coalescer.Flush());
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Single_held_chunk_is_copied_not_aliased()
    {
        var allocator = new PooledNativeAllocator();
        using var coalescer = new ArrowChunkCoalescer(Schema, LargeTarget, LargeCap, allocator);
        Assert.Empty(coalescer.Add(Chunk(allocator, 0, 100)));
        using var batch = coalescer.Flush();
        Assert.NotNull(batch);

        // The input chunk's blocks are back on the allocator's free lists; a same-sized chunk with other
        // values reuses them, so an output that aliased the input would now read these values instead.
        using var clobber = Chunk(allocator, 5000, 100, pad: "w");

        AssertRows(batch, 0, 100);
    }

    [Fact]
    public void Chunk_crossing_the_row_cap_is_sliced_and_remainder_stays_pending()
    {
        var allocator = new PooledNativeAllocator();
        using (var coalescer = new ArrowChunkCoalescer(Schema, LargeTarget, maxRowsPerBatch: 250, allocator))
        {
            Assert.Empty(coalescer.Add(Chunk(allocator, 0, 100)));
            Assert.Empty(coalescer.Add(Chunk(allocator, 100, 100)));

            var emitted = coalescer.Add(Chunk(allocator, 200, 100)).ToList();
            var single = Assert.Single(emitted);
            using (single)
            {
                AssertRows(single, 0, 250);
            }

            // The remainder starts at row 50 of its chunk: not byte-aligned in the validity bitmap.
            using var remainder = coalescer.Flush();
            Assert.NotNull(remainder);
            AssertRows(remainder, 250, 50);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Byte_target_emits_before_row_cap()
    {
        var allocator = new PooledNativeAllocator();
        var pad = new string('x', 50);
        using (var coalescer = new ArrowChunkCoalescer(Schema, targetBatchBytes: 1024, LargeCap, allocator))
        {
            // 100 rows of 50-byte strings is well past 1 KiB, so the first chunk crosses the target and is
            // split into batches near 1 KiB each, far below the row cap.
            var emitted = coalescer.Add(Chunk(allocator, 0, 100, pad)).ToList();
            Assert.True(emitted.Count >= 2, $"expected the chunk split across batches, got {emitted.Count}");

            var next = 0L;
            foreach (var batch in emitted)
            {
                using (batch)
                {
                    Assert.InRange(batch.Length, 1, 99);
                    AssertRows(batch, next, batch.Length, pad);
                    next += batch.Length;
                }
            }

            using (var rest = coalescer.Flush())
            {
                if (rest is not null)
                {
                    AssertRows(rest, next, rest.Length, pad);
                    next += rest.Length;
                }
            }

            Assert.Equal(100L, next);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Dispose_releases_pending_chunks()
    {
        var allocator = new PooledNativeAllocator();
        var baseline = allocator.RentedBytes;

        var coalescer = new ArrowChunkCoalescer(Schema, LargeTarget, maxRowsPerBatch: 250, allocator);
        Assert.Empty(coalescer.Add(Chunk(allocator, 0, 100)));
        Assert.Empty(coalescer.Add(Chunk(allocator, 100, 100)));
        using (var emitted = Assert.Single(coalescer.Add(Chunk(allocator, 200, 100)).ToList()))
        {
            AssertRows(emitted, 0, 250);
        }

        Assert.NotEqual(baseline, allocator.RentedBytes);
        coalescer.Dispose();

        Assert.Equal(baseline, allocator.RentedBytes);
    }

    // The whole v0 egress type matrix, so every column kind crosses concatenation seams and slice
    // boundaries together.
    private static readonly Schema MatrixSchema = new Schema.Builder()
        .Field(f => f.Name("i32").DataType(Int32Type.Default).Nullable(true))
        .Field(f => f.Name("i64").DataType(Int64Type.Default).Nullable(true))
        .Field(f => f.Name("f64").DataType(DoubleType.Default).Nullable(true))
        .Field(f => f.Name("dec").DataType(new Decimal128Type(38, 9)).Nullable(true))
        .Field(f => f.Name("s").DataType(StringType.Default).Nullable(true))
        .Field(f => f.Name("b").DataType(BooleanType.Default).Nullable(true))
        .Field(f => f.Name("d").DataType(Date32Type.Default).Nullable(true))
        .Field(f => f.Name("ts").DataType(new TimestampType(TimeUnit.Microsecond, "UTC")).Nullable(true))
        .Build();

    private static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    // Row `id`'s value in column `col`, or null. Each column has its own null period so validity bits do
    // not line up across columns; with `nulls: false` every value is present.
    private static object? MatrixValue(long id, int col, bool nulls)
    {
        if (nulls && id % (3 + col) == 1)
        {
            return null;
        }

        return col switch
        {
            0 => (int)(id * 3 - 1000),
            1 => id * 1_000_003L,
            2 => id * 0.5,
            // Beyond System.Decimal's 28-29 digits, so only an exact 16-byte copy survives.
            3 => SqlDecimal.Parse("12345678901234567890123456789." + Math.Abs(id % 1_000_000_000).ToString("D9", CultureInfo.InvariantCulture)),
            4 => id % 5 == 0 ? string.Empty : "ză" + id.ToString(CultureInfo.InvariantCulture),
            5 => id % 3 != 0,
            6 => DateOnly.FromDateTime(Epoch.UtcDateTime).AddDays((int)(id % 365)),
            7 => Epoch.AddTicks(id * 17 * TimeSpan.TicksPerMillisecond + id * 10),
            _ => throw new ArgumentOutOfRangeException(nameof(col)),
        };
    }

    private static RecordBatch MatrixChunk(PooledNativeAllocator allocator, long firstId, int rows, bool nulls = true)
    {
        var builder = new ArrowBatchBuilder(MatrixSchema, int.MaxValue, allocator, maxRowsPerBatch: int.MaxValue);
        var row = new object?[MatrixSchema.FieldsList.Count];
        for (var id = firstId; id < firstId + rows; id++)
        {
            for (var col = 0; col < row.Length; col++)
            {
                row[col] = MatrixValue(id, col, nulls);
            }

            builder.AppendRow(row);
        }

        return builder.Flush()!;
    }

    private static void AssertMatrixRows(RecordBatch batch, long firstId, int rows, bool nulls = true) =>
        AssertMatrixRows(batch, i => firstId + i, rows, nulls);

    private static void AssertMatrixRows(RecordBatch batch, Func<int, long> idOf, int rows, bool nulls = true)
    {
        Assert.Equal(rows, batch.Length);
        for (var col = 0; col < MatrixSchema.FieldsList.Count; col++)
        {
            var array = batch.Column(col);
            Assert.Equal(rows, array.Length);
            var expectedNulls = 0;
            for (var i = 0; i < rows; i++)
            {
                var id = idOf(i);
                var expected = MatrixValue(id, col, nulls);
                Assert.True(expected is null == array.IsNull(i), $"column {col} row {id}: null mismatch");
                if (expected is null)
                {
                    expectedNulls++;
                    continue;
                }

                object actual = col switch
                {
                    0 => ((Int32Array)array).GetValue(i)!.Value,
                    1 => ((Int64Array)array).GetValue(i)!.Value,
                    2 => ((DoubleArray)array).GetValue(i)!.Value,
                    3 => ((Decimal128Array)array).GetSqlDecimal(i)!.Value,
                    4 => ((StringArray)array).GetString(i)!,
                    5 => ((BooleanArray)array).GetValue(i)!.Value,
                    6 => ((Date32Array)array).GetDateOnly(i)!.Value,
                    7 => ((TimestampArray)array).GetTimestamp(i)!.Value,
                    _ => throw new ArgumentOutOfRangeException(nameof(col)),
                };
                Assert.True(expected.Equals(actual), $"column {col} row {id}: expected {expected}, got {actual}");
            }

            Assert.Equal(expectedNulls, array.NullCount);
        }
    }

    [Fact]
    public void Every_matrix_type_survives_slices_at_unaligned_bitmap_offsets()
    {
        var allocator = new PooledNativeAllocator();
        using (var coalescer = new ArrowChunkCoalescer(MatrixSchema, LargeTarget, maxRowsPerBatch: 50, allocator))
        {
            // Chunk lengths 13/37/101 and a 50-row cap put every seam and every slice start at a bit
            // offset that is not a multiple of 8 (13, 50, 63, 100, 150 ...).
            var next = 0L;
            var emittedRows = 0L;
            foreach (var rows in new[] { 13, 37, 101, 29, 3 })
            {
                foreach (var batch in coalescer.Add(MatrixChunk(allocator, next, rows)).ToList())
                {
                    using (batch)
                    {
                        Assert.Equal(50, batch.Length);
                        AssertMatrixRows(batch, emittedRows, batch.Length);
                        emittedRows += batch.Length;
                    }
                }

                next += rows;
            }

            using var rest = coalescer.Flush();
            Assert.NotNull(rest);
            AssertMatrixRows(rest, emittedRows, rest.Length);
            Assert.Equal(next, emittedRows + rest.Length);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Input_columns_with_their_own_array_offset_are_rebased()
    {
        var allocator = new PooledNativeAllocator();
        using (var coalescer = new ArrowChunkCoalescer(MatrixSchema, LargeTarget, LargeCap, allocator))
        {
            // Each input column is itself a slice starting at row 11 of its buffers, as an imported array
            // with a non-zero offset is: utf8 offsets then start far from zero and bitmaps mid-byte.
            foreach (var firstId in new long[] { 0, 100, 200 })
            {
                var parent = MatrixChunk(allocator, firstId - 11, 100);
                var columns = new IArrowArray[MatrixSchema.FieldsList.Count];
                for (var col = 0; col < columns.Length; col++)
                {
                    columns[col] = ArrowArrayFactory.Slice(parent.Column(col), 11, 89);
                }

                // The slices share the parent's buffers; disposing the sliced batch releases them once.
                Assert.Empty(coalescer.Add(new RecordBatch(MatrixSchema, columns, 89)));
            }

            using var batch = coalescer.Flush();
            Assert.NotNull(batch);
            AssertMatrixRows(batch, i => (i / 89 * 100L) + (i % 89), 267);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Columns_without_nulls_omit_the_validity_bitmap()
    {
        var allocator = new PooledNativeAllocator();
        using (var coalescer = new ArrowChunkCoalescer(MatrixSchema, LargeTarget, maxRowsPerBatch: 60, allocator))
        {
            Assert.Empty(coalescer.Add(MatrixChunk(allocator, 0, 13, nulls: false)));
            using (var batch = Assert.Single(coalescer.Add(MatrixChunk(allocator, 13, 70, nulls: false)).ToList()))
            {
                AssertMatrixRows(batch, 0, 60, nulls: false);
                for (var col = 0; col < batch.ColumnCount; col++)
                {
                    Assert.Equal(0, batch.Column(col).NullCount);
                    Assert.True(batch.Column(col).Data.Buffers[0].IsEmpty, $"column {col} carries a validity bitmap");
                }
            }

            using var rest = coalescer.Flush();
            Assert.NotNull(rest);
            AssertMatrixRows(rest, 60, 23, nulls: false);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Concatenation_does_not_stage_columns_in_managed_memory()
    {
        // 122,880 int64 rows is ~983 KB per column: staged on the managed heap, that is one large-object
        // allocation per column per batch, which the pooled native allocator exists to avoid.
        const int Rows = 122_880;
        var schema = new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(true))
            .Field(f => f.Name("x").DataType(DoubleType.Default).Nullable(true))
            .Build();
        var allocator = new PooledNativeAllocator();

        RecordBatch Build(long first)
        {
            var builder = new ArrowBatchBuilder(schema, int.MaxValue, allocator, maxRowsPerBatch: int.MaxValue);
            for (var id = first; id < first + Rows; id++)
            {
                builder.AppendRow([id, id % 11 == 0 ? null : id * 0.25]);
            }

            return builder.Flush()!;
        }

        using (var coalescer = new ArrowChunkCoalescer(schema, LargeTarget, Rows, allocator))
        {
            var warm = Build(0);
            using (Assert.Single(coalescer.Add(warm).ToList()))
            {
            }

            var chunk = Build(Rows);
            var before = GC.GetAllocatedBytesForCurrentThread();
            var emitted = coalescer.Add(chunk).ToList();
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            using (var batch = Assert.Single(emitted))
            {
                Assert.Equal(Rows, batch.Length);
                Assert.Equal(Rows * 2L, ((Int64Array)batch.Column(0)).GetValue(Rows - 1)!.Value + 1);
            }

            Assert.True(allocated < 64 * 1024, $"concatenation allocated {allocated} managed bytes");
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public void Type_outside_the_v0_matrix_is_refused_at_construction()
    {
        var schema = new Schema.Builder()
            .Field(f => f.Name("f").DataType(FloatType.Default).Nullable(true))
            .Build();

        var error = Assert.Throws<NotSupportedException>(
            () => new ArrowChunkCoalescer(schema, LargeTarget, LargeCap, new PooledNativeAllocator()));
        Assert.Contains("'f'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Bit_copy_matches_a_bit_by_bit_copy_at_every_offset_pair()
    {
        var source = new byte[48];
        for (var i = 0; i < source.Length; i++)
        {
            source[i] = (byte)((i * 73) ^ 0xA5);
        }

        foreach (var count in new[] { 0, 1, 7, 8, 9, 63, 64, 65, 71, 72, 130, 250 })
        {
            for (var sourceBit = 0; sourceBit < 16; sourceBit++)
            {
                for (var targetBit = 0; targetBit < 16; targetBit++)
                {
                    var actual = new byte[48];
                    ArrowColumnConcat.CopyBits(source, sourceBit, actual, targetBit, count);

                    var expected = new byte[48];
                    for (var k = 0; k < count; k++)
                    {
                        var s = sourceBit + k;
                        if ((source[s >> 3] & (1 << (s & 7))) != 0)
                        {
                            var t = targetBit + k;
                            expected[t >> 3] |= (byte)(1 << (t & 7));
                        }
                    }

                    Assert.True(
                        expected.AsSpan().SequenceEqual(actual),
                        $"count {count}, source bit {sourceBit}, target bit {targetBit}");
                }
            }
        }
    }
}
