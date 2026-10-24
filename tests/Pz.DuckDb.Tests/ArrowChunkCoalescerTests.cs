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
}
