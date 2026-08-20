using Apache.Arrow;

namespace Pz.Engine.Execution;

/// <summary>Byte-size estimation for progress reporting: <c>batch.Length</c> is a row
/// count, not a byte size, so <see cref="NodeProgressEvent"/>'s <c>Bytes</c> field is instead derived by
/// summing every column's underlying Arrow buffer lengths (recursing into nested/child arrays, e.g.
/// lists and structs). This is an approximation — Arrow buffers may be shared/sliced across batches and
/// validity bitmaps are included — but it is cheap (no data copies) and stable enough for a progress
/// indicator, which is all this is used for.</summary>
public static class ArrowBatchExtensions
{
    public static long ApproximateSize(this RecordBatch batch)
    {
        var total = 0L;
        foreach (var array in batch.Arrays)
        {
            total += ApproximateSize(array.Data);
        }

        return total;
    }

    private static long ApproximateSize(ArrayData data)
    {
        var total = 0L;
        foreach (var buffer in data.Buffers)
        {
            total += buffer.Length;
        }

        if (data.Children is not null)
        {
            foreach (var child in data.Children)
            {
                total += ApproximateSize(child);
            }
        }

        return total;
    }
}
