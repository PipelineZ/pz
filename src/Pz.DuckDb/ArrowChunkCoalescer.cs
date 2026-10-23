using Apache.Arrow;
using Apache.Arrow.Memory;

namespace Pz.DuckDb;

/// <summary>Coalesces a stream of small Arrow chunks (DuckDB's ~2048-row data chunks) into engine-owned
/// batches by concatenating whole columns: one buffer copy per column per output batch, no per-cell work.
///
/// Ownership: <see cref="Add"/> takes ownership of its chunk at the call, before its result is
/// enumerated. A held chunk is disposed right after the concatenation that consumes its last pending row,
/// or by <see cref="Dispose"/>; a chunk partly consumed by a slice stays held until its remainder
/// is emitted. Every output batch's buffers are freshly allocated from the allocator passed in — never
/// shared with an input chunk — so an output batch outlives the chunks it came from and its owner
/// disposes it independently. Not thread-safe.</summary>
internal sealed class ArrowChunkCoalescer : IDisposable
{
    private readonly Schema _schema;
    private readonly int _targetBatchBytes;
    private readonly int _maxRowsPerBatch;
    private readonly MemoryAllocator _allocator;

    // Chunks with pending rows, oldest first, and each one's buffer footprint. Rows before `_headOffset`
    // in `_held[0]` were already emitted.
    private readonly List<RecordBatch> _held = new();
    private readonly List<long> _heldBytes = new();
    private int _headOffset;
    private int _pendingRows;
    private long _pendingBytes;
    private bool _disposed;

    public ArrowChunkCoalescer(Schema schema, int targetBatchBytes, int maxRowsPerBatch, MemoryAllocator allocator)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(allocator);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRowsPerBatch);
        _schema = schema;
        _targetBatchBytes = targetBatchBytes;
        _maxRowsPerBatch = maxRowsPerBatch;
        _allocator = allocator;
    }

    /// <summary>Takes ownership of <paramref name="chunk"/> immediately, then yields every batch that is
    /// now due: one of exactly the row cap while pending rows reach it, then, while pending bytes reach
    /// the target, one of the leading pending rows whose share of pending bytes is the target (so a
    /// target smaller than one chunk still yields batches near the target). Each yielded batch belongs to
    /// the caller. Stopping the enumeration early leaves the remaining rows pending.</summary>
    public IEnumerable<RecordBatch> Add(RecordBatch chunk)
    {
        ArgumentNullException.ThrowIfNull(chunk);
        if (_disposed)
        {
            chunk.Dispose();
            throw new ObjectDisposedException(nameof(ArrowChunkCoalescer));
        }

        if (chunk.Length == 0)
        {
            chunk.Dispose();
        }
        else
        {
            var bytes = BufferBytes(chunk);
            _held.Add(chunk);
            _heldBytes.Add(bytes);
            _pendingRows += chunk.Length;
            _pendingBytes += bytes;
        }

        return EmitDue();
    }

    /// <summary>Emits every pending row as one batch (owned by the caller), or returns null when nothing
    /// is pending.</summary>
    public RecordBatch? Flush()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _pendingRows == 0 ? null : Emit(_pendingRows);
    }

    /// <summary>Disposes every held chunk without emitting its pending rows.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var chunk in _held)
        {
            chunk.Dispose();
        }

        _held.Clear();
        _heldBytes.Clear();
        _headOffset = 0;
        _pendingRows = 0;
        _pendingBytes = 0;
    }

    private IEnumerable<RecordBatch> EmitDue()
    {
        while (!_disposed && _pendingRows > 0)
        {
            if (_pendingRows >= _maxRowsPerBatch)
            {
                yield return Emit(_maxRowsPerBatch);
            }
            else if (EffectivePendingBytes() is var bytes && bytes >= _targetBatchBytes)
            {
                var rows = (int)Math.Ceiling(_pendingRows * ((double)_targetBatchBytes / bytes));
                yield return Emit(Math.Clamp(rows, 1, _pendingRows));
            }
            else
            {
                yield break;
            }
        }
    }

    /// <summary>Pending bytes with the already-emitted rows of a partly consumed head chunk pro-rated
    /// out, so a chunk sliced for the byte target is not re-counted at full size on every slice.</summary>
    private double EffectivePendingBytes() =>
        _headOffset == 0
            ? _pendingBytes
            : _pendingBytes - (_heldBytes[0] * (double)_headOffset / _held[0].Length);

    /// <summary>Concatenates the first <paramref name="rows"/> pending rows into one batch. State changes
    /// (disposing consumed chunks, advancing the head) happen only after every column concatenated, so a
    /// failure leaves every chunk held for <see cref="Dispose"/>.</summary>
    private RecordBatch Emit(int rows)
    {
        // Which held chunks contribute, and where the last one stops: chunks [0, fullyConsumed) are used up
        // by this batch; when `tailRows` > 0, chunk `fullyConsumed` contributes its first `tailRows`
        // pending rows and stays held.
        var fullyConsumed = 0;
        var tailRows = 0;
        var remaining = rows;
        while (remaining > 0)
        {
            var available = _held[fullyConsumed].Length - (fullyConsumed == 0 ? _headOffset : 0);
            if (available <= remaining)
            {
                remaining -= available;
                fullyConsumed++;
            }
            else
            {
                tailRows = remaining;
                remaining = 0;
            }
        }

        var contributing = fullyConsumed + (tailRows > 0 ? 1 : 0);
        var columnCount = _schema.FieldsList.Count;
        var arrays = new IArrowArray[columnCount];
        var built = 0;
        try
        {
            // A single input would make the concatenator return that input unchanged (aliasing chunk
            // memory), so one contributing chunk is padded with an empty slice to force a copy.
            var slices = new List<IArrowArray>(Math.Max(contributing, 2));
            for (var col = 0; col < columnCount; col++)
            {
                slices.Clear();
                for (var i = 0; i < contributing; i++)
                {
                    var column = _held[i].Column(col);
                    var offset = i == 0 ? _headOffset : 0;
                    var length = i == fullyConsumed ? tailRows : column.Length - offset;
                    // Slices share their parent's buffers: they are never disposed; the chunk is.
                    slices.Add(offset == 0 && length == column.Length
                        ? column
                        : ArrowArrayFactory.Slice(column, offset, length));
                }

                if (slices.Count == 1)
                {
                    slices.Add(ArrowArrayFactory.Slice(slices[0], 0, 0));
                }

                arrays[col] = ArrowArrayConcatenator.Concatenate(slices, _allocator);
                built++;
            }
        }
        catch
        {
            for (var col = 0; col < built; col++)
            {
                arrays[col].Dispose();
            }

            throw;
        }

        var batch = new RecordBatch(_schema, arrays, rows);

        for (var i = 0; i < fullyConsumed; i++)
        {
            _held[i].Dispose();
            _pendingBytes -= _heldBytes[i];
        }

        _held.RemoveRange(0, fullyConsumed);
        _heldBytes.RemoveRange(0, fullyConsumed);
        _headOffset = tailRows > 0 ? (fullyConsumed == 0 ? _headOffset : 0) + tailRows : 0;
        _pendingRows -= rows;
        return batch;
    }

    /// <summary>The chunk's buffer footprint: every buffer of every column at its full length, so a
    /// partly consumed chunk keeps counting all of its memory until it is released.</summary>
    private static long BufferBytes(RecordBatch chunk)
    {
        var total = 0L;
        for (var col = 0; col < chunk.ColumnCount; col++)
        {
            foreach (var buffer in chunk.Column(col).Data.Buffers)
            {
                total += buffer.Length;
            }
        }

        return total;
    }
}
