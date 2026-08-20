using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Paths;

namespace Pz.Connector.AzureBlob;

/// <summary>Date-partitioned (fan-out), per-partition-atomic write session: routes each incoming Arrow
/// row into the output folder its <c>partition_by</c> timestamp renders to
/// (<see cref="PathTemplate.Render(string, DateTimeOffset)"/>) and drives one inner
/// <see cref="AzureWriteSession"/> (Parquet/Csv) per distinct folder — so a batch spanning two days lands
/// as two blobs. Each inner session keeps its own temp-blob→server-side-copy-promote commit-xor-abort
/// state machine, independent of the others; the overall write is therefore at-least-once at the SET level
/// (a failed run may leave a subset of partitions committed — <c>pz retry</c>/re-run reconciles), never
/// falsely atomic across partitions: a staged all-or-nothing promote is deliberately not used, because on
/// an object store it can only pretend to be atomic.
///
/// The fan-out session honours the same commit-xor-abort contract as a single session: exactly one of
/// <see cref="CommitAsync"/>/<see cref="AbortAsync"/> runs, after which writes/commits/aborts are rejected;
/// <see cref="DisposeAsync"/> always releases every opened inner session best-effort (each inner session's
/// own state machine then aborts the ones never committed and no-ops the committed ones), never masking an
/// earlier failure with cleanup fallout.
///
/// Batch ownership: the batch handed to <see cref="WriteBatchAsync"/> is engine-owned only until
/// the call returns. Every per-folder slice is materialised SYNCHRONOUSLY (copied out of the incoming batch
/// into pooled, off-heap builder buffers via <see cref="ArrowBatchBuilder"/>) BEFORE the first inner
/// <c>WriteBatchAsync</c> await, so the engine-owned batch is never read across an await boundary.</summary>
internal sealed class AzurePartitionedWriteSession : ISinkWriteSession
{
    private readonly Dictionary<string, ISinkWriteSession> _byFolder = new(StringComparer.Ordinal);
    private readonly Func<string, ValueTask<ISinkWriteSession>> _open;
    private readonly string _pathTemplate;
    private readonly int _partitionColIndex;
    private readonly Schema _schema;
    private AzureSessionState _state = AzureSessionState.Open;

    internal AzurePartitionedWriteSession(
        Func<string, ValueTask<ISinkWriteSession>> open, string pathTemplate, int partitionColIndex, Schema schema)
    {
        _open = open;
        _pathTemplate = pathTemplate;
        _partitionColIndex = partitionColIndex;
        _schema = schema;
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");

        // 1. Group the incoming batch's row indices by destination folder. Determinism: the folder comes
        //    only from the row's partition value via PathTemplate.Render (invariant culture) -- never from
        //    wall-clock. Folder discovery/order is a pure function of the batch.
        var partitionColumn = batch.Column(_partitionColIndex);
        var groups = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        for (var row = 0; row < batch.Length; row++)
        {
            var folder = PathTemplate.Render(_pathTemplate, ReadPartitionInstant(partitionColumn, row));
            if (!groups.TryGetValue(folder, out var indices))
            {
                indices = [];
                groups[folder] = indices;
            }

            indices.Add(row);
        }

        // 2. Materialise every per-folder slice SYNCHRONOUSLY -- before any await -- so the engine-owned
        //    `batch` -- whose pooled buffers the engine recycles the moment WriteBatchAsync returns -- is
        //    never read across an await boundary. After this loop `batch` and its arrays are no longer
        //    referenced; each slice owns pooled buffers copied out of the batch.
        var slices = new List<(string Folder, RecordBatch Slice)>(groups.Count);
        try
        {
            foreach (var (folder, indices) in groups)
            {
                slices.Add((folder, BuildSlice(batch, indices)));
            }

            // 3. Route each slice to its folder's inner session (opened lazily on first use for that
            //    folder). Opening is sequential -- `_byFolder` is a plain Dictionary, not thread-safe for
            //    concurrent inserts, and every folder in `slices` is distinct by construction (step 1's
            //    grouping), so this loop never blocks on network I/O itself. The writes themselves --
            //    the actual per-folder network round trips -- then run concurrently, bounded so a batch
            //    spanning many partitions doesn't fan out an unbounded number of in-flight requests.
            var writes = new (ISinkWriteSession Session, RecordBatch Slice)[slices.Count];
            for (var i = 0; i < slices.Count; i++)
            {
                writes[i] = (await GetOrOpenAsync(slices[i].Folder).ConfigureAwait(false), slices[i].Slice);
            }

            using var admission = new SemaphoreSlim(Environment.ProcessorCount);
            await Task.WhenAll(writes.Select(async w =>
            {
                await admission.WaitAsync(ct).ConfigureAwait(false);
                try
                {
                    await w.Session.WriteBatchAsync(w.Slice, ct).ConfigureAwait(false);
                }
                finally
                {
                    admission.Release();
                }
            })).ConfigureAwait(false);
        }
        finally
        {
            foreach (var (_, slice) in slices)
            {
                slice.Dispose();
            }
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");

        // Commit-attempted is permanent the instant commit begins: per the ISinkWriteSession contract a
        // failed commit must still forbid a later abort (its true outcome is unknown). Marking the fan-out
        // Committed up-front enforces that even if an inner commit throws part-way -- the already-promoted
        // partitions stay (per-partition atomic), the not-yet-committed ones are cleaned by DisposeAsync.
        _state = AzureSessionState.Committed;

        long rows = 0;
        long batches = 0;
        foreach (var session in _byFolder.Values)
        {
            var result = await session.CommitAsync(ct).ConfigureAwait(false);
            rows += result.RowsWritten;
            batches += result.BatchesWritten;
        }

        return new WriteResult(rows, batches);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");
        _state = AzureSessionState.Aborted;

        foreach (var session in _byFolder.Values)
        {
            try
            {
                await session.AbortAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort by design: abort runs on an already-failing path, so a per-partition abort
                // failure must never mask the earlier error (mirrors AzureWriteSession.TryDeleteTempBlobAsync).
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Always release every opened inner session. Each inner session's own state machine does the right
        // thing: an inner never committed aborts (deletes its temp blob), a committed inner no-ops, a
        // commit-attempted-but-failed inner leaves its blobs alone. Suppress cleanup fallout unconditionally.
        foreach (var session in _byFolder.Values)
        {
            try
            {
                await session.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppressed by design: never mask an earlier failure with dispose fallout.
            }
        }

        _byFolder.Clear();
    }

    private async ValueTask<ISinkWriteSession> GetOrOpenAsync(string folder)
    {
        if (_byFolder.TryGetValue(folder, out var existing))
        {
            return existing;
        }

        var session = await _open(folder).ConfigureAwait(false);
        _byFolder[folder] = session;
        return session;
    }

    /// <summary>Materialises the rows at <paramref name="indices"/> of <paramref name="batch"/> into a
    /// fresh <see cref="RecordBatch"/> over the same schema, column by column, via
    /// <see cref="ArrowBatchBuilder"/> (pooled native buffers). Called synchronously inside
    /// <see cref="WriteBatchAsync"/> before any await -- the returned slice owns its buffers and does not
    /// alias the engine-owned input. <paramref name="indices"/> is non-empty by construction.</summary>
    private RecordBatch BuildSlice(RecordBatch batch, List<int> indices)
    {
        var builder = new ArrowBatchBuilder(_schema);
        var values = new object?[batch.ColumnCount];
        foreach (var row in indices)
        {
            for (var col = 0; col < batch.ColumnCount; col++)
            {
                values[col] = ReadCell(batch.Column(col), row);
            }

            builder.AppendRow(values);
        }

        // Flush emits every pending row as one batch regardless of size -- a single incoming batch is
        // already bounded, so each per-folder slice is a subset of it.
        return builder.Flush()!;
    }

    /// <summary>Reads one cell as the CLR shape <see cref="ArrowBatchBuilder"/>'s appenders expect (v0 type
    /// matrix). Null cells pass through as <c>null</c> (SQL NULL for that column).</summary>
    private static object? ReadCell(IArrowArray array, int row)
    {
        if (array.IsNull(row))
        {
            return null;
        }

        return array switch
        {
            Int32Array a => a.GetValue(row)!.Value,
            Int64Array a => a.GetValue(row)!.Value,
            DoubleArray a => a.GetValue(row)!.Value,
            BooleanArray a => a.GetValue(row)!.Value,
            Decimal128Array a => a.GetValue(row)!.Value,
            StringArray a => a.GetString(row),
            Date32Array a => a.GetDateOnly(row)!.Value,
            TimestampArray a => a.GetTimestamp(row)!.Value,
            _ => throw new PzConnectorException(
                $"partitioned write does not support Arrow column type '{array.Data.DataType}'", isTransient: false),
        };
    }

    /// <summary>Reads the row's partition value as a UTC instant. The column type was already validated as
    /// timestamp/date by <c>AzureSink</c> before this session was created; a null partition value cannot be
    /// routed to any folder, so it is a permanent, named failure.</summary>
    private DateTimeOffset ReadPartitionInstant(IArrowArray column, int row)
    {
        if (column.IsNull(row))
        {
            throw new PzConnectorException(
                $"partition_by column '{_schema.FieldsList[_partitionColIndex].Name}' has a null value in row {row}; " +
                "a null partition value cannot be routed to a partition folder", isTransient: false);
        }

        return column switch
        {
            TimestampArray a => a.GetTimestamp(row)!.Value,
            Date32Array a => new DateTimeOffset(a.GetDateOnly(row)!.Value.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc)),
            _ => throw new PzConnectorException(
                $"partition_by column '{_schema.FieldsList[_partitionColIndex].Name}' is not a timestamp/date column " +
                $"(got '{column.Data.DataType}')", isTransient: false),
        };
    }

    private void EnsureOpen(string action)
    {
        if (_state != AzureSessionState.Open)
        {
            throw new InvalidOperationException(
                $"cannot {action} a partitioned session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}
