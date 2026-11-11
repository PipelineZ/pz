using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Paths;

namespace Pz.Shared.Sinks;

/// <summary>Date-partitioned (fan-out), per-partition-atomic write session: routes each incoming Arrow
/// row into the output folder its <c>partition_by</c> value renders to
/// (<see cref="PathTemplate.Render(string, DateTimeOffset)"/>) and drives one inner session per distinct
/// folder, so a batch spanning two days lands as two files. Each inner session keeps its own
/// commit-xor-abort state machine. The overall write is therefore at-least-once at the set level (a
/// failed run may leave a subset of partitions committed, which <c>pz retry</c> or a re-run reconciles)
/// and never falsely atomic across partitions: a staged all-or-nothing promote is deliberately not
/// used, because on a file or object store it can only pretend to be atomic.
///
/// <para>The fan-out session honours the same contract as a single session: exactly one of
/// <see cref="CommitAsync"/>/<see cref="AbortAsync"/> runs, after which writes, commits and aborts are
/// rejected; <see cref="DisposeAsync"/> always releases every opened inner session best-effort, never
/// masking an earlier failure with cleanup fallout.</para>
///
/// <para>Batch ownership: the engine owns the batch handed to <see cref="WriteBatchAsync"/> until the
/// call returns. Every per-folder slice is copied out of it into pooled, off-heap builder buffers
/// (<see cref="ArrowBatchBuilder"/>) synchronously, before the first await, so the engine-owned batch
/// is never read across an await boundary.</para>
///
/// <para>Compiled into the azureblob, gcs and sftp connectors from this one source file: connectors
/// never reference each other.</para></summary>
internal sealed class PartitionedWriteSession : ISinkWriteSession
{
    private readonly Dictionary<string, ISinkWriteSession> _byFolder = new(StringComparer.Ordinal);
    private readonly Func<string, ValueTask<ISinkWriteSession>> _open;
    private readonly string _pathTemplate;
    private readonly int _partitionColIndex;
    private readonly Schema _schema;
    private readonly int _maxConcurrentWrites;
    private readonly IDisposable? _sharedResource;
    private State _state = State.Open;

    /// <param name="open">Opens the inner session for one rendered folder; called once per folder.</param>
    /// <param name="maxConcurrentWrites">How many folders' writes may be in flight at once. Use 1 when
    /// every inner session shares one serialized channel (one SFTP connection is one SSH channel, and
    /// concurrent requests over it race on its pending-request state).</param>
    /// <param name="sharedResource">A resource the inner sessions share but do not own, disposed once
    /// after every inner session has been disposed.</param>
    internal PartitionedWriteSession(
        Func<string, ValueTask<ISinkWriteSession>> open, string pathTemplate, int partitionColIndex, Schema schema,
        int maxConcurrentWrites, IDisposable? sharedResource = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrentWrites, 1);
        _open = open;
        _pathTemplate = pathTemplate;
        _partitionColIndex = partitionColIndex;
        _schema = schema;
        _maxConcurrentWrites = maxConcurrentWrites;
        _sharedResource = sharedResource;
    }

    private enum State { Open, Committed, Aborted }

    /// <summary>Resolves <paramref name="partitionBy"/> to its ordinal in <paramref name="schema"/>,
    /// failing fast (permanent, naming the output and column) before the sink opens any resource when
    /// the column is absent or not timestamp/date-typed.</summary>
    internal static int ResolvePartitionColumn(string output, Schema schema, string partitionBy)
    {
        var fields = schema.FieldsList;
        for (var i = 0; i < fields.Count; i++)
        {
            if (!string.Equals(fields[i].Name, partitionBy, StringComparison.Ordinal))
            {
                continue;
            }

            if (fields[i].DataType.TypeId is not (ArrowTypeId.Timestamp or ArrowTypeId.Date32))
            {
                throw new PzConnectorException(
                    $"output '{output}': partition_by column '{partitionBy}' is not a timestamp/date column " +
                    $"(got '{fields[i].DataType}')", isTransient: false);
            }

            return i;
        }

        throw new PzConnectorException(
            $"output '{output}': partition_by column '{partitionBy}' is not present in the write schema",
            isTransient: false);
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        ct.ThrowIfCancellationRequested();

        // 1. Group the row indices by destination folder. The folder comes only from the row's partition
        //    value via PathTemplate.Render (invariant culture), never from wall-clock, so folder discovery
        //    and order are a pure function of the batch.
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

        // 2. Materialise every per-folder slice before any await (see Batch ownership above). After this
        //    loop `batch` is no longer referenced; each slice owns buffers copied out of it.
        var slices = new List<(string Folder, RecordBatch Slice)>(groups.Count);
        try
        {
            foreach (var (folder, indices) in groups)
            {
                slices.Add((folder, BuildSlice(batch, indices)));
            }

            // 3. Route each slice to its folder's inner session, opened lazily on first use. On a
            //    serialized channel every open and write completes before the next folder's open starts,
            //    in folder discovery order.
            if (_maxConcurrentWrites == 1)
            {
                foreach (var (folder, slice) in slices)
                {
                    var session = await GetOrOpenAsync(folder).ConfigureAwait(false);
                    await session.WriteBatchAsync(slice, ct).ConfigureAwait(false);
                }

                return;
            }

            //    Otherwise the opens still run sequentially (`_byFolder` is a plain Dictionary, and every
            //    folder in `slices` is distinct by construction), then the writes run with at most
            //    _maxConcurrentWrites in flight, so a batch spanning many partitions never fans out an
            //    unbounded number of requests.
            var writes = new (ISinkWriteSession Session, RecordBatch Slice)[slices.Count];
            for (var i = 0; i < slices.Count; i++)
            {
                writes[i] = (await GetOrOpenAsync(slices[i].Folder).ConfigureAwait(false), slices[i].Slice);
            }

            using var admission = new SemaphoreSlim(_maxConcurrentWrites);
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
        // Committed up-front enforces that even if an inner commit throws part-way: the partitions already
        // committed stay (per-partition atomic), and DisposeAsync cleans up the rest.
        _state = State.Committed;

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
        _state = State.Aborted;

        foreach (var session in _byFolder.Values)
        {
            try
            {
                await session.AbortAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort by design: abort runs on an already-failing path, so a per-partition abort
                // failure must never mask the earlier error.
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            // Release every opened inner session. Each inner session's own state machine does the right
            // thing: one never committed aborts (deleting its temp file), a committed one no-ops, and a
            // commit-attempted-but-failed one leaves its files alone.
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
        finally
        {
            _sharedResource?.Dispose();
        }
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

    /// <summary>Copies the rows at <paramref name="indices"/> of <paramref name="batch"/> into a fresh
    /// <see cref="RecordBatch"/> over the same schema (pooled native buffers). The returned slice owns its
    /// buffers and does not alias the engine-owned input. <paramref name="indices"/> is non-empty by
    /// construction.</summary>
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

        // Flush emits every pending row as one batch regardless of size: an incoming batch is already
        // bounded, so each per-folder slice is a subset of it.
        return builder.Flush()!;
    }

    /// <summary>Reads one cell as the CLR shape <see cref="ArrowBatchBuilder"/>'s appenders expect (v0 type
    /// matrix). Null cells pass through as <c>null</c>.</summary>
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
            Decimal128Array a => Decimal128Cells.GetExactValue(a, row),
            StringArray a => a.GetString(row),
            Date32Array a => a.GetDateOnly(row)!.Value,
            TimestampArray a => a.GetTimestamp(row)!.Value,
            _ => throw new PzConnectorException(
                $"partitioned write does not support Arrow column type '{array.Data.DataType}'", isTransient: false),
        };
    }

    /// <summary>Reads the row's partition value as a UTC instant. The sink validated the column as
    /// timestamp/date before creating this session; a null partition value cannot be routed to any
    /// folder, so it is a permanent, named failure.</summary>
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
        if (_state != State.Open)
        {
            throw new InvalidOperationException(
                $"cannot {action} a partitioned session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}
