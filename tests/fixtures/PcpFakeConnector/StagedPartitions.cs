using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace PcpFakeConnector;

/// <summary>Projects every batch of <paramref name="inner"/> to <paramref name="columns"/>, in that
/// order, the way a source that honors <see cref="ReadHints.Columns"/> shapes its batches. The
/// LocalFiles connector underneath ignores the hint and yields its full shape, so this is what turns
/// the fixture into a pruning connector. Column names match case-insensitively, as the engine matches
/// them; a hinted name the batch does not carry is a fixture misuse and throws.</summary>
internal sealed class PruningReadPartition(IDatasetPartition inner, IReadOnlyList<string> columns) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in inner.ReadAsync(options, ct).WithCancellation(ct).ConfigureAwait(false))
        {
            var fields = new List<Field>(columns.Count);
            var arrays = new List<IArrowArray>(columns.Count);
            foreach (var name in columns)
            {
                var index = -1;
                for (var i = 0; i < batch.Schema.FieldsList.Count; i++)
                {
                    if (string.Equals(batch.Schema.FieldsList[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    {
                        index = i;
                        break;
                    }
                }

                if (index < 0)
                {
                    batch.Dispose();
                    throw new InvalidOperationException($"--prune-columns: hinted column '{name}' is not in the batch");
                }

                fields.Add(batch.Schema.FieldsList[index]);
                arrays.Add(batch.Column(index));
            }

            // The projected batch shares the inner batch's buffers; disposing the inner batch would
            // release them under the projection, so the inner batch is left to the projection's
            // consumer, which owns the projected batch and, through it, those buffers.
            yield return new RecordBatch(new Schema(fields, batch.Schema.Metadata), arrays, batch.Length);
        }
    }
}

/// <summary>Replays <paramref name="inner"/> forever, so a read has no natural end and the only thing
/// that can stop it is cancellation -- the shape a host-side cancellation test needs, since every
/// honest dataset this fixture serves finishes in milliseconds. The pause between passes keeps the
/// stream from spinning the CPU while a test waits to cancel it.</summary>
internal sealed class EndlessReadPartition(IDatasetPartition inner) : IDatasetPartition
{
    private static readonly TimeSpan PassInterval = TimeSpan.FromMilliseconds(20);

    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        while (true)
        {
            await foreach (var batch in inner.ReadAsync(options, ct).WithCancellation(ct).ConfigureAwait(false))
            {
                yield return batch;
            }

            await Task.Delay(PassInterval, ct).ConfigureAwait(false);
        }
    }
}

/// <summary>Counts rows as <paramref name="inner"/> drains and, once the drain completed, exposes
/// <c>"&lt;prior&gt;+&lt;rows&gt;"</c> as the sync-state candidate, where <c>&lt;prior&gt;</c> is the
/// token the host replayed through <see cref="DatasetSpec.PriorSyncState"/> (or <c>"0"</c> on a first
/// run). Deterministic and visibly cumulative across runs -- <c>"0+4"</c>, then <c>"0+4+4"</c> -- so a
/// host-side test can prove the prior token reached the connector. No candidate before the drain
/// finished: a host that polls early must see "none", not a partial count.</summary>
internal sealed class SyncStateReadPartition(IDatasetPartition inner, DatasetSpec spec)
    : IDatasetPartition, ISyncStatePartition
{
    private long _rows;
    private int _drained;

    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await foreach (var batch in inner.ReadAsync(options, ct).WithCancellation(ct).ConfigureAwait(false))
        {
            _rows += batch.Length;
            yield return batch;
        }

        Volatile.Write(ref _drained, 1);
    }

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = Volatile.Read(ref _drained) == 1
            ? $"{spec.PriorSyncState ?? "0"}+{_rows.ToString(System.Globalization.CultureInfo.InvariantCulture)}"
            : null;
        return candidate is not null;
    }
}

/// <summary>Wraps one already-planned partition so its ENTIRE drain (open through end-of-stream) runs
/// under one gate operation -- "one GateAcquire/GateComplete per partition read". Batches still flow
/// through as the inner partition produces them: the gate's own op is a start/finish handshake
/// running concurrently with the drain, not the drain itself.</summary>
internal sealed class GatedReadPartition(IDatasetPartition inner, IOperationGate gate, string opLabel)
    : IDatasetPartition
{
    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        ReadCoreAsync(options, ct);

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = gate.ExecuteAsync<object?>(opLabel, idempotent: false, async ct2 =>
        {
            started.TrySetResult();
            await finished.Task.WaitAsync(ct2).ConfigureAwait(false);
            return null;
        }, ct);

        await started.Task.ConfigureAwait(false);

        var enumerator = inner.ReadAsync(options, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                RecordBatch? batch;
                try
                {
                    batch = await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
                }
                catch (PzConnectorException ex) when (ex.IsTransient)
                {
                    finished.TrySetException(ex);
                    throw;
                }
                catch
                {
                    finished.TrySetResult();
                    throw;
                }

                if (batch is null)
                {
                    break;
                }

                yield return batch;
            }

            finished.TrySetResult();
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            // Joined on every path so GateComplete is always flushed and the task is always observed;
            // never a NEW failure out of a finally, which would replace the exception in flight.
            try
            {
                await gated.ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }
}

/// <summary>Gives a partition a stable id, the shape <c>StablePartitionIds</c> promises.</summary>
internal sealed class IdentifiedReadPartition(IDatasetPartition inner, string partitionId) : IIdentifiedPartition
{
    public string PartitionId => partitionId;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        inner.ReadAsync(options, ct);
}

/// <summary>A stable id AND a sync-state token on one partition: the combination that makes the
/// host build its identified-sync-state shim, which nothing else exercises.</summary>
internal sealed class IdentifiedSyncStateReadPartition(SyncStateReadPartition inner, string partitionId)
    : IIdentifiedPartition, ISyncStatePartition
{
    public string PartitionId => partitionId;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        inner.ReadAsync(options, ct);

    public bool TryGetSyncStateCandidate(out string? candidate) => inner.TryGetSyncStateCandidate(out candidate);
}
