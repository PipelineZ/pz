using System.Runtime.CompilerServices;
using Pz.Connectors.Abstractions;

namespace Pz.Engine.Execution;

/// <summary>Whether any partition of one SourceLoad reported that a connector rule skipped its read
/// (<see cref="ISkippedReadPartition"/>). Noted after each partition's read finishes; safe for concurrent
/// partitions.</summary>
internal sealed class SkippedReads
{
    private int _any;

    public bool Any => Volatile.Read(ref _any) != 0;

    public void Note(IDatasetPartition partition)
    {
        if (partition is ISkippedReadPartition { ReadSkipped: true })
        {
            Interlocked.Exchange(ref _any, 1);
        }
    }

    public void MarkSkipped() => Interlocked.Exchange(ref _any, 1);

    public void NoteAll(IEnumerable<IDatasetPartition> partitions)
    {
        foreach (var partition in partitions)
        {
            Note(partition);
        }
    }

    /// <summary>Passes a lazily planned partition stream through unchanged, keeping a reference to each
    /// partition that can report a skip so <see cref="NoteAll"/> can ask it once the load is done. Partitions
    /// that cannot report one are not kept.</summary>
    public async IAsyncEnumerable<IDatasetPartition> Track(IAsyncEnumerable<IDatasetPartition> partitions,
        List<IDatasetPartition> kept, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var partition in partitions.WithCancellation(ct).ConfigureAwait(false))
        {
            if (partition is ISkippedReadPartition)
            {
                lock (kept)
                {
                    kept.Add(partition);
                }
            }

            yield return partition;
        }
    }
}
