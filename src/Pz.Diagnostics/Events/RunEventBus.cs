using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Pz.Diagnostics.Events;

/// <summary>Transport-neutral fan-out point for the typed run-event stream.
/// Backed by <see cref="Channel.CreateUnbounded{T}(UnboundedChannelOptions)"/> with a single reader:
/// <see cref="Publish"/> is a <c>TryWrite</c> that never blocks and never throws — a slow or hung
/// renderer must never be able to stall the engine publishing events — and returns <see langword="false"/>
/// only once <see cref="Complete"/> has been called. Because there is exactly one channel and one reader,
/// events are observed in the exact order they were published: for any single node that is
/// Started → Progress* → [RetryScheduled*] → Completed (the engine publishes them from one logical flow
/// per node); interleaving across concurrently-running nodes reflects real concurrency and is not
/// ordered relative to each other.</summary>
public sealed class RunEventBus
{
    private readonly Channel<RunEvent> _channel = Channel.CreateUnbounded<RunEvent>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    // Test-only seam bookkeeping: SingleReader=true — the deliberate optimization this bus relies on —
    // makes the BCL's ChannelReader<T>.Count throw NotSupportedException, so PendingCountForTests below
    // tracks pending items itself via an Interlocked counter rather than reading the channel directly.
    private int _pendingCount;

    /// <summary>Never blocks, never throws. Returns <see langword="false"/> only after
    /// <see cref="Complete"/> has been called (the channel's writer is closed).</summary>
    public bool Publish(RunEvent evt)
    {
        var written = _channel.Writer.TryWrite(evt);
        if (written)
        {
            Interlocked.Increment(ref _pendingCount);
        }

        return written;
    }

    public async IAsyncEnumerable<RunEvent> ReadAllAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var evt in _channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
        {
            Interlocked.Decrement(ref _pendingCount);
            yield return evt;
        }
    }

    /// <summary>Idempotent: safe to call more than once (e.g. from both a terminal RunCompleted
    /// publish and a defensive caller-side cleanup).</summary>
    public void Complete() => _channel.Writer.TryComplete();

    /// <summary>Test-only seam: the number of published events not
    /// yet observed by the reader, letting a test prove <see cref="Publish"/> never blocks structurally —
    /// by publishing while a reader is deliberately gated off and asserting every publish landed in the
    /// channel — instead of timing a wall-clock ceiling.</summary>
    internal int PendingCountForTests => Volatile.Read(ref _pendingCount);
}
