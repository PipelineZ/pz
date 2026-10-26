namespace Pz.Engine.Dispatch;

/// <summary>The dispatch permits one node runs under: its connection's <c>max_concurrency</c> permit, when the
/// connection declares one, and a global one. <see cref="RunOrchestrator"/> acquires and releases through it,
/// and publishes it as <see cref="Current"/> around the executor call, so a wait that holds no connector
/// session (a retry backoff, an open breaker's cool-down) can hand both permits back for its length through
/// <see cref="WhileReleasedAsync"/>. Used by one node at a time; not safe for concurrent use.</summary>
internal sealed class DispatchLease(SemaphoreSlim global, SemaphoreSlim? instance)
{
    private static readonly AsyncLocal<DispatchLease?> Ambient = new();

    private bool _holdsInstance;
    private bool _holdsGlobal;

    /// <summary>The lease of the node whose executor call is on this async flow; null outside one.</summary>
    internal static DispatchLease? Current => Ambient.Value;

    /// <summary>Sets <see cref="Current"/> for the async flow until disposed; restores the previous value.</summary>
    internal static IDisposable Use(DispatchLease lease)
    {
        ArgumentNullException.ThrowIfNull(lease);
        var previous = Ambient.Value;
        Ambient.Value = lease;
        return new Scope(previous);
    }

    /// <summary>Instance permit first, then global: a node queued on its own connection's cap must not occupy
    /// a global slot meanwhile. Returns holding both or, on cancellation, throws holding neither.</summary>
    internal async Task AcquireAsync(CancellationToken ct)
    {
        if (_holdsInstance || _holdsGlobal)
        {
            throw new InvalidOperationException("the dispatch lease is already held");
        }

        if (instance is not null)
        {
            await instance.WaitAsync(ct).ConfigureAwait(false);
            _holdsInstance = true;
        }

        try
        {
            await global.WaitAsync(ct).ConfigureAwait(false);
            _holdsGlobal = true;
        }
        catch
        {
            Release();
            throw;
        }
    }

    /// <summary>Releases whatever is held, global first, then instance (the reverse of acquisition). A second
    /// call without an acquisition in between releases nothing.</summary>
    internal void Release()
    {
        if (_holdsGlobal)
        {
            _holdsGlobal = false;
            global.Release();
        }

        if (_holdsInstance)
        {
            _holdsInstance = false;
            instance!.Release();
        }
    }

    /// <summary>Runs <paramref name="wait"/> without the permits and re-acquires them after, queueing behind
    /// nodes already waiting. On cancellation, during the wait or the re-acquire, throws holding nothing.</summary>
    internal async Task WhileReleasedAsync(Func<CancellationToken, Task> wait, CancellationToken ct)
    {
        Release();
        await wait(ct).ConfigureAwait(false);
        await AcquireAsync(ct).ConfigureAwait(false);
    }

    private sealed class Scope(DispatchLease? previous) : IDisposable
    {
        public void Dispose() => Ambient.Value = previous;
    }
}
