using System.Collections.Concurrent;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>The connector side of the PCP reverse channel. One instance per process; the host opens
/// exactly one <c>HostChannel</c> call, held for the process's lifetime.
///
/// <para>Every outgoing message -- gate traffic and logs alike -- goes through one
/// buffer-until-attach mechanism: a TCS that resolves once the HostChannel call is actually being
/// served. Because there is exactly one such call, <see cref="Detach"/> (its end, for any reason) is
/// terminal rather than a wait for reattachment: it fails the TCS instead of replacing it, so nothing
/// already waiting -- or sent afterward -- blocks on a channel that will never come back. See
/// <see cref="Close"/>.</para></summary>
internal sealed class HostChannelPeer
{
    /// <summary>Log events queued before the host's pump has attached are held here, newest wins:
    /// a connector that logs heavily while the host is still dialing must not grow without bound.</summary>
    private const int LogBacklogCapacity = 256;

    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _pendingGrants = new(StringComparer.Ordinal);
    private readonly Lock _attachGate = new();
    private readonly Queue<LogEvent> _logBacklog = new();
    private bool _isAttached;

    /// <summary>Set the moment <see cref="Detach"/> ends the one <c>HostChannel</c> call this process
    /// will ever see: the host opens it exactly once (see <c>HostChannelPump</c>'s own doc on the host
    /// side), so once it ends nothing will attach again. Every send already waiting on the current
    /// <see cref="_attached"/> TCS -- or issued after this point -- fails immediately instead of
    /// waiting on a channel that is never coming back, mirroring <c>HostChannelPump.FailAllPending</c>
    /// on the host side of the same channel rather than leaving a gated operation hanging forever.</summary>
    private bool _closed;

    /// <summary>Set alongside <see cref="_closed"/>, under the same lock: what <see cref="SendAsync"/>
    /// throws for a send issued after closing, since re-faulting the (possibly already-resolved)
    /// <see cref="_attached"/> TCS cannot cover that case by itself.</summary>
    private Exception? _closedReason;

    /// <summary>The tail of the log send sequence. Every log send is chained onto it while
    /// <see cref="_attachGate"/> is held, so the order sends are started is the order the events were
    /// produced -- and the backlog, chained first inside <see cref="Attach"/>, cannot be overtaken by
    /// a live <see cref="QueueLog"/> from another thread.</summary>
    private Task _logChain = Task.CompletedTask;

    private TaskCompletionSource<IServerStreamWriter<HostChannelUp>> _attached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Called once per <c>HostChannel</c> RPC, right as it starts serving. Unblocks every send
    /// (log or gate) that was already waiting on this channel to open, then flushes the held logs.
    /// The backlog is chained onto the log sequence before the gate is released, so it is ahead of any
    /// log queued after this returns.</summary>
    public void Attach(IServerStreamWriter<HostChannelUp> writer)
    {
        TaskCompletionSource<IServerStreamWriter<HostChannelUp>> signal;
        lock (_attachGate)
        {
            if (_closed)
            {
                // The one HostChannel call for this process's lifetime already ended (see _closed's
                // own doc); a late Attach has nothing left to resolve.
                return;
            }

            signal = _attached;
            _isAttached = true;
            while (_logBacklog.Count > 0)
            {
                _logChain = ChainLogAsync(_logChain, _logBacklog.Dequeue());
            }
        }

        // After the chain is built: each chained send waits on this signal, so the writer is in place
        // by the time any of them reaches it.
        signal.TrySetResult(writer);
    }

    public void Detach() => Close(new PzConnectorException(
        "the PCP reverse channel ended; no further host attach is expected for this connector process",
        isTransient: false));

    /// <summary>Ends this peer for good, idempotently: also called (with a process-shutdown reason)
    /// when the host's own application lifetime starts stopping, which is what bounds the "never
    /// attaches at all" case -- a channel that Detach was never called for (the HostChannel RPC never
    /// even started) would otherwise wait on the very first Attach forever.</summary>
    public void Close(Exception reason)
    {
        lock (_attachGate)
        {
            if (_closed)
            {
                return;
            }

            _isAttached = false;
            _closed = true;
            _closedReason = reason;
            // Fails whoever is already waiting on this signal (SendAsync's buffer-until-attach) rather
            // than leaving them parked: a gated operation cannot finish -- and, over CancellationToken
            // .None, could not even be cancelled -- if this simply replaced the TCS with a fresh
            // unresolved one, since no future Attach is coming. A no-op when _attached already resolved
            // to a writer (the common Attach-then-Detach shape); SendAsync's own _closed check above
            // covers that case instead.
            _attached.TrySetException(reason);
        }
    }

    /// <summary>Best-effort, in order: sent once a channel is attached, otherwise held (bounded)
    /// until the next <see cref="Attach"/> flushes the backlog ahead of anything newer. Both paths run
    /// under the attach gate, so producing an event and placing it in the send sequence is one
    /// step -- concurrent callers cannot reorder each other, and neither can outrun the backlog.</summary>
    public void QueueLog(LogEvent log)
    {
        lock (_attachGate)
        {
            if (!_isAttached)
            {
                if (_logBacklog.Count == LogBacklogCapacity)
                {
                    _logBacklog.Dequeue();
                }

                _logBacklog.Enqueue(log);
                return;
            }

            _logChain = ChainLogAsync(_logChain, log);
        }
    }

    /// <summary>One link of the log sequence: never faults (<see cref="SendBestEffortAsync"/> swallows),
    /// so the chain can never be poisoned by a channel that closed under it.</summary>
    private async Task ChainLogAsync(Task previous, LogEvent log)
    {
        // Off the caller's stack first: this link is created while the attach gate is held, and the
        // send path takes that same gate. Ordering does not depend on the yield -- it comes from
        // awaiting the previous link, which completes only once its own send is done.
        await Task.Yield();
        await previous.ConfigureAwait(false);
        await SendBestEffortAsync(new HostChannelUp { Log = log }).ConfigureAwait(false);
    }

    /// <summary>Registers interest in a grant BEFORE the GateAcquire is sent, so a grant that arrives
    /// while the send is still in flight has somewhere to land.</summary>
    public Task RegisterGrant(string requestId)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingGrants[requestId] = tcs;
        return tcs.Task;
    }

    public void ForgetGrant(string requestId) => _pendingGrants.TryRemove(requestId, out _);

    public void OnGateGrant(string requestId)
    {
        if (_pendingGrants.TryRemove(requestId, out var tcs))
        {
            tcs.TrySetResult();
        }
    }

    public Task SendGateBudgetAsync(int remaining, DateTimeOffset resetAt) =>
        SendBestEffortAsync(new HostChannelUp
        {
            GateBudget = new GateBudget { Remaining = remaining, ResetAtUnixMs = resetAt.ToUnixTimeMilliseconds() },
        });

    public async Task SendAsync(HostChannelUp msg, CancellationToken ct)
    {
        TaskCompletionSource<IServerStreamWriter<HostChannelUp>> signal;
        lock (_attachGate)
        {
            // Checked explicitly, not left to _attached's own fault: TrySetException on an already-
            // RESOLVED TCS (the common shape -- Attach ran once, then Detach) is a no-op, since a TCS
            // cannot move from "has result" to faulted. A send issued after that point would otherwise
            // still retrieve the now-stale writer from the already-completed Task instead of failing.
            if (_closed)
            {
                throw _closedReason!;
            }

            signal = _attached;
        }

        // Buffer-until-attach, not a synchronous throw: a message queued (GateAcquire, or a log) before
        // HostChannel's server method has run Attach() waits here instead of failing the caller outright.
        // If nobody has attached yet when Close/Detach runs, THIS TCS is still unresolved, and
        // TrySetException there does unblock the wait -- the check above only covers the send issued
        // after the fact.
        var writer = await signal.Task.WaitAsync(ct).ConfigureAwait(false);

        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await writer.WriteAsync(msg).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task SendBestEffortAsync(HostChannelUp msg)
    {
        try
        {
            await SendAsync(msg, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Logging and budget hints are best-effort: a channel that closed between Attach and this
            // flush loses the line, same as any other logging path racing a shutdown.
        }
    }
}
