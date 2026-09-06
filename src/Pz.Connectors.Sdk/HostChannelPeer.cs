using System.Collections.Concurrent;
using Grpc.Core;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>The connector side of the PCP reverse channel. One instance per process, reused across
/// however many <c>HostChannel</c> calls the host makes (there is normally exactly one, held open for
/// the process's lifetime).
///
/// <para>Every outgoing message -- gate traffic and logs alike -- goes through one
/// buffer-until-attach mechanism: a TCS that resolves once a HostChannel call is actually being
/// served, replaced with a fresh unresolved one on <see cref="Detach"/> so a message queued after the
/// channel drops waits for the NEXT attach rather than racing a disposed writer.</para></summary>
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

    private TaskCompletionSource<IServerStreamWriter<HostChannelUp>> _attached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Called once per <c>HostChannel</c> RPC, right as it starts serving. Unblocks every send
    /// (log or gate) that was already waiting on this channel to open, then flushes the held logs.</summary>
    public void Attach(IServerStreamWriter<HostChannelUp> writer)
    {
        TaskCompletionSource<IServerStreamWriter<HostChannelUp>> signal;
        LogEvent[] backlog;
        lock (_attachGate)
        {
            signal = _attached;
            _isAttached = true;
            backlog = _logBacklog.ToArray();
            _logBacklog.Clear();
        }

        signal.TrySetResult(writer);
        foreach (var log in backlog)
        {
            _ = SendBestEffortAsync(new HostChannelUp { Log = log });
        }
    }

    public void Detach()
    {
        lock (_attachGate)
        {
            _isAttached = false;
            _attached = new TaskCompletionSource<IServerStreamWriter<HostChannelUp>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    /// <summary>Best-effort, in order: sent now if a channel is attached, otherwise held (bounded)
    /// until the next <see cref="Attach"/> flushes the backlog ahead of anything newer.</summary>
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
        }

        _ = SendBestEffortAsync(new HostChannelUp { Log = log });
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
            signal = _attached;
        }

        // Buffer-until-attach, not a synchronous throw: a message queued (GateAcquire, or a log) before
        // HostChannel's server method has run Attach() waits here instead of failing the caller outright.
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
