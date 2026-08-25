using System.Collections.Concurrent;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The host side of the PCP reverse channel: one bidi <c>HostChannel</c> call per
/// <see cref="PcpClient"/>, opened after Configure. The connector is the semantic client of this
/// exchange (see the proto's own comment on why the RPC's request/response types are swapped) -- this
/// pump WRITES <c>GateGrant</c> (<see cref="HostChannelDown"/>, the host's own outbox) and READS
/// everything the connector sends (<see cref="HostChannelUp"/>): <c>GateAcquire</c>/<c>GateComplete</c>
/// drive a real <see cref="IOperationGate.ExecuteAsync{T}"/> call so retry/pacing policy stays entirely
/// host-side, <c>GateBudget</c> feeds <see cref="IOperationGate.ReportBudget"/>, and <c>LogEvent</c>
/// reaches <paramref name="logSink"/> (level, message, fields -- wired to <c>Pz.Diagnostics</c> connector
/// logging by whoever constructs this pump; never config values). <c>WriteAck</c>/<c>ReadState</c>
/// (checkpointing/sync-state) are read and ignored here -- out of scope for this pump, tracked for
/// whichever task wires <c>ICheckpointingPartition</c>/<c>ISyncStatePartition</c> through PCP.
///
/// <para>Lifetime is independent of the shims: <see cref="Start"/> takes an already-resolved
/// <see cref="IOperationGate"/> (the shim only HOLDS one, via <see cref="IOperationGateAware"/>, for
/// whoever constructs this pump to read), and <see cref="DisposeAsync"/> tears the pump down without
/// touching the shim or the underlying <see cref="PcpClient"/>/<see cref="ConnectorProcess"/> -- both
/// outlive it. Cancelling the pump's own lifetime (via <see cref="DisposeAsync"/>) ends the
/// <c>HostChannel</c> call quietly: no exception is thrown out of the pump loop, and every in-flight
/// gate wait fails with <see cref="OperationCanceledException"/> rather than hanging or surfacing as a
/// connector failure. A connector that dies (or otherwise ends the channel) while a gate request is
/// in flight fails that request with a transient <see cref="PzConnectorException"/> instead -- the
/// established <see cref="ProcessFailureMapping"/> pattern, stderr tail included -- so nothing is left
/// waiting forever.</para></summary>
public sealed class HostChannelPump : IAsyncDisposable
{
    private readonly PcpClient _client;
    private readonly ConnectorProcess _process;
    private readonly IOperationGate _gate;
    private readonly Action<int, string, IReadOnlyDictionary<string, string>>? _logSink;
    private readonly CancellationTokenSource _stopping = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<object?>> _pending = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<Task, byte> _inFlight = new();
    private readonly AsyncDuplexStreamingCall<HostChannelDown, HostChannelUp> _call;
    private readonly Task _pumpLoop;
    private int _disposed;

    private HostChannelPump(
        PcpClient client, ConnectorProcess process, IOperationGate gate,
        Action<int, string, IReadOnlyDictionary<string, string>>? logSink)
    {
        _client = client;
        _process = process;
        _gate = gate;
        _logSink = logSink;
        _call = client.Grpc.HostChannel(cancellationToken: _stopping.Token);
        _pumpLoop = Task.Run(PumpAsync);
    }

    /// <summary>Opens the reverse channel and starts pumping it in the background. Never throws for a
    /// connector that has nothing to say on it (the fixture's drain-only mode, or any connector that
    /// never declares <see cref="ConnectorCapabilities.GatedOperations"/>) -- the call simply sits idle
    /// until <see cref="DisposeAsync"/> ends it.</summary>
    public static HostChannelPump Start(
        PcpClient client, ConnectorProcess process, IOperationGate gate,
        Action<int, string, IReadOnlyDictionary<string, string>>? logSink = null) =>
        new(client, process, gate, logSink);

    private async Task PumpAsync()
    {
        try
        {
            await foreach (var msg in _call.ResponseStream.ReadAllAsync(_stopping.Token).ConfigureAwait(false))
            {
                Dispatch(msg);
            }

            // The connector ended its half of the channel on its own -- never expected on a live
            // connector (Task 5's fixture holds it open until ApplicationStopping/host shutdown) -- so
            // any gate request still waiting on THIS channel will never see its GateComplete.
            FailAllPending(ProcessFailureMapping.ToPzConnectorException(
                _client, _process, "connector ended the reverse channel while a gated operation was pending"));
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
            // Our own shutdown (DisposeAsync cancelled the pump's lifetime token) -- quiet, no PZ03xx.
            FailAllPendingWithCancellation();
        }
        catch (RpcException ex) when (_stopping.IsCancellationRequested && ex.StatusCode is StatusCode.Cancelled)
        {
            // Same as above: gRPC sometimes reports our own cancellation as a status code rather than
            // OperationCanceledException.
            FailAllPendingWithCancellation();
        }
        catch (RpcException ex)
        {
            FailAllPending(ProcessFailureMapping.MapControlPlane(_client, _process, ex, CancellationToken.None));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            FailAllPending(ex);
        }
    }

    private void Dispatch(HostChannelUp msg)
    {
        switch (msg.MsgCase)
        {
            case HostChannelUp.MsgOneofCase.GateAcquire:
                Track(HandleGateAcquireAsync(msg.GateAcquire));
                break;
            case HostChannelUp.MsgOneofCase.GateComplete:
                CompletePending(msg.GateComplete);
                break;
            case HostChannelUp.MsgOneofCase.GateBudget:
                SafeReportBudget(msg.GateBudget);
                break;
            case HostChannelUp.MsgOneofCase.Log:
                SafeLog(msg.Log);
                break;
            default:
                // WriteAck / ReadState / None: not this pump's concern (see class doc).
                break;
        }
    }

    /// <summary>Runs the real, host-side gate for one connector-reported operation: the gate's own
    /// <c>op</c> sends the matching <c>GateGrant</c> and waits for <c>GateComplete</c>, so a transient
    /// failure the connector reports becomes a <see cref="PzConnectorException"/> thrown FROM INSIDE the
    /// gate -- exactly what makes the engine's real retry/pacing policy (not the connector) decide
    /// whether to grant again. The gate's final outcome (success, or retries exhausted) has nowhere
    /// further to go over this channel -- the connector already knows its own per-attempt result from
    /// what it reported in each GateComplete -- so it is intentionally swallowed here.</summary>
    private async Task HandleGateAcquireAsync(GateAcquire acquire)
    {
        try
        {
            await _gate.ExecuteAsync<object?>(
                acquire.OpLabel,
                acquire.Idempotent,
                ct => GrantAndAwaitCompletionAsync(acquire.RequestId, ct),
                _stopping.Token).ConfigureAwait(false);
        }
        catch
        {
            // See summary above: nothing further to report over this channel.
        }
    }

    private async Task<object?> GrantAndAwaitCompletionAsync(string requestId, CancellationToken ct)
    {
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pending[requestId] = tcs;
        try
        {
            await SendDownAsync(new HostChannelDown { GateGrant = new GateGrant { RequestId = requestId } })
                .ConfigureAwait(false);
        }
        catch
        {
            _pending.TryRemove(requestId, out _);
            throw;
        }

        using var registration = ct.Register(
            static state => ((TaskCompletionSource<object?>)state!).TrySetCanceled(), tcs);
        return await tcs.Task.ConfigureAwait(false);
    }

    private void CompletePending(GateComplete complete)
    {
        if (!_pending.TryRemove(complete.RequestId, out var tcs))
        {
            // Unknown or already-resolved request id: a protocol anomaly, not worth tearing the pump
            // down over.
            return;
        }

        if (complete.TransientError is { } detail)
        {
            var retryAfter = detail.RetryAfterMs == 0 ? null : (TimeSpan?)TimeSpan.FromMilliseconds(detail.RetryAfterMs);
            tcs.TrySetException(new PzConnectorException(detail.Message, detail.IsTransient, retryAfter));
        }
        else
        {
            tcs.TrySetResult(null);
        }
    }

    private void SafeReportBudget(GateBudget budget)
    {
        try
        {
            _gate.ReportBudget(budget.Remaining, DateTimeOffset.FromUnixTimeMilliseconds(budget.ResetAtUnixMs));
        }
        catch
        {
            // Best-effort, mirrors IRunEvents' Safe* wrappers: a throwing gate must not break the pump.
        }
    }

    private void SafeLog(LogEvent log)
    {
        if (_logSink is null)
        {
            return;
        }

        try
        {
            _logSink(log.Level, log.Message, new Dictionary<string, string>(log.Fields, StringComparer.Ordinal));
        }
        catch
        {
            // Best-effort: a throwing log sink must not break the pump.
        }
    }

    private async Task SendDownAsync(HostChannelDown msg)
    {
        // IClientStreamWriter<T>.WriteAsync is not safe to call concurrently; several GateAcquire
        // handlers can be in flight together (nothing here limits concurrency across op labels).
        await _writeLock.WaitAsync(_stopping.Token).ConfigureAwait(false);
        try
        {
            await _call.RequestStream.WriteAsync(msg).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private void Track(Task task)
    {
        _inFlight[task] = 0;
        _ = task.ContinueWith(t => _inFlight.TryRemove(t, out _), TaskScheduler.Default);
    }

    private void FailAllPending(Exception ex)
    {
        foreach (var requestId in _pending.Keys)
        {
            if (_pending.TryRemove(requestId, out var tcs))
            {
                tcs.TrySetException(ex);
            }
        }
    }

    private void FailAllPendingWithCancellation()
    {
        foreach (var requestId in _pending.Keys)
        {
            if (_pending.TryRemove(requestId, out var tcs))
            {
                tcs.TrySetCanceled();
            }
        }
    }

    /// <summary>Idempotent. Ends the pump's lifetime token first (the quiet-shutdown path every catch
    /// clause above checks for), then joins the read loop and every still-running gate handler before
    /// releasing the call -- so nothing from this pump is still touching <c>client.Grpc</c> once this
    /// returns.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _stopping.CancelAsync().ConfigureAwait(false);

        try
        {
            await _pumpLoop.ConfigureAwait(false);
        }
        catch
        {
            // PumpAsync's own catch clauses already resolved every pending gate request; nothing
            // further to surface from the loop task itself.
        }

        try
        {
            await Task.WhenAll(_inFlight.Keys).ConfigureAwait(false);
        }
        catch
        {
            // HandleGateAcquireAsync never lets an exception escape (see its own catch-all); this is
            // defense in depth, not an expected path.
        }

        _call.Dispose();
        _stopping.Dispose();
        _writeLock.Dispose();
    }
}
