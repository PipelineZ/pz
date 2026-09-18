using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Pins the fix for the gate-completion leak: a cancelled gated operation must still send
/// <c>GateComplete</c>, because that message is the only thing that releases the host's real
/// <c>IOperationGate.ExecuteAsync</c> permit (see <c>HostChannelPump.GrantAndAwaitCompletionAsync</c>
/// on the host side, which this test does not construct -- only the connector side is under test).</summary>
public sealed class HostOperationGateTests
{
    [Fact]
    public async Task GateComplete_is_sent_after_the_gated_operation_is_cancelled()
    {
        var peer = new HostChannelPeer();
        var writer = new ListStreamWriter<HostChannelUp>();
        peer.Attach(writer);
        var gate = new HostOperationGate(peer);

        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource();

        var executing = gate.ExecuteAsync<object?>(
            "op-label",
            idempotent: false,
            async ct2 =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.Infinite, ct2).ConfigureAwait(false);
                return null;
            },
            cts.Token);

        // The GateAcquire send and the wait-for-grant registration both run synchronously inside
        // ExecuteAsync before its first real suspension point (granted.WaitAsync on a TCS nobody has
        // resolved yet), so by the time control returns here the message is already in the list --
        // no polling needed.
        var acquire = Assert.Single(writer.Written, static m => m.MsgCase == HostChannelUp.MsgOneofCase.GateAcquire);
        peer.OnGateGrant(acquire.GateAcquire.RequestId);

        // Deterministic handoff to the op body: it always sets `started` before awaiting cancellation.
        await started.Task;
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => executing);

        var complete = Assert.Single(writer.Written, static m => m.MsgCase == HostChannelUp.MsgOneofCase.GateComplete);
        Assert.Equal(acquire.GateAcquire.RequestId, complete.GateComplete.RequestId);
        Assert.NotNull(complete.GateComplete.TransientError);
        Assert.False(complete.GateComplete.TransientError.IsTransient);
    }

    /// <summary>Pins the fix for the hang this class's own doc warns against: GateComplete's send is
    /// best-effort on <c>CancellationToken.None</c>, so before the fix a channel that resets (Detach)
    /// AFTER the grant but BEFORE the completion goes out left <c>ExecuteAsync</c> waiting on a TCS
    /// nothing would ever resolve -- forever, since nothing could cancel it. The operation itself has
    /// already produced its result by the time Detach runs (from inside the operation body, so the
    /// sequencing is exact, not timing-dependent); this proves the surrounding gate does not swallow
    /// that result behind an unsendable acknowledgement.</summary>
    [Fact]
    public async Task ExecuteAsync_returns_the_result_when_the_channel_resets_before_GateComplete_can_be_sent()
    {
        var peer = new HostChannelPeer();
        var writer = new ListStreamWriter<HostChannelUp>();
        peer.Attach(writer);
        var gate = new HostOperationGate(peer);

        var executing = gate.ExecuteAsync(
            "op-label",
            idempotent: false,
            ct2 =>
            {
                // The grant has already resolved by the time this runs (ExecuteAsync awaits it before
                // calling op), so the channel resetting here reproduces "reset after grant, before
                // GateComplete" without any timing dependence.
                peer.Detach();
                return Task.FromResult(42);
            },
            CancellationToken.None);

        var acquire = Assert.Single(writer.Written, static m => m.MsgCase == HostChannelUp.MsgOneofCase.GateAcquire);
        peer.OnGateGrant(acquire.GateAcquire.RequestId);

        var result = await executing.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(42, result);
    }

    /// <summary>Same hang, from the OTHER side: the channel never attaches at all before the
    /// operation's own token is cancelled. GateAcquire's own send already honoured <c>ct</c>
    /// (unaffected by this fix), so this pins that a never-attached peer does not somehow bypass that
    /// and hang regardless.</summary>
    [Fact]
    public async Task ExecuteAsync_is_cancellable_when_the_channel_never_attaches()
    {
        var peer = new HostChannelPeer();
        var gate = new HostOperationGate(peer);
        using var cts = new CancellationTokenSource();

        var executing = gate.ExecuteAsync<object?>(
            "op-label", idempotent: false, _ => Task.FromResult<object?>(null), cts.Token);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => executing.WaitAsync(TimeSpan.FromSeconds(10)));
    }
}
