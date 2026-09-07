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
}
