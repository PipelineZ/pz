using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Pins the fix for the hang <c>HostChannelPeer</c>'s own class doc warns against: before the
/// fix, a peer that closed (Detach, or the host never attaching at all) left every buffered send
/// waiting on a TCS nothing would ever resolve -- and <see cref="HostChannelPeer.SendBestEffortAsync"/>
/// sends on <c>CancellationToken.None</c>, so nothing could even cancel it out.</summary>
public sealed class HostChannelPeerTests
{
    [Fact]
    public async Task SendAsync_fails_immediately_once_the_peer_has_detached_instead_of_waiting_for_reattach()
    {
        var peer = new HostChannelPeer();
        var writer = new ListStreamWriter<HostChannelUp>();
        peer.Attach(writer);
        peer.Detach();

        // No attach is ever coming again (the host calls HostChannel exactly once); a send issued
        // AFTER Detach must fail fast rather than buffer on a channel that cannot reopen.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => peer.SendAsync(new HostChannelUp(), CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task SendBestEffortAsync_returns_once_the_peer_has_detached_instead_of_hanging_forever()
    {
        var peer = new HostChannelPeer();
        var writer = new ListStreamWriter<HostChannelUp>();
        peer.Attach(writer);
        peer.Detach();

        // Best-effort: swallows the failure, but must still RETURN -- this is the exact shape
        // HostOperationGate.ExecuteAsync depends on for GateComplete.
        await peer.SendBestEffortAsync(new HostChannelUp()).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task SendBestEffortAsync_returns_when_the_channel_never_attached_and_the_process_is_stopping()
    {
        var peer = new HostChannelPeer();

        // Nobody ever called Attach: the "never attaches" half of the bug. Close() is what
        // PcpServer wires to IHostApplicationLifetime.ApplicationStopping for exactly this case.
        peer.Close(new PzConnectorException("connector process is stopping", isTransient: false));

        await peer.SendBestEffortAsync(new HostChannelUp()).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Attach_after_Close_is_a_no_op_rather_than_reopening_a_closed_peer()
    {
        var peer = new HostChannelPeer();
        peer.Close(new PzConnectorException("closed", isTransient: false));

        var writer = new ListStreamWriter<HostChannelUp>();
        peer.Attach(writer);

        // Still closed: a send still fails fast instead of finding the late writer live.
        await Assert.ThrowsAsync<PzConnectorException>(
            () => peer.SendAsync(new HostChannelUp(), CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Empty(writer.Written);
    }
}
