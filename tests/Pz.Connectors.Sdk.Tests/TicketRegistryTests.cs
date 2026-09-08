using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class TicketRegistryTests
{
    [Fact]
    public void A_ticket_burns_exactly_once()
    {
        var registry = new TicketRegistry();
        var entry = new WriteTicket(new WriteSessionState("s1", "op1", new NullSession()), default);
        var ticket = registry.Mint(entry);

        Assert.Equal(16, ticket.Length);
        Assert.True(registry.TryBurn(ticket, out var first));
        Assert.Same(entry, first);
        Assert.False(registry.TryBurn(ticket, out _));
    }

    [Fact]
    public void A_ticket_of_the_wrong_length_never_resolves()
    {
        var registry = new TicketRegistry();
        registry.Mint(new WriteTicket(new WriteSessionState("s1", "op1", new NullSession()), default));
        Assert.False(registry.TryBurn(new byte[15], out _));
    }

    [Fact]
    public void Sync_state_capture_is_empty_until_completed_and_null_stays_empty()
    {
        var capture = new SyncStateCapture();
        Assert.False(capture.TryGet(out var before));
        Assert.Null(before);

        capture.Complete(null);
        Assert.False(capture.TryGet(out _));

        capture.Complete("0+4");
        Assert.True(capture.TryGet(out var token));
        Assert.Equal("0+4", token);
    }

    private sealed class NullSession : ISinkWriteSession
    {
        public ValueTask WriteBatchAsync(Apache.Arrow.RecordBatch batch, CancellationToken ct) => ValueTask.CompletedTask;
        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => ValueTask.FromResult(new WriteResult(0, 0));
        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
