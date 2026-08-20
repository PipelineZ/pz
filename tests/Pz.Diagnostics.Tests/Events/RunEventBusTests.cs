using Pz.Diagnostics.Events;

namespace Pz.Diagnostics.Tests.Events;

/// <summary>Bus mechanics only: TryWrite-never-blocks publish over an unbounded
/// single-reader channel, in-order single-reader fan-out, and idempotent completion.</summary>
public class RunEventBusTests
{
    private static RunStartedEvent Evt(int n) =>
        new(DateTimeOffset.UnixEpoch, "run-1", $"project-{n}", n);

    [Fact]
    public async Task Publish_preserves_order()
    {
        var bus = new RunEventBus();
        for (var i = 0; i < 100; i++)
        {
            Assert.True(bus.Publish(Evt(i)));
        }

        bus.Complete();

        var received = new List<RunEvent>();
        await foreach (var evt in bus.ReadAllAsync())
        {
            received.Add(evt);
        }

        Assert.Equal(100, received.Count);
        for (var i = 0; i < 100; i++)
        {
            Assert.Equal($"project-{i}", Assert.IsType<RunStartedEvent>(received[i]).ProjectName);
        }
    }

    [Fact]
    public void Publish_never_blocks()
    {
        var bus = new RunEventBus();

        // No reader is ever attached: an unbounded channel's TryWrite must return immediately
        // (true) for every call, proving a slow/absent renderer can never stall a publisher.
        for (var i = 0; i < 10_000; i++)
        {
            Assert.True(bus.Publish(Evt(i)));
        }
    }

    [Fact]
    public async Task Complete_ends_enumeration()
    {
        var bus = new RunEventBus();
        bus.Publish(Evt(1));
        bus.Complete();

        var received = new List<RunEvent>();
        await foreach (var evt in bus.ReadAllAsync())
        {
            received.Add(evt);
        }

        Assert.Single(received);
    }

    [Fact]
    public void Publish_after_complete_returns_false()
    {
        var bus = new RunEventBus();
        bus.Complete();

        Assert.False(bus.Publish(Evt(1)));
    }

    [Fact]
    public void Complete_is_idempotent()
    {
        var bus = new RunEventBus();
        bus.Complete();
        bus.Complete(); // must not throw
    }

    /// <summary>Structural proof — no wall-clock anywhere — that a slow renderer cannot stall a publisher.
    /// The reader is blocked on a <see cref="TaskCompletionSource"/> gate right after consuming event 0 —
    /// never draining anything further — so every one of the 199 subsequent publishes lands purely in the
    /// channel's unbounded buffer with zero consumption racing it. Proof is (a) every
    /// <see cref="RunEventBus.Publish"/> call still returns <see langword="true"/>, and
    /// (b) <see cref="RunEventBus.PendingCountForTests"/> reads back exactly 199 — the channel actually
    /// holds everything, not just "returned true". Only after that assertion is the gate released, letting
    /// the reader drain the rest and prove full in-order delivery.</summary>
    [Fact]
    public async Task Publish_never_blocks_behind_a_slow_renderer()
    {
        var bus = new RunEventBus();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var readerStarted = new SemaphoreSlim(0);

        var received = new List<RunEvent>();
        var reader = Task.Run(async () =>
        {
            var seenFirst = false;
            await foreach (var evt in bus.ReadAllAsync())
            {
                received.Add(evt);
                if (!seenFirst)
                {
                    seenFirst = true;
                    readerStarted.Release();
                    await gate.Task; // blocked here until the test explicitly releases it below.
                }
            }
        });

        // Publish the first event and wait for the reader to pick it up and park on the gate, so the
        // remaining publishes below race a reader that is guaranteed to never consume anything further
        // until released.
        Assert.True(bus.Publish(Evt(0)));
        await readerStarted.WaitAsync();

        for (var i = 1; i < 200; i++)
        {
            Assert.True(bus.Publish(Evt(i)));
        }

        // Structural proof: the reader is still parked on the gate (nothing consumed since event 0), so
        // the channel must be holding exactly the 199 events just published.
        Assert.Equal(199, bus.PendingCountForTests);

        gate.SetResult();
        bus.Complete();
        await reader;

        Assert.Equal(200, received.Count);
        for (var i = 0; i < 200; i++)
        {
            Assert.Equal($"project-{i}", Assert.IsType<RunStartedEvent>(received[i]).ProjectName);
        }
    }
}
