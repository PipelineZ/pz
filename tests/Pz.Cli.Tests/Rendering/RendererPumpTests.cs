using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

public class RendererPumpTests
{
    private sealed class RecordingRenderer : IEventRenderer
    {
        public readonly List<RunEvent> Received = [];
        public void Render(RunEvent evt) => Received.Add(evt);
    }

    private sealed class ThrowingRenderer : IEventRenderer
    {
        public int Calls;
        public void Render(RunEvent evt)
        {
            Calls++;
            throw new InvalidOperationException("boom");
        }
    }

    [Fact]
    public async Task Dispatches_events_in_publish_order_and_completes_after_bus_completes()
    {
        var bus = new RunEventBus();
        var renderer = new RecordingRenderer();
        var pump = new RendererPump(bus, renderer);

        bus.Publish(new RunStartedEvent(DateTimeOffset.UtcNow, "run-1", "hello_pz", 1));
        bus.Publish(new RunCompletedEvent(DateTimeOffset.UtcNow, "run-1", "success", 1, 0, 0, 5));
        bus.Complete();

        await pump.Completion.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, renderer.Received.Count);
        Assert.IsType<RunStartedEvent>(renderer.Received[0]);
        Assert.IsType<RunCompletedEvent>(renderer.Received[1]);
    }

    [Fact]
    public async Task Renderer_exception_does_not_stop_draining_later_events()
    {
        var bus = new RunEventBus();
        var renderer = new ThrowingRenderer();
        var pump = new RendererPump(bus, renderer);

        bus.Publish(new RunStartedEvent(DateTimeOffset.UtcNow, "run-1", "hello_pz", 1));
        bus.Publish(new RunCompletedEvent(DateTimeOffset.UtcNow, "run-1", "success", 1, 0, 0, 5));
        bus.Complete();

        await pump.Completion.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, renderer.Calls);
    }

    [Fact]
    public async Task Completion_does_not_finish_before_bus_completes()
    {
        var bus = new RunEventBus();
        var renderer = new RecordingRenderer();
        var pump = new RendererPump(bus, renderer);

        bus.Publish(new RunStartedEvent(DateTimeOffset.UtcNow, "run-1", "hello_pz", 1));

        var completedEarly = pump.Completion.IsCompleted;
        Assert.False(completedEarly);

        bus.Complete();
        await pump.Completion.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
