using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>Single reader task over a <see cref="RunEventBus"/>: reads events in publish order and
/// dispatches each one to a chosen <see cref="IEventRenderer"/>. Started once per run (composition root:
/// <c>RunCommand.ExecuteRun</c>); <see cref="Completion"/> finishes once the bus's writer completes
/// (fired after <c>RunCompletedEvent</c>) and every already-buffered event has been rendered — the
/// caller awaits it with a 5-second drain timeout so a hung renderer can never hang the
/// process. A renderer that throws is swallowed per event so one bad render doesn't stop later events
/// from draining (rendering is presentation-only, same best-effort spirit as <c>IRunEvents</c>).</summary>
public sealed class RendererPump
{
    private readonly Task _completion;

    public RendererPump(RunEventBus bus, IEventRenderer renderer, CancellationToken ct = default)
    {
        _completion = PumpAsync(bus, renderer, ct);
    }

    public Task Completion => _completion;

    private static async Task PumpAsync(RunEventBus bus, IEventRenderer renderer, CancellationToken ct)
    {
        await foreach (var evt in bus.ReadAllAsync(ct).ConfigureAwait(false))
        {
            try
            {
                renderer.Render(evt);
            }
            catch
            {
                // Best-effort: rendering must never stop the drain of remaining events.
            }
        }
    }
}
