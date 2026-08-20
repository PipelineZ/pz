using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>Fans one event out to an ordered list of renderers, so a persistence sink can coexist with
/// console/NDJSON output.
///
/// <see cref="RendererPump"/> takes exactly ONE renderer, and <see cref="RunEventBus"/> is deliberately
/// SingleReader — adding a second reader would give up an optimization the bus relies on. Composing at
/// the renderer level instead means neither has to change.
///
/// A throwing renderer is swallowed per renderer, matching the pump's own per-event swallow: rendering
/// is presentation-only and best-effort, so one bad renderer must not suppress a good one.
///
/// <see cref="DisposeAsync"/> forwards to every child that owns disposable resources (e.g.
/// <c>LiveTreeRenderer</c>'s <see cref="IDisposable"/>, or a
/// <c>SqlEventRenderer</c>'s <see cref="IAsyncDisposable"/> sink) -- composing a disposable renderer
/// behind this class must not silently drop its cleanup. Same best-effort discipline as
/// <see cref="Render"/>: one child's disposal failure must not stop the others from disposing.</summary>
public sealed class CompositeEventRenderer(params IEventRenderer[] renderers) : IEventRenderer, IAsyncDisposable
{
    public void Render(RunEvent evt)
    {
        foreach (var renderer in renderers)
        {
            try
            {
                renderer.Render(evt);
            }
            catch
            {
                // Best-effort, per renderer — see class doc.
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var renderer in renderers)
        {
            try
            {
                switch (renderer)
                {
                    case IAsyncDisposable asyncDisposable:
                        await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                        break;
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }
            catch
            {
                // Best-effort, per renderer — see class doc.
            }
        }
    }
}
