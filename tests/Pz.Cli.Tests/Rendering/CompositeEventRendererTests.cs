using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

public sealed class CompositeEventRendererTests
{
    private sealed class Recording : IEventRenderer
    {
        private readonly List<string>? _order;
        private readonly string? _name;

        public List<RunEvent> Seen { get; } = [];

        public Recording() { }

        public Recording(List<string> order, string name)
        {
            _order = order;
            _name = name;
        }

        public void Render(RunEvent evt)
        {
            Seen.Add(evt);
            if (_order != null && _name != null)
            {
                _order.Add(_name);
            }
        }
    }

    private sealed class Throwing : IEventRenderer
    {
        public void Render(RunEvent evt) => throw new InvalidOperationException("boom");
    }

    private static RunEvent Evt(string runId) =>
        new RunStartedEvent(DateTimeOffset.UnixEpoch, runId, "demo", 1);

    [Fact]
    public void Dispatches_to_every_renderer_in_order()
    {
        var order = new List<string>();
        var a = new Recording(order, "a");
        var b = new Recording(order, "b");
        var composite = new CompositeEventRenderer(a, b);

        composite.Render(Evt("r1"));

        Assert.Equal(["a", "b"], order);
    }

    [Fact]
    public void A_throwing_renderer_does_not_stop_the_others()
    {
        // Mirrors RendererPump's own per-event swallow: rendering is presentation-only and
        // best-effort, so one bad renderer must never suppress a good one.
        var good = new Recording();
        var composite = new CompositeEventRenderer(new Throwing(), good);

        composite.Render(Evt("r1"));

        Assert.Single(good.Seen);
    }

    [Fact]
    public void No_renderers_is_a_no_op()
    {
        new CompositeEventRenderer().Render(Evt("r1"));
    }
}
