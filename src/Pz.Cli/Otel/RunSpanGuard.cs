using System.Diagnostics;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Otel;

/// <summary>Gives the run span its outcome. A run that reaches <see cref="Complete"/> carries its status and node
/// counts; one that leaves the run method any other way (an early config-error return, cancellation during setup,
/// an exception) is marked Error naming the phase it ended in. Declare it after the run span so it is disposed
/// first, while the span is still open.</summary>
internal sealed class RunSpanGuard(Activity? run, RunPhaseClock phases) : IDisposable
{
    private bool _completed;

    public void Complete(string status, int succeeded, int failed, int skipped)
    {
        _completed = true;
        if (run is null) return;
        run.SetTag("pz.run.status", status);
        run.SetTag("pz.run.nodes.succeeded", succeeded);
        run.SetTag("pz.run.nodes.failed", failed);
        run.SetTag("pz.run.nodes.skipped", skipped);
        run.SetTag("pz.run.node_count", succeeded + failed + skipped);
        if (status != "success") run.SetStatus(ActivityStatusCode.Error, status);
    }

    public void Dispose()
    {
        if (_completed || run is null) return;
        // An early return disposes the phase scope before this guard, so the phase it ended in is the last begun.
        var phase = phases.Current ?? phases.LastBegun;
        run.SetStatus(ActivityStatusCode.Error, phase is null ? "run ended before completing" : $"run ended during {phase}");
    }
}
