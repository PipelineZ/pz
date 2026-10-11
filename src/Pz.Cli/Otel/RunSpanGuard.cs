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
        if (run is null) return;
        var current = phases.Current;
        // Complete runs before finalize; a finalize scope still open here means finalize did not finish.
        if (_completed && current != RunPhaseClock.Finalize) return;
        // A phase scope still open names where the run ended; otherwise the run left between phases (state
        // backend check, session open), so name the last phase that finished rather than the next one.
        var message = current is not null ? $"run ended during {current}"
            : phases.LastBegun is { } last ? $"run ended after {last}"
            : "run ended before completing";
        run.SetStatus(ActivityStatusCode.Error, message);
    }
}
