using System.Diagnostics;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Otel;

/// <summary>Gives the run span its outcome. A run that reaches <see cref="Complete"/> carries its status and node
/// counts; <see cref="Finish"/> marks the run as having reached its terminal snapshot. A run that leaves the run
/// method without calling <see cref="Finish"/> (an early config-error return, cancellation, an exception in any
/// phase including finalize) is marked Error naming the phase it began last. Declare it right after the run span
/// and before any phase scope, so it is disposed last.</summary>
internal sealed class RunSpanGuard(Activity? run, RunPhaseClock phases) : IDisposable
{
    private bool _finished;

    public void Complete(string status, int succeeded, int failed, int skipped)
    {
        if (run is null) return;
        run.SetTag("pz.run.status", status);
        run.SetTag("pz.run.nodes.succeeded", succeeded);
        run.SetTag("pz.run.nodes.failed", failed);
        run.SetTag("pz.run.nodes.skipped", skipped);
        run.SetTag("pz.run.node_count", succeeded + failed + skipped);
        if (status != "success") run.SetStatus(ActivityStatusCode.Error, status);
    }

    /// <summary>The run reached its terminal snapshot; disposal leaves the status <see cref="Complete"/> set.</summary>
    public void Finish() => _finished = true;

    public void Dispose()
    {
        if (run is null || _finished) return;
        run.SetStatus(ActivityStatusCode.Error,
            phases.LastBegun is { } last ? $"run ended in {last}" : "run ended before completing");
    }
}
