using System.Runtime.InteropServices;

namespace Pz.Cli;

/// <summary>Everything that asks a run to stop, routed into one token: Ctrl-C from a terminal, and the
/// signals a supervisor sends instead — SIGTERM is the default stop signal of systemd, Docker,
/// Kubernetes and Airflow, SIGHUP what a closing terminal or SSH session sends. Left at their defaults
/// those kill the process outright: no cooperative cancel, no drained renderer, a run_results.json
/// stuck at "running", and connectors that never get to clean up.
///
/// The FIRST signal is marked handled, so the process ends by the run winding down — exit code 3, the
/// same as Ctrl-C always was — rather than by the runtime's default termination. A SECOND signal is
/// left unhandled and terminates the process: a run that will not wind down must never be a trap for
/// the operator or for a supervisor's stop sequence. SIGQUIT is deliberately left alone: it is how an
/// operator asks a stuck process for a dump.</summary>
internal sealed class StopSignals : IDisposable
{
    private readonly ConsoleCancelEventHandler _onCancelKey;
    private readonly List<PosixSignalRegistration> _registrations = [];
    private int _signalled;

    private StopSignals(CancellationTokenSource run)
    {
        // True for the first signal only. Cancel() on a disposed source throws, and a signal can arrive
        // after the run has finished but before Dispose below has unregistered.
        bool AbsorbAndStop()
        {
            if (Interlocked.Exchange(ref _signalled, 1) != 0)
            {
                return false;
            }

            try
            {
                run.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }

            return true;
        }

        _onCancelKey = (_, args) => args.Cancel = AbsorbAndStop();
        Console.CancelKeyPress += _onCancelKey;

        foreach (var signal in (ReadOnlySpan<PosixSignal>)[PosixSignal.SIGTERM, PosixSignal.SIGHUP])
        {
            _registrations.Add(PosixSignalRegistration.Create(signal, context => context.Cancel = AbsorbAndStop()));
        }
    }

    public static StopSignals Register(CancellationTokenSource run) => new(run);

    public void Dispose()
    {
        Console.CancelKeyPress -= _onCancelKey;
        foreach (var registration in _registrations)
        {
            registration.Dispose();
        }
    }
}
