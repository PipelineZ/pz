using System.Runtime.InteropServices;

namespace Pz.Cli.Tests;

/// <summary>The stop signal a supervisor sends — SIGTERM from systemd, Docker, Kubernetes, Airflow —
/// must reach the run as the same cooperative cancel Ctrl-C is. These facts deliver a real signal to
/// the test process: while <see cref="StopSignals"/> is registered the runtime hands it to the handler
/// instead of terminating, and the wait on the token is the gate (no sleeps).
///
/// <para>A signal reaches every registration in the process, including the ones other tests' in-process
/// `pz run`s hold, so this class runs with nothing else in flight.</para></summary>
[Collection(StopSignalsCollection.Name)]
public sealed class StopSignalsTests
{
    private const int SigHup = 1;
    private const int SigTerm = 15;

    [DllImport("libc", EntryPoint = "kill", SetLastError = true)]
    private static extern int Kill(int pid, int signal);

    [SkippableTheory]
    [InlineData(SigTerm)]
    [InlineData(SigHup)]
    public async Task A_stop_signal_cancels_the_run_token_instead_of_killing_the_process(int signal)
    {
        Skip.If(OperatingSystem.IsWindows(), "delivers a POSIX signal to the test process");

        using var cts = new CancellationTokenSource();
        var cancelled = new TaskCompletionSource();
        using var onCancel = cts.Token.Register(() => cancelled.TrySetResult());
        using var signals = StopSignals.Register(cts);

        Assert.Equal(0, Kill(Environment.ProcessId, signal));

        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(cts.IsCancellationRequested);
    }

    [Fact]
    public void Disposing_unregisters_without_cancelling()
    {
        using var cts = new CancellationTokenSource();

        StopSignals.Register(cts).Dispose();

        Assert.False(cts.IsCancellationRequested);
    }
}

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class StopSignalsCollection
{
    public const string Name = "process-signals-serialized";
}
