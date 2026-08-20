using System.Diagnostics;

namespace Pz.TestSupport;

/// <summary>Runs a `dotnet` child with redirected stdout/stderr, hermetically and machine-serialized.
///
/// Hermetic: persistent build children (MSBuild nodeReuse workers, the Roslyn VBCSCompiler server)
/// inherit the redirected pipe handles and hold them open after the direct child exits, so ReadToEnd()
/// never sees EOF and the calling fixture hangs its whole test assembly. The env
/// var set here kills MSBuild worker reuse for the child; build/pack call sites must ALSO append
/// <see cref="BuildArgs"/> so neither an MSBuild worker nor a Roslyn compiler server is spawned at all.
///
/// Machine-serialized: the consuming test assemblies run as parallel processes and build/pack fixture
/// projects whose obj/bin intermediates are shared (directly, or via ProjectReference — e.g. every
/// connector-host fixture builds Pz.Connectors.Abstractions' Debug intermediates), so unserialized
/// children race on MSBuild intermediate files. Every Run holds a
/// machine-wide named mutex — cross-process on both CI OSes in modern .NET. The generous timeout is a
/// deadlock backstop, not a performance assertion: the last waiter in a cold-CI gate queues behind a
/// dozen-plus serialized hermetic (full cold-start) builds.</summary>
public static class HermeticDotnet
{
    /// <summary>Append to every `dotnet build`/`dotnet pack` child's arguments (leading space included).</summary>
    public const string BuildArgs = " /nodeReuse:false -p:UseSharedCompilation=false";

    private const string MutexName = "pz-fixture-pack";

    private static readonly TimeSpan LockTimeout = TimeSpan.FromMinutes(10);

    public static (string Stdout, string Stderr) Run(string arguments, string label)
    {
        using var mutex = new Mutex(initiallyOwned: false, MutexName);
        var owned = false;
        try
        {
            try
            {
                owned = mutex.WaitOne(LockTimeout);
            }
            catch (AbandonedMutexException)
            {
                // A prior holder's process died mid-run; the mutex is ours now and the fixture
                // projects' intermediates are rebuilt from scratch by our own child anyway.
                owned = true;
            }

            if (!owned)
            {
                throw new InvalidOperationException(
                    $"machine-wide dotnet-child lock '{MutexName}' not acquired within {LockTimeout} for {label}");
            }

            var psi = new ProcessStartInfo("dotnet", arguments) { RedirectStandardOutput = true, RedirectStandardError = true };
            psi.Environment["MSBUILDDISABLENODEREUSE"] = "1";
            using var proc = Process.Start(psi)!;

            // Drain stderr concurrently with the stdout read: sequential ReadToEnd() on two redirected
            // pipes can deadlock if the child fills the un-drained stderr buffer (small on Windows)
            // while the parent is still blocked on stdout.
            var stderrTask = proc.StandardError.ReadToEndAsync();
            var stdout = proc.StandardOutput.ReadToEnd();
            var stderr = stderrTask.GetAwaiter().GetResult();
            proc.WaitForExit();
            if (proc.ExitCode != 0)
            {
                throw new InvalidOperationException($"{label} failed:\n{stdout}\n{stderr}");
            }

            return (stdout, stderr);
        }
        finally
        {
            if (owned)
            {
                mutex.ReleaseMutex();
            }
        }
    }
}
