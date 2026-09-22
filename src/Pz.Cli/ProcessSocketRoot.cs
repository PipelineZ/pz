using System.Diagnostics;
using System.Globalization;
using Pz.Engine.Execution;
using Pz.PackageManagement.Hosting;
using Pz.Core.Validation;

namespace Pz.Cli;

/// <summary>Chooses the directory a <c>ProcessConnectorHost</c> gives each spawned connector its own
/// owner-only socket directory under.
///
/// <para>Run-scoped (<c>.pz/runs/&lt;id&gt;/sockets</c>) whenever a run exists, so a crashed run's
/// leftovers are collected with the run rather than accumulating in the system temp directory. A verb
/// with no run (<c>pz validate</c>, <c>pz plan</c>, <c>pz connectors</c>, <c>pz mcp</c>) still opens
/// connectors, so it gets a temp directory the caller owns and deletes.</para></summary>
internal static class ProcessSocketRoot
{
    /// <summary>Longest root that can still serve a connector. A unix domain socket's path lives in
    /// <c>sockaddr_un.sun_path</c>: 108 bytes on Linux, 104 on macOS, NUL included. Under the root
    /// returned here <c>ConnectorProcess</c> appends <c>/pcp-XXXXXXXX/control.sock</c> and the data
    /// plane a further <c>.data</c> — 31 bytes — so a longer root cannot bind at all, and the failure
    /// would surface from inside the child as an unexplained bind error rather than as anything a user
    /// could act on. Budgeted against the smaller (macOS) limit so one rule covers both.</summary>
    private const int MaxRootLength = 104 - 1 - 31;

    /// <summary>Guards <see cref="SweepStaleRoots"/> to run at most once per process from
    /// <see cref="Resolve"/> -- "at startup", not on every runless verb's own call.</summary>
    private static int _swept;

    /// <summary><paramref name="runId"/> null (or a run-scoped root too long to serve a socket) selects
    /// the temp route. <c>Owned</c> true means the caller created the directory and must delete it;
    /// false means it belongs to the run directory.</summary>
    public static (string Root, bool Owned) Resolve(string projectDir, string? runId)
    {
        if (runId is { Length: > 0 })
        {
            var runScoped = Path.Combine(new RunPaths(projectDir, runId).RunDir, "sockets");
            if (runScoped.Length <= MaxRootLength)
            {
                return (runScoped, false);
            }
        }

        var tempRoot = Path.GetTempPath();
        if (Interlocked.Exchange(ref _swept, 1) == 0)
        {
            // Only this (temp, not run-scoped) route can leak: a run-scoped root is collected with its
            // run directory regardless of how the process ends, but a temp root has nothing else that
            // ever cleans it up, so a killed process (no chance to reach the caller's own delete)
            // leaves it behind forever. Best-effort and never blocks minting this call's own root.
            SweepStaleRoots(tempRoot);
        }

        // Short on purpose: this is the fallback for a project directory that was already too deep, so
        // spending path budget on a descriptive name would defeat it. The pid segment is what lets a
        // leaked root be attributed to the pz process that minted it — concurrent pz processes (or a
        // test asserting on its own additions) can otherwise not tell whose root is whose.
        var temp = Path.Combine(tempRoot, $"pz-{Environment.ProcessId}-" + Guid.NewGuid().ToString("N")[..8]);
        if (temp.Length > MaxRootLength)
        {
            // The run-scoped root already failed this same budget above, and now the fallback -- whose
            // own name is as short as the scheme allows -- fails it too, which only happens when
            // Path.GetTempPath() itself (TMPDIR/TEMP) is unusually deep. Refused before
            // Directory.CreateDirectory, so there is nothing to leak: a bind failure surfacing from
            // inside the spawned child would give the user nothing to act on, so this is caught at the
            // one point that can still name the cause.
            throw new ConnectorHostException(PzErrorCode.ConnectorSpawnFailed,
                $"temp directory '{Path.GetTempPath()}' is too deep to host a connector's control socket " +
                $"(a unix domain socket path is capped around 104 bytes)",
                "point TMPDIR (or TEMP on Windows) at a shorter directory and retry");
        }

        Directory.CreateDirectory(temp);
        if (!OperatingSystem.IsWindows())
        {
            // Each instance directory under this root is created 0700 by ConnectorProcess; narrowing the
            // shared parent as well keeps a world-writable /tmp from being the only thing between
            // another local user and a connector's socket directory.
            File.SetUnixFileMode(
                temp, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }

        return (temp, true);
    }

    /// <summary>Deletes every <c>pz-&lt;pid&gt;-&lt;8 hex&gt;</c> directory directly under
    /// <paramref name="tempRoot"/> whose <c>pid</c> names a process that is no longer running. A pid
    /// that is still alive is never touched -- including this process's own, and including a pid the OS
    /// has since reused for an unrelated process, since either way a live process might still be serving
    /// sockets under that root. Internal (not private) and taking the root explicitly so a test can
    /// point it at a throwaway directory instead of the real system temp directory.
    ///
    /// <para>Best-effort throughout: a directory that fails to delete (already gone, raced by another
    /// sweep, or -- on a shared multi-user temp directory -- owned by a different user and therefore not
    /// even ours to remove) is skipped rather than treated as a sweep failure. This is housekeeping for
    /// roots an earlier <c>pz</c> process could never clean up itself (killed, not merely exited), never
    /// a correctness gate for the caller minting its own root.</para></summary>
    internal static void SweepStaleRoots(string tempRoot)
    {
        IEnumerable<string> candidates;
        try
        {
            candidates = Directory.EnumerateDirectories(tempRoot, "pz-*");
        }
        catch
        {
            return; // temp root unreadable or gone; nothing to sweep
        }

        foreach (var dir in candidates)
        {
            if (!TryParseOwningPid(Path.GetFileName(dir), out var pid) || IsAlive(pid))
            {
                continue;
            }

            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // best-effort, see the method doc
            }
        }
    }

    /// <summary>Parses the <c>&lt;pid&gt;</c> segment out of a <c>pz-&lt;pid&gt;-&lt;8 hex&gt;</c>
    /// directory name (see the name minted in <see cref="Resolve"/> above). Anything that does not fit
    /// that exact shape is not one of this scheme's roots and is left alone rather than guessed at.</summary>
    private static bool TryParseOwningPid(string dirName, out int pid)
    {
        pid = 0;
        var rest = dirName["pz-".Length..];
        var dash = rest.LastIndexOf('-');
        if (dash <= 0 || rest.Length - dash - 1 != 8)
        {
            return false;
        }

        return int.TryParse(
            rest[..dash], NumberStyles.None, CultureInfo.InvariantCulture, out pid) && pid > 0;
    }

    /// <summary>True unless <paramref name="pid"/> is provably not a running process. Anything short of
    /// that proof (a permission failure reading another user's process, a transient error) is treated as
    /// "might still be alive" -- the safe direction for a sweep that must never delete a live process's
    /// socket directory.</summary>
    private static bool IsAlive(int pid)
    {
        try
        {
            using var process = Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            // GetProcessById's documented contract for "no such process is running": the one case this
            // sweep can call proven-dead.
            return false;
        }
        catch
        {
            return true;
        }
    }
}
