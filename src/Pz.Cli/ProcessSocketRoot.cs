using Pz.Engine.Execution;

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

        // Short on purpose: this is the fallback for a project directory that was already too deep, so
        // spending path budget on a descriptive name would defeat it.
        var temp = Path.Combine(
            Path.GetTempPath(), "pz-" + Guid.NewGuid().ToString("N")[..8]);
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
}
