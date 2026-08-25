using System.Diagnostics;
using System.Text;
using Pz.Connectors.Protocol;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>Owns one spawned connector child process end to end: the run-scoped socket directory it
/// listens on, its stderr for failure diagnostics, and the shutdown ladder that gets it (and anything
/// it forked) gone. One instance per connector instance; the control-plane handshake (Task 7's
/// <c>PcpClient</c>) and data-plane transfers (Task 8) both dial the sockets this type computes but
/// never own the process itself -- disposal here is the only place the child is killed.</summary>
public sealed class ConnectorProcess : IAsyncDisposable
{
    /// <summary>Env vars ever copied from the host into a connector child. Deliberately minimal:
    /// nothing here can carry a secret, and the child gets its actual configuration only through the
    /// Configure RPC -- never through the environment. Both-case proxy variants are included because
    /// tooling disagrees on casing and a connector may shell out to something that only honors one.</summary>
    private static readonly string[] EnvAllowlist =
    [
        "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL",
        "http_proxy", "https_proxy", "no_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY",
    ];

    /// <summary>Ring-buffer cap for captured stderr: enough for a real failure message plus a stack
    /// trace, small enough that a runaway connector logging forever cannot grow this unboundedly.</summary>
    private const int StderrTailCapBytes = 8 * 1024;

    private readonly Process _process;
    private readonly string _socketDir;
    private readonly object _stderrLock = new();
    private readonly StringBuilder _stderrTail = new();
    private readonly TaskCompletionSource _exitSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _disposed;

    private ConnectorProcess(Process process, string socketDir, string socketPath)
    {
        _process = process;
        _socketDir = socketDir;
        SocketPath = socketPath;
        DataSocketPath = socketPath + ProtocolConstants.DataSocketSuffix;
    }

    /// <summary>Path of the control-plane unix socket (named pipe on windows) the child was told to
    /// listen on via <c>--pz-socket</c>.</summary>
    public string SocketPath { get; }

    /// <summary>Data-plane socket path; always <see cref="SocketPath"/> plus
    /// <see cref="ProtocolConstants.DataSocketSuffix"/>, never computed independently by a caller.</summary>
    public string DataSocketPath { get; }

    /// <summary>True once the child is gone. Safe to read at any point in this instance's lifetime,
    /// including after <see cref="DisposeAsync"/>: that method's own contract is to have the process
    /// exited before it detaches the underlying <see cref="Process"/> handle, so once disposed this
    /// always reads true instead of forwarding to <see cref="Process.HasExited"/> on a handle that would
    /// throw <see cref="InvalidOperationException"/> rather than answer the question.</summary>
    public bool HasExited => Interlocked.CompareExchange(ref _disposed, 0, 0) != 0 || _process.HasExited;

    /// <summary>Last <see cref="StderrTailCapBytes"/> of the child's stderr, oldest-truncated. Meant
    /// for failure diagnostics attached to thrown exceptions -- there is no guarantee it captures a
    /// complete line at the front.</summary>
    public string StderrTail
    {
        get
        {
            lock (_stderrLock)
            {
                return _stderrTail.ToString();
            }
        }
    }

    /// <summary>Fires once, from the process's exit callback, after stderr has finished draining. Never
    /// fires twice and never fires for a process that never started (that path throws from
    /// <see cref="Spawn"/> instead).</summary>
    public event Action? Exited;

    /// <summary>Test-only escape hatch for asserting on the underlying OS pid; not part of the public
    /// ABI-facing surface (see <c>InternalsVisibleTo</c> in the csproj).</summary>
    internal int ProcessIdForTests => _process.Id;

    /// <summary>Creates the run-scoped socket directory (owner-only permissions), then spawns
    /// <paramref name="entrypointPath"/> with <c>--pz-socket &lt;SocketPath&gt;</c> and a minimal env
    /// allowlist. Throws <see cref="ConnectorHostException"/> PZ0355 if the entrypoint is missing, is
    /// not executable, or otherwise fails to start.
    ///
    /// <para><paramref name="extraArgs"/> is a test-only escape hatch, never used on the production
    /// spawn path (connection config crosses only through the Configure RPC -- see
    /// <c>Pz.PackageManagement.ProcessHosting.PcpClient</c> -- never argv): it exists so a test can
    /// select which failure a fixture connector stages via its own argv switches.</para></summary>
    public static ConnectorProcess Spawn(
        string entrypointPath, string socketDir, string packageName, IReadOnlyList<string>? extraArgs = null)
    {
        var createdSocketDir = CreateSocketDir(socketDir);
        var socketPath = Path.Combine(socketDir, "control.sock");

        var startInfo = new ProcessStartInfo
        {
            FileName = entrypointPath,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add("--pz-socket");
        startInfo.ArgumentList.Add(socketPath);
        if (extraArgs is not null)
        {
            foreach (var arg in extraArgs)
            {
                startInfo.ArgumentList.Add(arg);
            }
        }

        startInfo.EnvironmentVariables.Clear();
        foreach (var name in EnvAllowlist)
        {
            var value = Environment.GetEnvironmentVariable(name);
            if (value is not null)
            {
                startInfo.EnvironmentVariables[name] = value;
            }
        }

        Process process;
        try
        {
            process = Process.Start(startInfo) ?? throw new InvalidOperationException("Process.Start returned null");
        }
        catch (Exception ex)
        {
            if (createdSocketDir)
            {
                // Nothing to hand back to the caller when Spawn throws, so the socket dir this call
                // itself created would otherwise leak; a pre-existing dir is left alone since it
                // isn't ours to remove.
                try
                {
                    Directory.Delete(socketDir, recursive: true);
                }
                catch
                {
                    // best-effort; the PZ0355 below is the failure that matters to the caller
                }
            }

            throw new ConnectorHostException(
                "PZ0355",
                $"connector package '{packageName}' failed to start entrypoint '{entrypointPath}': {ex.Message}",
                "check the package's entrypoints for this platform");
        }

        var connectorProcess = new ConnectorProcess(process, socketDir, socketPath);
        connectorProcess.WireStreams();
        return connectorProcess;
    }

    /// <summary>Returns whether this call is the one that created <paramref name="socketDir"/> (as
    /// opposed to it already existing) -- <see cref="Spawn"/>'s failure path uses this to clean up
    /// only what it created.</summary>
    private static bool CreateSocketDir(string socketDir)
    {
        var alreadyExisted = Directory.Exists(socketDir);
        Directory.CreateDirectory(socketDir);
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                socketDir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }

        return !alreadyExisted;
    }

    private void WireStreams()
    {
        _process.EnableRaisingEvents = true;
        _process.ErrorDataReceived += (_, e) => AppendStderr(e.Data);
        _process.Exited += (_, _) =>
        {
            // WaitForExit drains any output still buffered by the OS pipe before we declare the
            // process (and its stderr capture) settled, so StderrTail is complete by the time
            // Exited fires and a caller reading it from the callback sees the final line.
            try
            {
                _process.WaitForExit();
            }
            catch
            {
                // best-effort; the process is already reported exited either way
            }

            _exitSignal.TrySetResult();
            Exited?.Invoke();
        };
        _process.BeginErrorReadLine();

        // Stdin is redirected (never inherited from this process's own console) but deliberately
        // left open: nothing in the protocol talks over stdin, so a child that happens to block on
        // it (e.g. this suite's noop.sh fixture) stays alive under host control until the kill
        // ladder below ends it, rather than exiting early on an incidental EOF.
    }

    private void AppendStderr(string? data)
    {
        if (data is null)
        {
            return;
        }

        lock (_stderrLock)
        {
            _stderrTail.Append(data).Append('\n');
            var excess = _stderrTail.Length - StderrTailCapBytes;
            if (excess > 0)
            {
                _stderrTail.Remove(0, excess);
            }
        }
    }

    /// <summary>Shutdown ladder's second rung: the caller sends the Shutdown RPC first (control-plane
    /// concern, outside this type), then awaits this for <paramref name="grace"/> before killing the
    /// whole process group. A process that exits on its own within the grace window short-circuits
    /// the kill.
    ///
    /// <para>Also safe to call after <see cref="DisposeAsync"/> already ran (e.g. a caller that killed
    /// this <see cref="ConnectorProcess"/> directly while <see cref="PcpClient"/> still held it, then let
    /// <see cref="PcpClient.DisposeAsync"/> run its own shutdown ladder over the same instance) --
    /// <see cref="HasExited"/> reads true once disposed rather than throwing on the detached handle, so
    /// this short-circuits exactly as it would for a process that was already confirmed gone.</para></summary>
    public async Task KillAfterGraceAsync(TimeSpan grace, CancellationToken ct)
    {
        if (HasExited)
        {
            return;
        }

        try
        {
            await _exitSignal.Task.WaitAsync(grace, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            KillProcessGroup();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            KillProcessGroup();
            throw;
        }
    }

    private void KillProcessGroup()
    {
        try
        {
            if (!_process.HasExited)
            {
                // net10's entireProcessTree walk covers the fixture's `sleep 300 &` child on both
                // unix (via /proc) and windows; a Job Object with kill-on-close would be sturdier
                // against a host crash between spawn and kill (kept as a hardening follow-up rather
                // than added speculatively here).
                _process.Kill(entireProcessTree: true);
            }
        }
        catch (InvalidOperationException)
        {
            // process already exited between the HasExited check and the kill call
        }
    }

    /// <summary>Idempotent. Always ends with a process-group kill (in case the caller skipped the
    /// graceful ladder) and deletes the run-scoped socket directory.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        KillProcessGroup();

        try
        {
            await _exitSignal.Task.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // the group kill above is best-effort at this point; proceed to cleanup regardless
        }

        _process.Dispose();

        try
        {
            if (Directory.Exists(_socketDir))
            {
                Directory.Delete(_socketDir, recursive: true);
            }
        }
        catch
        {
            // best-effort; a leftover socket dir is a cleanliness issue, not a correctness one
        }
    }
}
