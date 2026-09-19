using System.Diagnostics;
using System.Runtime.Versioning;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="ConnectorProcess"/> against tiny bash fixtures under
/// <c>fake-exes/</c> rather than the full gRPC fixture -- these tests are about process lifecycle
/// (spawn failure, stderr capture, socket-dir permissions, group kill), not the wire protocol, and
/// bash scripts exercise that surface with none of the protocol machinery in the way.
///
/// Unix permission bits only: <see cref="File.GetUnixFileMode(string)"/>/<see cref="File.SetUnixFileMode(string, UnixFileMode)"/>
/// are no-op fictions on Windows, hence the platform attribute below -- same reasoning
/// <see cref="Pz.Cli.Tests.RunRetentionFailureTests"/> already uses for the same trick. Every fact
/// still carries its own <c>Skip.If(OperatingSystem.IsWindows(), ...)</c> so a Windows run reports
/// these as skipped rather than simply absent.
///
/// <para>Unlike this suite's siblings, these facts spawn bash scripts directly (not the
/// <c>PcpFakeConnector</c> fixture) to drive <see cref="ConnectorProcess"/> lifecycle mechanics --
/// spawn failure, stderr capture, socket-dir permissions, process-group kill via <c>pgrep</c> -- so
/// every fact here stays Windows-skipped even though the AF_UNIX transport itself is now proven
/// elsewhere in this category.</para></summary>
[SupportedOSPlatform("linux")]
[Trait("Category", "Pcp")]
public sealed class ConnectorProcessTests : IDisposable
{
    private readonly List<string> _socketDirs = [];

    private static string FixturePath(string name) =>
        Path.Combine(AppContext.BaseDirectory, "ProcessHosting", "fake-exes", name);

    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-connproc-test-" + Guid.NewGuid().ToString("N"));
        _socketDirs.Add(dir);
        return dir;
    }

    [SkippableFact]
    public void Spawn_missing_executable_is_PZ0355()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");

        var missing = Path.Combine(AppContext.BaseDirectory, "ProcessHosting", "fake-exes", "does-not-exist.sh");
        var ex = Assert.Throws<ConnectorHostException>(
            () => ConnectorProcess.Spawn(missing, NewSocketDir(), "test-package"));

        Assert.Equal("PZ0355", ex.Code);
        Assert.Contains("test-package", ex.Message);
        Assert.Contains(missing, ex.Message);
    }

    [SkippableFact]
    public void Spawn_failure_does_not_leak_the_socket_dir_it_created()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");

        var missing = Path.Combine(AppContext.BaseDirectory, "ProcessHosting", "fake-exes", "does-not-exist.sh");
        var socketDir = NewSocketDir();
        var ex = Assert.Throws<ConnectorHostException>(
            () => ConnectorProcess.Spawn(missing, socketDir, "test-package"));

        Assert.Equal("PZ0355", ex.Code);
        Assert.False(Directory.Exists(socketDir));
    }

    [SkippableFact]
    public async Task Stderr_is_captured_as_ring_buffer()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("die.sh"));

        await using var process = ConnectorProcess.Spawn(FixturePath("die.sh"), NewSocketDir(), "test-package");

        // Not the Exited event: die.sh can be gone before a handler is attached, and an event
        // subscribed after it fired is never raised.
        await process.ExitedForTests.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(process.HasExited);
        Assert.Contains("die.sh: known failure line", process.StderrTail);
    }

    [SkippableFact]
    public async Task Exit_code_is_captured_for_a_plain_nonzero_exit()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("die.sh"));

        await using var process = ConnectorProcess.Spawn(FixturePath("die.sh"), NewSocketDir(), "test-package");
        await process.ExitedForTests.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(1, process.ExitCode);
        Assert.Equal("exited with code 1", process.ExitDescription);
    }

    [SkippableFact]
    public async Task Group_kill_is_named_by_its_signal()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("hang.sh"));

        var process = ConnectorProcess.Spawn(FixturePath("hang.sh"), NewSocketDir(), "test-package");

        // DisposeAsync's kill sends SIGKILL directly (.NET's Process.Kill on Unix), so the POSIX wait
        // status this produces -- 128 + signal -- is exactly the OOM-kill (137) shape PZ0356/PZ0358
        // need to name, deterministically, with no need to raise a real signal by hand.
        await process.DisposeAsync();

        Assert.Equal(137, process.ExitCode);
        Assert.Equal("exited with code 137 (signal SIGKILL)", process.ExitDescription);
    }

    /// <summary>Stdout is redirected so a connector's chatter can never reach pz's own stdout (which
    /// may be the NDJSON event stream). A redirected pipe nobody reads fills at the OS buffer size and
    /// blocks the child's next write forever — a hang with no diagnostic. The fixture writes past that
    /// size, including a megabyte with no newline, and only then reports on stderr.</summary>
    [SkippableFact]
    public async Task Chatty_stdout_does_not_block_the_child()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("chatty.sh"));

        await using var process = ConnectorProcess.Spawn(FixturePath("chatty.sh"), NewSocketDir(), "test-package");

        await process.ExitedForTests.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Contains("chatty.sh: finished writing", process.StderrTail);
        // Stdout is drained, not kept: the tail stays the connector's own diagnostics.
        Assert.DoesNotContain("progress line", process.StderrTail);
    }

    [SkippableFact]
    public async Task Socket_dir_is_owner_only()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("noop.sh"));

        var socketDir = NewSocketDir();
        await using var process = ConnectorProcess.Spawn(FixturePath("noop.sh"), socketDir, "test-package");

        var mode = File.GetUnixFileMode(socketDir);
        Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute, mode);
    }

    [SkippableFact]
    public async Task Dispose_kills_the_process_group()
    {
        Skip.If(OperatingSystem.IsWindows(), "bash fixtures are unix-only");
        ChmodExecutable(FixturePath("hang.sh"));

        var process = ConnectorProcess.Spawn(FixturePath("hang.sh"), NewSocketDir(), "test-package");
        var parentPid = process.ProcessIdForTests;

        // Give hang.sh a moment to spawn its `sleep` child before we look for it.
        int? childPid = null;
        for (var i = 0; i < 100 && childPid is null; i++)
        {
            childPid = FindChildPid(parentPid);
            if (childPid is null)
            {
                await Task.Delay(50);
            }
        }

        Assert.NotNull(childPid);

        await process.DisposeAsync();

        // A short grace lets the OS finish reaping; kill -0 / GetProcessById throws once truly gone.
        await AssertProcessGoneAsync(parentPid);
        await AssertProcessGoneAsync(childPid!.Value);
    }

    private static void ChmodExecutable(string path)
    {
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
                UnixFileMode.GroupRead | UnixFileMode.GroupExecute |
                UnixFileMode.OtherRead | UnixFileMode.OtherExecute);
        }
    }

    private static int? FindChildPid(int parentPid)
    {
        try
        {
            var psi = new ProcessStartInfo("pgrep", $"-P {parentPid}")
            {
                RedirectStandardOutput = true,
                UseShellExecute = false,
            };
            using var pgrep = Process.Start(psi);
            if (pgrep is null)
            {
                return null;
            }

            var output = pgrep.StandardOutput.ReadToEnd();
            pgrep.WaitForExit();
            var firstLine = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault();
            return firstLine is not null && int.TryParse(firstLine, out var pid) ? pid : null;
        }
        catch
        {
            return null;
        }
    }

    private static async Task AssertProcessGoneAsync(int pid)
    {
        for (var i = 0; i < 100; i++)
        {
            try
            {
                var candidate = Process.GetProcessById(pid);
                if (candidate.HasExited)
                {
                    return;
                }
            }
            catch (ArgumentException)
            {
                return; // no such process -- gone, which is the assertion
            }

            await Task.Delay(50);
        }

        Assert.Fail($"process {pid} was still alive after the grace window");
    }

    public void Dispose()
    {
        foreach (var dir in _socketDirs)
        {
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, recursive: true);
                }
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }
}
