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
/// these as skipped rather than simply absent.</summary>
[SupportedOSPlatform("linux")]
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

        var exited = new TaskCompletionSource();
        process.Exited += () => exited.TrySetResult();
        await exited.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(process.HasExited);
        Assert.Contains("die.sh: known failure line", process.StderrTail);
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
