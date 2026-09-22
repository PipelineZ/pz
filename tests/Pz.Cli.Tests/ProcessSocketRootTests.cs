using System.Diagnostics;
using Pz.Cli;
using Pz.Core.Validation;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Tests;

/// <summary>Where a process-hosted connector's control socket lives. The rule is not cosmetic: a unix
/// domain socket path is capped by <c>sockaddr_un.sun_path</c> (104 bytes on macOS, 108 on Linux), and
/// a project directory deep enough to blow that budget would otherwise leave every out-of-process
/// connector failing to bind, from inside the child, with nothing a user could act on.
///
/// <para>Every owned root is deleted in the same fact that minted it, never at class teardown:
/// <c>ProcessHostParityTests</c> asserts that no directory of this exact shape survives a
/// <c>pz validate</c>, and that suite runs in a different process, concurrently with this one.</para>
///
/// <para>Joins "console-and-env-serialized" (see that collection's definition in
/// <c>RestoreCommandTests.cs</c>) because <see cref="A_temp_root_too_deep_for_a_socket_is_refused_not_handed_back"/>
/// mutates the process-global TMPDIR environment variable, which <c>Path.GetTempPath()</c> reads on
/// every call across the whole assembly -- an uncollected class calling it concurrently would race the
/// mutation.</para></summary>
[Collection("console-and-env-serialized")]
public sealed class ProcessSocketRootTests
{
    [Fact]
    public void A_run_puts_its_sockets_under_the_run_directory()
    {
        var (root, owned) = ProcessSocketRoot.Resolve("/tmp/proj", "20260824T101112131Z-ab12");

        Assert.Equal(
            Path.Combine("/tmp/proj", ".pz", "runs", "20260824T101112131Z-ab12", "sockets"), root);
        Assert.False(owned); // collected with the run directory; nothing for the caller to delete
    }

    [Fact]
    public void No_run_falls_back_to_a_directory_the_caller_owns()
    {
        var (root, owned) = ProcessSocketRoot.Resolve("/tmp/proj", runId: null);
        try
        {
            Assert.True(owned);
            Assert.True(Directory.Exists(root));
            Assert.StartsWith(Path.GetTempPath(), root, StringComparison.Ordinal);
        }
        finally
        {
            Delete(root);
        }
    }

    /// <summary>The guard the sun_path cap exists for: a project directory long enough that the
    /// run-scoped root could not serve a socket takes the same owned-temp route a runless verb takes,
    /// rather than producing a path that cannot bind.</summary>
    [Fact]
    public void A_run_whose_directory_is_too_deep_for_a_socket_falls_back_too()
    {
        var deep = "/tmp/" + new string('d', 80);
        var (root, owned) = ProcessSocketRoot.Resolve(deep, "20260824T101112131Z-ab12");
        try
        {
            Assert.True(owned);
            Assert.DoesNotContain(deep, root, StringComparison.Ordinal);
        }
        finally
        {
            Delete(root);
        }
    }

    /// <summary>The guard the too-long-run-directory fact above proves the run-scoped side of: when
    /// <c>Path.GetTempPath()</c> itself (TMPDIR) is too deep to leave room for the socket path even for
    /// the fallback's short "pz-&lt;8 hex&gt;" name, Resolve refuses loudly (PZ0355) instead of handing
    /// back an unbindable root a spawned child would fail to bind with nothing a user could act on.
    /// GetTempPath() reads TMPDIR on every call (no caching to defeat), so setting it for the duration
    /// of this test is enough -- restored in `finally` so it never leaks into another test's temp
    /// directory resolution.</summary>
    [SkippableFact]
    public void A_temp_root_too_deep_for_a_socket_is_refused_not_handed_back()
    {
        // TMPDIR (and the /tmp literal below) is a POSIX convention; Path.GetTempPath() on Windows
        // reads %TEMP%/%TMP% instead, so this drive would not exercise the guard there.
        Skip.If(OperatingSystem.IsWindows(), "TMPDIR is a POSIX convention; Windows reads %TEMP%/%TMP%");

        var original = Environment.GetEnvironmentVariable("TMPDIR");
        var deepTemp = "/tmp/" + new string('t', 90);
        Environment.SetEnvironmentVariable("TMPDIR", deepTemp);
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(() => ProcessSocketRoot.Resolve("/tmp/proj", runId: null));

            Assert.Equal(PzErrorCode.ConnectorSpawnFailed, ex.Code);
            Assert.Contains(deepTemp, ex.Message, StringComparison.Ordinal);
            Assert.Contains("TMPDIR", ex.Hint, StringComparison.Ordinal);
            Assert.False(Directory.Exists(deepTemp)); // refused before Directory.CreateDirectory -- nothing to leak
        }
        finally
        {
            Environment.SetEnvironmentVariable("TMPDIR", original);
        }
    }

    private static void Delete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
    }
}

/// <summary><see cref="ProcessSocketRoot.SweepStaleRoots"/>: the housekeeping that reclaims a temp
/// socket root a killed (not merely exited) <c>pz</c> process left behind -- <see cref="Resolve"/>
/// itself has nothing that ever runs it, since a run-scoped root does not need it (collected with its
/// run directory regardless of how the process ends). Each test points the sweep at its own scratch
/// directory rather than the real system temp directory, so it needs neither the
/// "console-and-env-serialized" collection above (no TMPDIR mutation) nor cleans up anything real.</summary>
public sealed class ProcessSocketRootSweepTests : IDisposable
{
    private readonly string _scratch = Directory.CreateTempSubdirectory("pz-sweep-test-").FullName;

    [Fact]
    public void A_root_whose_pid_is_no_longer_running_is_deleted()
    {
        var deadPid = SpawnAndWaitForExit();
        var stale = Path.Combine(_scratch, $"pz-{deadPid}-deadbeef");
        Directory.CreateDirectory(stale);

        ProcessSocketRoot.SweepStaleRoots(_scratch);

        Assert.False(Directory.Exists(stale));
    }

    /// <summary>The other half of the same guard: a live pid's root -- including this very process's
    /// own -- must never be swept, or the sweep could delete sockets a running process is still
    /// serving.</summary>
    [Fact]
    public void A_root_whose_pid_is_still_running_is_left_alone()
    {
        var alive = Path.Combine(_scratch, $"pz-{Environment.ProcessId}-a1a1a1a1");
        Directory.CreateDirectory(alive);

        ProcessSocketRoot.SweepStaleRoots(_scratch);

        Assert.True(Directory.Exists(alive));
    }

    /// <summary>A directory that merely starts with "pz-" but does not fit the exact
    /// "pz-&lt;pid&gt;-&lt;8 hex&gt;" shape (no pid to prove dead) is not this scheme's to guess at, dead
    /// pid or not.</summary>
    [Fact]
    public void A_directory_that_does_not_fit_the_naming_scheme_is_left_alone()
    {
        var notOurs = Path.Combine(_scratch, "pz-not-a-pid-root");
        Directory.CreateDirectory(notOurs);

        ProcessSocketRoot.SweepStaleRoots(_scratch);

        Assert.True(Directory.Exists(notOurs));
    }

    /// <summary>Spawns a trivial child, waits for it to exit, and returns its pid -- a pid this specific
    /// test run can prove is dead (rather than picking an arbitrary large number and hoping nothing on
    /// the box happens to hold it).</summary>
    private static int SpawnAndWaitForExit()
    {
        var psi = new ProcessStartInfo
        {
            FileName = OperatingSystem.IsWindows() ? "cmd.exe" : "/bin/sh",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add(OperatingSystem.IsWindows() ? "/c" : "-c");
        psi.ArgumentList.Add("exit 0");

        using var process = Process.Start(psi)!;
        process.WaitForExit();
        return process.Id;
    }

    public void Dispose()
    {
        try { Directory.Delete(_scratch, recursive: true); } catch { /* best-effort cleanup */ }
    }
}
