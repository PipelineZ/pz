using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>Where a process-hosted connector's control socket lives. The rule is not cosmetic: a unix
/// domain socket path is capped by <c>sockaddr_un.sun_path</c> (104 bytes on macOS, 108 on Linux), and
/// a project directory deep enough to blow that budget would otherwise leave every out-of-process
/// connector failing to bind, from inside the child, with nothing a user could act on.
///
/// <para>Every owned root is deleted in the same fact that minted it, never at class teardown:
/// <c>ProcessHostParityTests</c> asserts that no directory of this exact shape survives a
/// <c>pz validate</c>, and that suite runs in a different process, concurrently with this one.</para></summary>
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

    private static void Delete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
    }
}
