using System.Diagnostics;

namespace Pz.TestSupport;

public static class DockerFacts
{
    private static readonly Lazy<bool> DockerAvailable = new(() =>
    {
        try
        {
            var psi = new ProcessStartInfo("docker", "info") { RedirectStandardOutput = true, RedirectStandardError = true };
            using var process = Process.Start(psi);
            return process is not null && process.WaitForExit(5000) && process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    });

    /// <summary>Non-throwing probe for IClassFixture setup: a class fixture whose constructor throws
    /// SkipException gets wrapped in a TestClassException and reported as FAILED, not skipped — so a
    /// class fixture must check this and no-op, leaving <see cref="SkipUnlessDocker"/> to the test
    /// class constructor, where SkipException does turn into a skip.</summary>
    public static bool IsAvailable => DockerAvailable.Value;

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable.Value, "docker is not available");

    public static void SkipIfOffline() => Skip.If(
        Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1", "PZ_TESTS_OFFLINE=1");
}
