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

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable.Value, "docker is not available");

    public static void SkipIfOffline() => Skip.If(
        Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1", "PZ_TESTS_OFFLINE=1");
}
