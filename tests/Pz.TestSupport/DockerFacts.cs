using System.Diagnostics;

namespace Pz.TestSupport;

public static class DockerFacts
{
    /// <summary>Every suite here runs Linux images, so a daemon that answers but runs Windows containers
    /// (a Windows CI runner's default) counts as no docker: gating on `docker info` alone would send those
    /// suites into image pulls that can only fail.</summary>
    private static readonly Lazy<bool> DockerAvailable = new(() =>
    {
        try
        {
            var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
            psi.ArgumentList.Add("info");
            psi.ArgumentList.Add("--format");
            psi.ArgumentList.Add("{{.OSType}}");
            using var process = Process.Start(psi);
            if (process is null)
            {
                return false;
            }

            var osType = process.StandardOutput.ReadToEndAsync();
            return process.WaitForExit(5000) && process.ExitCode == 0
                && osType.Wait(1000) && osType.Result.Trim() == "linux";
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

    /// <summary>Non-throwing probe for the same reason as <see cref="IsAvailable"/>: a class fixture's
    /// <c>InitializeAsync</c> checks this to no-op instead of calling <see cref="SkipIfOffline"/>, whose
    /// SkipException would report FAILED rather than Skipped when thrown outside a test method.</summary>
    public static bool IsOffline => Environment.GetEnvironmentVariable("PZ_TESTS_OFFLINE") == "1";

    public static void SkipUnlessDocker() => Skip.IfNot(DockerAvailable.Value, "docker is not available");

    public static void SkipIfOffline() => Skip.If(IsOffline, "PZ_TESTS_OFFLINE=1");
}
