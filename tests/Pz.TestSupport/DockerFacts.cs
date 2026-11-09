using System.Collections.Concurrent;
using System.Diagnostics;

namespace Pz.TestSupport;

public static class DockerFacts
{
    /// <summary>How long the probe may take. The answer is cached for the whole test process, so a probe that
    /// times out on a slow but working daemon skips every docker suite in it.</summary>
    private static readonly TimeSpan ProbeTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Every suite here runs Linux images, so a daemon that answers but runs Windows containers
    /// (a Windows CI runner's default) counts as no docker: gating on reachability alone would send those
    /// suites into image pulls that can only fail. `docker version` asks the daemon its OS without running the
    /// CLI plugins `docker info` collects metadata from: on a fresh CI runner that plugin scan alone took
    /// longer than the old 5 s limit, and every AzuriteTls test skipped.</summary>
    private static readonly Lazy<bool> DockerAvailable = new(() =>
    {
        try
        {
            var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
            psi.ArgumentList.Add("version");
            psi.ArgumentList.Add("--format");
            psi.ArgumentList.Add("{{.Server.Os}}");
            using var process = Process.Start(psi);
            if (process is null)
            {
                return false;
            }

            var osType = process.StandardOutput.ReadToEndAsync();
            if (!process.WaitForExit(ProbeTimeout))
            {
                // Never leave the probe running past the answer.
                process.Kill(entireProcessTree: true);
                return false;
            }

            return process.ExitCode == 0 && osType.Wait(1000) && osType.Result.Trim() == "linux";
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

    /// <summary>How long one image pull may take before the suite counts the image as unavailable.</summary>
    private static readonly TimeSpan PullTimeout = TimeSpan.FromMinutes(10);

    private static readonly ConcurrentDictionary<string, Lazy<string?>> ImageProblems = new(StringComparer.Ordinal);

    /// <summary>Why docker cannot provide <paramref name="image"/> to this machine (no docker, offline with the
    /// image absent, or a pull docker refused: "pull access denied", a rate limit, an unreachable registry); null
    /// when the image is present or was pulled. An image docker cannot provide makes its suite as unrunnable as no
    /// docker at all, so it must skip the same way, not fail in a fixture. Asked once per image per process.</summary>
    public static string? ImageProblem(string image) =>
        ImageProblems.GetOrAdd(image, i => new Lazy<string?>(() => ProbeImage(i))).Value;

    /// <summary>Non-throwing form of <see cref="SkipUnlessImage"/>, for a fixture that must no-op rather than throw
    /// (see <see cref="IsAvailable"/>).</summary>
    public static bool CanProvide(string image) => ImageProblem(image) is null;

    /// <summary>Skips unless docker can provide every one of <paramref name="images"/>; call it where
    /// <see cref="SkipUnlessDocker"/> is called, before any container is built.</summary>
    public static void SkipUnlessImage(params string[] images)
    {
        foreach (var image in images)
        {
            if (ImageProblem(image) is { } problem)
            {
                throw new SkipException($"docker cannot provide image '{image}': {problem}");
            }
        }
    }

    private static string? ProbeImage(string image)
    {
        if (!DockerAvailable.Value)
        {
            return "docker is not available";
        }

        if (RunDocker(TimeSpan.FromSeconds(30), "image", "inspect", "--format", "{{.Id}}", image).ExitCode == 0)
        {
            return null;
        }

        if (IsOffline)
        {
            return $"{image} is not present locally and PZ_TESTS_OFFLINE=1 forbids pulling it";
        }

        var pull = RunDocker(PullTimeout, "pull", image);
        if (pull.ExitCode == 0)
        {
            return null;
        }

        var reason = pull.Error.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? (pull.ExitCode < 0 ? "the pull timed out" : $"docker pull exited {pull.ExitCode}");
        return $"docker pull {image} failed: {(reason.Length > 300 ? reason[..300] : reason)}";
    }

    private static (int ExitCode, string Error) RunDocker(TimeSpan timeout, params string[] args)
    {
        try
        {
            var psi = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true };
            foreach (var arg in args)
            {
                psi.ArgumentList.Add(arg);
            }

            using var process = Process.Start(psi);
            if (process is null)
            {
                return (-1, "docker could not be started");
            }

            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit(timeout))
            {
                process.Kill(entireProcessTree: true);
                return (-1, "");
            }

            stdout.Wait(1000);
            return (process.ExitCode, stderr.Wait(1000) ? stderr.Result : "");
        }
        catch (Exception ex)
        {
            return (-1, ex.Message);
        }
    }

    public static void SkipIfOffline() => Skip.If(IsOffline, "PZ_TESTS_OFFLINE=1");
}
