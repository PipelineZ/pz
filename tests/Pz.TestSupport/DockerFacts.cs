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

    /// <summary>What a docker suite does about one image.</summary>
    public enum ImageVerdict { Run, Skip, Fail }

    /// <summary>A verdict and, unless it is <see cref="ImageVerdict.Run"/>, the reason.</summary>
    public sealed record ImageGate(ImageVerdict Verdict, string? Reason);

    /// <summary>True under CI (<c>CI</c> set), where a docker suite must run or fail, never skip because of an
    /// image.</summary>
    public static bool UnderCi => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("CI"));

    private static readonly ConcurrentDictionary<string, Lazy<ImageGate>> ImageGates = new(StringComparer.Ordinal);

    /// <summary>The rule, without docker: no usable daemon skips (as a docker-less machine always has). Otherwise a
    /// missing image is always pulled; <c>PZ_TESTS_OFFLINE</c> governs network-dependent tests, not fixtures, so it
    /// is not consulted here. A pull that fails (denied, rate-limited, a registry out of reach) skips with its reason
    /// locally, and fails under CI, so a CI run never quietly loses a docker suite.</summary>
    public static ImageGate DecideImage(string image, bool daemonAvailable, Func<bool> isPresent,
        Func<(int ExitCode, string Error)> pull, bool underCi)
    {
        if (!daemonAvailable)
        {
            return new(ImageVerdict.Skip, "docker is not available");
        }

        if (isPresent())
        {
            return new(ImageVerdict.Run, null);
        }

        var (exitCode, error) = pull();
        if (exitCode == 0)
        {
            return new(ImageVerdict.Run, null);
        }

        var line = error.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? (exitCode < 0 ? "the pull timed out" : $"docker pull exited {exitCode}");
        var reason = $"docker pull {image} failed: {(line.Length > 300 ? line[..300] : line)}";
        return new(underCi ? ImageVerdict.Fail : ImageVerdict.Skip, reason);
    }

    /// <summary><see cref="DecideImage"/> against this machine's docker, asked once per image per process.</summary>
    public static ImageGate GateFor(string image) =>
        ImageGates.GetOrAdd(image, i => new Lazy<ImageGate>(() => DecideImage(i, DockerAvailable.Value,
            () => RunDocker(TimeSpan.FromSeconds(30), "image", "inspect", "--format", "{{.Id}}", i).ExitCode == 0,
            () => RunDocker(PullTimeout, "pull", i), UnderCi))).Value;

    /// <summary>Why a suite cannot run with <paramref name="image"/>; null when it can.</summary>
    public static string? ImageProblem(string image) => GateFor(image).Reason;

    /// <summary>Non-throwing form of <see cref="SkipUnlessImage"/>, for a fixture that must no-op rather than throw
    /// (see <see cref="IsAvailable"/>). False only for a skip: under CI a failed pull answers true, so the fixture
    /// goes on to start its container and the suite fails loudly.</summary>
    public static bool CanProvide(string image) =>
        ImagesToGate([image]).All(i => GateFor(i).Verdict != ImageVerdict.Skip);

    /// <summary>The images a suite needs before its first container starts: its own, plus the resource reaper
    /// (ryuk) Testcontainers starts first when it is enabled, so a refused ryuk pull skips or fails by the same rule
    /// as any other image. Nothing in this repository configures ryuk, so Testcontainers' own settings (environment
    /// or <c>~/.testcontainers.properties</c>) decide whether it runs and which image it is.</summary>
    public static IReadOnlyList<string> ImagesToGate(IReadOnlyList<string> images, bool reaperEnabled, string reaperImage)
        => reaperEnabled && !images.Contains(reaperImage, StringComparer.Ordinal) ? [.. images, reaperImage] : images;

    /// <summary><see cref="ImagesToGate(IReadOnlyList{string}, bool, string)"/> with Testcontainers' own reaper
    /// settings.</summary>
    public static IReadOnlyList<string> ImagesToGate(IReadOnlyList<string> images)
        => Reaper.Value is { } reaper ? ImagesToGate(images, reaper.Enabled, reaper.Image) : images;

    /// <summary>Testcontainers' own reaper settings, read from the Testcontainers assembly the calling suite already
    /// loads (this library does not reference it, so suites without containers do not pull it in); null when it is
    /// not loadable, in which case no container, and so no reaper, can start either.</summary>
    private static readonly Lazy<(bool Enabled, string Image)?> Reaper = new(() =>
    {
        var settings = Type.GetType("DotNet.Testcontainers.Configurations.TestcontainersSettings, Testcontainers");
        if (settings?.GetProperty("ResourceReaperEnabled")?.GetValue(null) is not bool enabled)
        {
            return null;
        }

        // An override (TESTCONTAINERS_RYUK_CONTAINER_IMAGE, ~/.testcontainers.properties) wins; otherwise the default
        // the ResourceReaper type itself holds.
        var image = settings.GetProperty("ResourceReaperImage")?.GetValue(null) ?? DefaultReaperImage();
        return image?.GetType().GetProperty("FullName")?.GetValue(image) is string fullName ? (enabled, fullName) : null;
    });

    private static object? DefaultReaperImage()
    {
        var reaper = Type.GetType("DotNet.Testcontainers.Containers.ResourceReaper, Testcontainers");
        const System.Reflection.BindingFlags Static = System.Reflection.BindingFlags.Static
            | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;
        return reaper?.GetFields(Static).Select(f => f.GetValue(null))
            .Concat(reaper.GetProperties(Static).Where(p => p.GetIndexParameters().Length == 0).Select(p => p.GetValue(null)))
            .FirstOrDefault(v => v?.GetType().GetInterface("IImage") is not null);
    }

    /// <summary>Skips (locally) or fails (under CI) unless docker can provide every one of <paramref name="images"/>
    /// and the resource reaper (<see cref="ImagesToGate(IReadOnlyList{string})"/>); call it where
    /// <see cref="SkipUnlessDocker"/> is called, before any container is built.</summary>
    public static void SkipUnlessImage(params string[] images)
    {
        foreach (var image in ImagesToGate(images))
        {
            var gate = GateFor(image);
            switch (gate.Verdict)
            {
                case ImageVerdict.Skip:
                    throw new SkipException($"docker cannot provide image '{image}': {gate.Reason}");
                case ImageVerdict.Fail:
                    throw new InvalidOperationException(
                        $"docker cannot provide image '{image}' under CI, where a docker suite must run: {gate.Reason}");
            }
        }
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
