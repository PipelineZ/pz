using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz connector test`: drives the real verb against the real out-of-process fixture
/// (<c>tests/fixtures/PcpFakeConnector</c>, which delegates to a real <c>LocalFilesConnector</c>) --
/// staged as a process-hosted package exactly the way <c>ProcessHostParityTests</c> stages it, so this
/// exercises the same manifest/entrypoint/lock path a restored package would.
///
/// <para>Linux only: the staged package's entrypoint is a <c>#!/bin/sh</c> wrapper, which is
/// POSIX-only.</para></summary>
[SupportedOSPlatform("linux")]
[Trait("Category", "Pcp")]
[Collection("console-and-env-serialized")]
public sealed class ConnectorTestCommandTests : IDisposable
{
    private const string PackageId = "LocalFilesPcp";
    private const string PackageVersion = "1.0.0";
    private const string ProcessConnector = "localfiles-pcp";

    private readonly List<string> _dirs = [];

    [SkippableFact]
    public void Connector_test_passes_every_vector_against_a_well_behaved_fixture()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project);
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.NotEmpty(lines);
        Assert.DoesNotContain(lines, l => l.StartsWith("FAIL", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.StartsWith("PASS handshake", StringComparison.Ordinal));

        // Secret/PII hygiene: vector output must never echo the connection config values (the probe
        // config's root: is this project's own temp directory path).
        Assert.DoesNotContain(project, stdout, StringComparison.Ordinal);
    }

    /// <summary>The fixture's <c>--misreport-capabilities</c> mode ORs an undeclared capability into
    /// Hello -- a disagreement with the manifest's own declared set that <c>PcpClient</c>'s handshake
    /// gate refuses (PZ0356) before Configure ever runs. That must surface as exactly one failed
    /// vector, "handshake", and exit 1 -- not a config/usage error.</summary>
    [SkippableFact]
    public void Connector_test_reports_the_handshake_vector_failed_against_a_capability_mismatch()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project, extraFixtureArgs: "--misreport-capabilities");
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Single(lines);
        Assert.StartsWith("FAIL handshake", lines[0], StringComparison.Ordinal);
    }

    [SkippableFact]
    public void Connector_test_skips_the_sync_state_vector_for_a_connector_that_does_not_declare_it()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project);
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Contains(lines, l => l.StartsWith("SKIP sync-state-roundtrip", StringComparison.Ordinal));
    }

    [SkippableFact]
    public void Connector_test_passes_the_sync_state_vector_for_a_feed_connector()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project, extraFixtureArgs: "--sync-state", capabilities: FeedCapabilities);
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.DoesNotContain(lines, l => l.StartsWith("FAIL", StringComparison.Ordinal));
        Assert.Contains(lines, l => l.StartsWith("PASS sync-state-roundtrip", StringComparison.Ordinal));
        // The token is connector state, never echoed: only the vector's verdict reaches stdout.
        Assert.DoesNotContain("0+4", stdout, StringComparison.Ordinal);
    }

    [SkippableFact]
    public void Connector_test_fails_the_sync_state_vector_for_a_connector_that_declares_but_does_not_implement_it()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project, extraFixtureArgs: "--declare-sync-state-only", capabilities: FeedCapabilities);
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var failed = Assert.Single(lines, l => l.StartsWith("FAIL", StringComparison.Ordinal));
        Assert.StartsWith("FAIL sync-state-roundtrip: declares SyncState but does not implement GetNaturalReadShape/GetReadState", failed, StringComparison.Ordinal);
    }

    [Fact]
    public void Connector_test_exits_2_with_a_pz_coded_error_for_an_unknown_path()
    {
        var missing = Path.Combine(Path.GetTempPath(), "pz-connector-test-missing-" + Guid.NewGuid().ToString("N")[..8]);

        var stderr = RunAndCaptureStderr(["connector", "test", missing], out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0354", stderr, StringComparison.Ordinal);
    }

    /// <summary>The probe config is interpolated from the environment the way connections.yml is:
    /// the same <c>${VAR}</c> in both files resolves to the same value, so a connector's CI can keep
    /// one probe file with no literal address or credential in it.</summary>
    [SkippableFact]
    public void Connector_test_interpolates_environment_variables_in_the_probe_config()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project);
        var configPath = WriteProbeConfig(project);
        // The absolute root: becomes a reference; only the environment knows the real directory.
        File.WriteAllText(configPath, File.ReadAllText(configPath).Replace($"root: {project}", "root: ${PZ_TEST_PROBE_ROOT}", StringComparison.Ordinal));
        Environment.SetEnvironmentVariable("PZ_TEST_PROBE_ROOT", project);
        try
        {
            var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

            Assert.Equal(ExitCodes.Ok, exit);
            Assert.Contains("PASS handshake", stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("FAIL", stdout, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_TEST_PROBE_ROOT", null);
        }
    }

    /// <summary>An undeclared variable is the same PZ0103 connections.yml raises, every one of them at
    /// once, under the config/usage exit 2 -- never a probe run against a literal "${VAR}".</summary>
    [SkippableFact]
    public void Connector_test_exits_2_naming_every_undeclared_environment_variable_in_the_probe_config()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project);
        var configPath = Path.Combine(project, "probe.yml");
        File.WriteAllText(configPath, """
            connection:
              root: ${PZ_TEST_UNSET_ROOT}
            read:
              dataset: orders
              path: ${PZ_TEST_UNSET_PATH}
            """);

        var stderr = RunAndCaptureStderr(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0103", stderr, StringComparison.Ordinal);
        Assert.Contains("PZ_TEST_UNSET_ROOT", stderr, StringComparison.Ordinal);
        Assert.Contains("PZ_TEST_UNSET_PATH", stderr, StringComparison.Ordinal);
    }

    /// <summary>A missing/mistyped --config path must be a PZ-coded exit 2, not YamlMapper's raw
    /// FileNotFoundException escaping as an unhandled exception under exit 1 -- exit 1 is reserved for
    /// "a vector failed", which never even started here.</summary>
    [SkippableFact]
    public void Connector_test_exits_2_with_a_pz_coded_error_for_an_unknown_config_path()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project);
        var missingConfig = Path.Combine(project, "does-not-exist.yml");

        var stderr = RunAndCaptureStderr(["connector", "test", packageDir, "--config", missingConfig], out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0101", stderr, StringComparison.Ordinal);
        Assert.Contains(missingConfig, stderr, StringComparison.Ordinal);
        Assert.DoesNotContain("System.IO", stderr, StringComparison.Ordinal);
        Assert.DoesNotContain("   at ", stderr, StringComparison.Ordinal);
    }

    // --- fixture staging (mirrors ProcessHostParityTests.WriteProcessPackage) -------------------

    private static readonly string[] LocalFilesCapabilities =
        ["NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead"];

    /// <summary>What the fixture's --sync-state / --declare-sync-state-only modes report: a feed
    /// connector withdraws PartitionedRead and declares SyncState.</summary>
    private static readonly string[] FeedCapabilities =
        ["NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "SyncState"];

    /// <summary><paramref name="extraFixtureArgs"/> is baked into the wrapper script itself, appended
    /// AFTER the args <c>ConnectorProcess.Spawn</c> forwards (<c>--pz-socket &lt;path&gt;</c>) -- the
    /// production spawn path has no argv seam for staging a misbehavior switch (config crosses only
    /// through Configure, never argv), so the wrapper script is what stands in for "this specific
    /// package's binary happens to misbehave" without adding one.</summary>
    private static string WriteProcessPackage(
        string projectDir, string? extraFixtureArgs = null, string[]? capabilities = null)
    {
        var packageDir = Path.Combine(projectDir, "package");
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint,
            $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" \"$@\"{(extraFixtureArgs is null ? "" : " " + extraFixtureArgs)}\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        var manifest = new Dictionary<string, object?>
        {
            ["name"] = ProcessConnector,
            ["protocolMajorMin"] = 1,
            ["protocolMajorMax"] = 1,
            // Exactly what LocalFilesConnector declares -- the true set the fixture's Hello reports
            // absent any misbehavior switch, so a mismatch is only ever the switch's doing.
            ["capabilities"] = capabilities ?? LocalFilesCapabilities,
            ["runtime"] = "process",
            ["entrypoints"] = new Dictionary<string, string>
            {
                [RuntimeInformation.RuntimeIdentifier] = "bin/connector",
            },
        };
        File.WriteAllText(
            Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(manifest));

        return packageDir;
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, "PcpFakeConnector");
    }

    /// <summary>A minimal --config: an absolute <c>root:</c> (so path resolution needs no
    /// <c>base_dir</c> injection, which only <c>ConnectorRegistryFactory</c>'s project-aware load path
    /// provides -- this bare verb has no project), a small real CSV <c>read:</c> dataset, and a
    /// <c>write:</c> output the commit/abort/ticket vectors exercise. Both directions are supplied so
    /// every applicable vector actually runs rather than skipping.</summary>
    private static string WriteProbeConfig(string projectDir)
    {
        var dataDir = Path.Combine(projectDir, "data");
        Directory.CreateDirectory(dataDir);
        File.WriteAllText(Path.Combine(dataDir, "orders.csv"),
            "id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n");

        var configPath = Path.Combine(projectDir, "probe.yml");
        File.WriteAllText(configPath, $"""
            connection:
              root: {projectDir}
            read:
              dataset: orders
              path: data/orders.csv
              format: csv
              columns:
                id: bigint
                customer: varchar
                amount: double
            write:
              output: customer_totals
              mode: replace
              format: parquet
              path: out/
            """);
        return configPath;
    }

    private string NewProjectDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pzct" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    private static string RunAndCaptureStdout(string[] args, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try { exit = CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetOut(original); }
        return stdout.ToString();
    }

    private static string RunAndCaptureStderr(string[] args, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try { exit = CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetError(original); }
        return stderr.ToString();
    }

    public void Dispose()
    {
        foreach (var dir in _dirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }
}
