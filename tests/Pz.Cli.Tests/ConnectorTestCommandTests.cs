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
/// <para>Linux only: the fixture serves the protocol over unix domain sockets and does not implement
/// the named-pipe transport.</para></summary>
[SupportedOSPlatform("linux")]
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
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

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
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

        var project = NewProjectDir();
        var packageDir = WriteProcessPackage(project, extraFixtureArgs: "--misreport-capabilities");
        var configPath = WriteProbeConfig(project);

        var stdout = RunAndCaptureStdout(["connector", "test", packageDir, "--config", configPath], out var exit);

        Assert.Equal(ExitCodes.NodeFailures, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Single(lines);
        Assert.StartsWith("FAIL handshake", lines[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Connector_test_exits_2_with_a_pz_coded_error_for_an_unknown_path()
    {
        var missing = Path.Combine(Path.GetTempPath(), "pz-connector-test-missing-" + Guid.NewGuid().ToString("N")[..8]);

        var stderr = RunAndCaptureStderr(["connector", "test", missing], out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0354", stderr, StringComparison.Ordinal);
    }

    // --- fixture staging (mirrors ProcessHostParityTests.WriteProcessPackage) -------------------

    /// <summary><paramref name="extraFixtureArgs"/> is baked into the wrapper script itself, appended
    /// AFTER the args <c>ConnectorProcess.Spawn</c> forwards (<c>--pz-socket &lt;path&gt;</c>) -- the
    /// production spawn path has no argv seam for staging a misbehavior switch (config crosses only
    /// through Configure, never argv), so the wrapper script is what stands in for "this specific
    /// package's binary happens to misbehave" without adding one.</summary>
    private static string WriteProcessPackage(string projectDir, string? extraFixtureArgs = null)
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
            ["capabilities"] = new[]
            {
                "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead",
            },
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
