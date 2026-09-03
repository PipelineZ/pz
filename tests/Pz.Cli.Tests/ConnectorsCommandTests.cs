using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;
using Pz.PackageManagement.Restore;

namespace Pz.Cli.Tests;

/// <summary>`pz connectors`: builds the registry exactly like `run`/`plan` (builtins + restored per
/// lock, drift rules honored) and prints `name package version tiers capabilities`.
/// Joins "console-and-env-serialized" (see its definition in RestoreCommandTests.cs) both
/// for <see cref="CliLocalFeedFixture"/> and because it redirects Console.Out/Error and mutates the
/// process-global DATA_DIR/OUT_DIR environment variables.</summary>
[Collection("console-and-env-serialized")]
public sealed class ConnectorsCommandTests(CliLocalFeedFixture feed) : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-connectors-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Connectors_lists_builtins_with_capabilities()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data");
        Environment.SetEnvironmentVariable("OUT_DIR", "/tmp/pz-out");
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz"), _work);

        var output = RunAndCaptureStdout(["connectors", "--project", _work]);
        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).ToArray();

        var localfilesLine = Assert.Single(lines, l => l.StartsWith("localfiles", StringComparison.Ordinal));
        Assert.Contains("src:native+universal snk:native+universal", localfilesLine);
        Assert.Contains("pz (builtin)", localfilesLine);

        var postgresLine = Assert.Single(lines, l => l.StartsWith("postgres", StringComparison.Ordinal));
        Assert.Contains("src:universal", postgresLine);
        Assert.Contains("snk:universal", postgresLine);

        var s3Line = Assert.Single(lines, l => l.StartsWith("s3", StringComparison.Ordinal));
        Assert.Contains("snk:native-only", s3Line);
        Assert.Contains("src:native-only", s3Line);

        var azureLine = Assert.Single(lines, l => l.StartsWith("azureblob", StringComparison.Ordinal));
        Assert.Contains("src:native-only", azureLine);
        Assert.Contains("snk:native+universal", azureLine);

        var gcsLine = Assert.Single(lines, l => l.StartsWith("gcs", StringComparison.Ordinal));
        Assert.Contains("src:native-only", gcsLine);
        Assert.Contains("snk:native+universal", gcsLine);

        var duckdbLine = Assert.Single(lines, l => l.StartsWith("duckdb", StringComparison.Ordinal));
        Assert.Contains("src:native-only", duckdbLine);
        Assert.Contains("snk:native-only", duckdbLine);

        var ducklakeLine = Assert.Single(lines, l => l.StartsWith("ducklake", StringComparison.Ordinal));
        Assert.Contains("src:native-only", ducklakeLine);
        Assert.Contains("snk:native-only", ducklakeLine);

        var quackLine = Assert.Single(lines, l => l.StartsWith("quack", StringComparison.Ordinal));
        Assert.Contains("src:native-only", quackLine);
        Assert.Contains("snk:native-only", quackLine);

        var motherduckLine = Assert.Single(lines, l => l.StartsWith("motherduck", StringComparison.Ordinal));
        Assert.Contains("src:native-only", motherduckLine);
        Assert.Contains("snk:native-only", motherduckLine);
    }

    /// <summary>Covers <c>DescribeHostedPackage</c>'s single-non-builtin-package attribution (the common
    /// case; the rows above are all builtins). Stages a <c>runtime: "process"</c> package (the
    /// PcpFakeConnector fixture, exactly the way <c>ConnectorTestCommandTests</c> stages it) and asserts
    /// the hosted row's exact name, package id, version, and tiers fields. The capabilities column
    /// requires the connector's Hello, so this is the one listing test that actually spawns the
    /// connector process. Linux only: the fixture serves unix domain sockets.</summary>
    [SkippableFact]
    [SupportedOSPlatform("linux")]
    public void Connectors_lists_hosted_connector_with_package_attribution()
    {
        Skip.If(OperatingSystem.IsWindows(), "PcpFakeConnector serves unix domain sockets only");

        WriteProject("""
            name: connectors_test
            version: 0.1.0

            connectors:
              - package: LocalFilesPcp
                version: 1.0.0
            """);
        WriteSpawnableProcessPackage();

        var output = RunAndCaptureStdout(["connectors", "--project", _work]);
        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries).ToArray();

        var hostedLine = Assert.Single(lines, l => l.StartsWith("localfiles-pcp", StringComparison.Ordinal));
        var fields = hostedLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        Assert.Equal("localfiles-pcp", fields[0]);
        Assert.Equal("LocalFilesPcp", fields[1]);
        Assert.Equal("1.0.0", fields[2]);
        Assert.Equal("src:native+universal", fields[3]);
        Assert.Equal("snk:native+universal", fields[4]);
    }

    /// <summary>Mirrors <c>ConnectorTestCommandTests.WriteProcessPackage</c>, laid out under
    /// <c>.pz/packages/&lt;id&gt;/&lt;version&gt;/</c> with the matching pz.lock.json the drift check
    /// verifies. The manifest's capability set is exactly what the fixture's Hello reports, so the
    /// handshake gate passes.</summary>
    [SupportedOSPlatform("linux")]
    private void WriteSpawnableProcessPackage()
    {
        const string packageId = "LocalFilesPcp";
        const string version = "1.0.0";
        var packageDir = Path.Combine(_work, ".pz", "packages", packageId, version);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint, $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" \"$@\"\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        File.WriteAllText(Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(
            new Dictionary<string, object?>
            {
                ["name"] = "localfiles-pcp",
                ["protocolMajorMin"] = 1,
                ["protocolMajorMax"] = 1,
                ["capabilities"] = new[]
                {
                    "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead",
                },
                ["runtime"] = "process",
                ["entrypoints"] = new Dictionary<string, string>
                {
                    [RuntimeInformation.RuntimeIdentifier] = "bin/connector",
                },
            }));

        LockFileWriter.Write(
            new LockFile(LockFileWriter.CurrentVersion, RuntimeInformation.RuntimeIdentifier, [
                new LockedPackage(packageId, version, "sha512-listing-fixture", new LockedAssets([], [])),
            ]),
            Path.Combine(_work, "pz.lock.json"));
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, "PcpFakeConnector");
    }

    [Fact]
    public void Connectors_errors_cleanly_outside_project()
    {
        var emptyDir = Path.Combine(Path.GetTempPath(), "pz-connectors-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(emptyDir);
        try
        {
            var stderr = RunAndCaptureStderr(["connectors", "--project", emptyDir], out var exit);
            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("project.yml", stderr);
        }
        finally
        {
            Directory.Delete(emptyDir, recursive: true);
        }
    }

    /// <summary>Decision 6: `pz connectors` builds the registry exactly like `run`/`plan` -- a drifted
    /// lock is refused with the same PZ0321 remediation, not silently tolerated because this verb is
    /// read-only. Mirrors <c>RestoreCommandTests.Run_with_drifted_lock_is_error_PZ0321_hint_restore</c>.</summary>
    [Fact]
    public void Connectors_refuses_on_lock_drift()
    {
        WriteProject(FakeSourceConnectorProject());
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke());

        // Drift the requirement so the committed lock (pinned to 1.2.3) no longer satisfies it.
        WriteProject(FakeSourceConnectorProject(requiredVersion: "9.9.9"));

        var stderr = RunAndCaptureStderr(["connectors", "--project", _work], out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0321", stderr);
        Assert.Contains("restore", stderr);
    }

    private string FakeSourceConnectorProject(string requiredVersion = "1.2.3") => $"""
        name: connectors_test
        version: 0.1.0

        connectors:
          - package: FakeSourceConnector
            version: {requiredVersion}
        """;

    private void WriteProject(string yaml)
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), yaml);
    }

    private static string RunAndCaptureStdout(string[] args)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try { exit = CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetOut(original); }
        Assert.Equal(ExitCodes.Ok, exit);
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

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}
