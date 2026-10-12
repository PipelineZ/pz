using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;
using Pz.PackageManagement.Restore;

namespace Pz.EndToEnd.Tests;

/// <summary>The engine-level proof that remote stats a process-hosted connector reports reach
/// <c>run_results.json</c>: the host polls the read stats after a partition drains (or tears) and
/// reads the commit response's stats, merging both into the node's accumulator. The fixture's
/// <c>--emit-stats</c> reports per batch one request, 100 wire bytes, 0.25 "ru" and a
/// <c>fake.batches</c> tick on the read side, and one request per batch plus one on commit on the
/// write side.
///
/// <para>Linux only: the staged package's entrypoint is a <c>#!/bin/sh</c> wrapper.</para></summary>
[SupportedOSPlatform("linux")]
[Trait("Category", "Pcp")]
public sealed class ProcessHostStatsTests : IDisposable
{
    private const string PackageId = "LocalFilesPcp";
    private const string PackageVersion = "1.0.0";
    private const string ProcessConnector = "localfiles-pcp";

    private readonly List<string> _dirs = [];

    [SkippableFact]
    public void Stats_from_a_hosted_connector_reach_run_results()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var dir = NewProjectDir();
        WriteProject(dir, retry: false);
        WriteProcessPackage(dir, "--emit-stats --sync-state");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        Assert.Contains("arrow_stream", File.ReadAllText(Path.Combine(dir, ".pz", "target", "plan.json")), StringComparison.Ordinal);
        var nodes = ReadNodes(dir);
        var read = nodes.Single(n => n.GetProperty("kind").GetString() == "SourceLoad").GetProperty("remote");
        var batches = (long)read.GetProperty("extras").GetProperty("fake.batches").GetDouble();
        Assert.True(batches >= 1);
        Assert.Equal(batches, read.GetProperty("requests").GetInt64());
        Assert.Equal(batches * 100, read.GetProperty("wireBytes").GetInt64());
        Assert.Equal("ru", read.GetProperty("cost").GetProperty("unit").GetString());
        Assert.Equal(batches * 0.25, read.GetProperty("cost").GetProperty("units").GetDouble());

        var write = nodes.Single(n => n.GetProperty("kind").GetString() == "SinkWrite").GetProperty("remote");
        Assert.Equal(batches + 1, write.GetProperty("requests").GetInt64());
    }

    [SkippableFact]
    public void A_connector_without_stats_runs_unchanged()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var dir = NewProjectDir();
        WriteProject(dir, retry: false);
        WriteProcessPackage(dir, "--sync-state");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        Assert.All(ReadNodes(dir), n => Assert.False(n.TryGetProperty("remote", out _)));
    }

    /// <summary>The fixture tears every read after its first batch, so the retry is torn too and the
    /// run ends with a failed SourceLoad; both attempts' batches must still be in its facts.</summary>
    [SkippableFact]
    public void Stats_from_a_torn_read_still_count()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var dir = NewProjectDir();
        WriteProject(dir, retry: true);
        WriteProcessPackage(dir, "--emit-stats --sync-state --fail-read-midstream-transient");

        Assert.Equal(ExitCodes.NodeFailures, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        var read = ReadNodes(dir).Single(n => n.GetProperty("kind").GetString() == "SourceLoad");
        Assert.Equal("failed", read.GetProperty("status").GetString());
        var remote = read.GetProperty("remote");
        Assert.Equal(2, remote.GetProperty("requests").GetInt64());
        Assert.Equal(2, (long)remote.GetProperty("extras").GetProperty("fake.batches").GetDouble());
    }

    private static List<JsonElement> ReadNodes(string projectDir)
    {
        var runDir = Directory.EnumerateDirectories(Path.Combine(projectDir, ".pz", "runs")).Single();
        var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(runDir, "run_results.json")));
        return [.. doc.RootElement.GetProperty("nodes").EnumerateArray()];
    }

    private static void WriteProject(string dir, bool retry)
    {
        File.WriteAllText(Path.Combine(dir, "project.yml"),
            "name: pcp_stats\n" +
            "version: 0.1.0\n" +
            "connectors:\n" +
            $"  - package: {PackageId}\n    version: {PackageVersion}\n" +
            "engine:\n" +
            "  threads: 2\n");

        var retryBlock = retry ? "  retry: { max_attempts: 2, base_delay: '10ms', max_delay: '10ms' }\n" : "";
        File.WriteAllText(Path.Combine(dir, "connections.yml"), $"""
            files:
              connector: {ProcessConnector}
            {retryBlock}  entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double

            lake:
              connector: {ProcessConnector}
              entities:
                orders_copy:
                  write:
                    strategy: append
                    duplicates: accept
                    format: parquet
                    path: out/
            """);

        Directory.CreateDirectory(Path.Combine(dir, "pipelines"));
        File.WriteAllText(Path.Combine(dir, "pipelines", "orders_copy.sql"),
            "INSERT INTO {{ sink('lake', 'orders_copy') }}\n"
            + "select id, customer, amount\n"
            + "from {{ source('files', 'orders') }}\n"
            + "order by id\n");

        Directory.CreateDirectory(Path.Combine(dir, "data"));
        File.WriteAllText(Path.Combine(dir, "data", "orders.csv"),
            "id,customer,amount\n1,ann,10.5\n2,bob,20.25\n3,ann,5.25\n4,cy,100.0\n");
    }

    /// <summary>What <c>pz restore</c> would leave behind for a <c>runtime: "process"</c> package, with
    /// the fixture switches baked into a wrapper script (the production spawn path has no argv seam).</summary>
    private static void WriteProcessPackage(string projectDir, string? fixtureSwitches)
    {
        var packageDir = Path.Combine(projectDir, ".pz", "packages", PackageId, PackageVersion);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint,
            $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" \"$@\"{(fixtureSwitches is null ? "" : " " + fixtureSwitches)}\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        var manifest = new Dictionary<string, object?>
        {
            ["name"] = ProcessConnector,
            ["protocolMajorMin"] = 1,
            ["protocolMajorMax"] = 1,
            ["capabilities"] = new[] { "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "SyncState" },
            ["projectDirectoryAnchor"] = true,
            ["runtime"] = "process",
            ["entrypoints"] = new Dictionary<string, string>
            {
                [RuntimeInformation.RuntimeIdentifier] = "bin/connector",
            },
        };
        File.WriteAllText(Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(manifest));

        LockFileWriter.Write(
            new LockFile(LockFileWriter.CurrentVersion, RuntimeInformation.RuntimeIdentifier, [
                new LockedPackage(PackageId, PackageVersion, "sha512-host-stats-fixture", new LockedAssets([], [])),
            ]),
            Path.Combine(projectDir, "pz.lock.json"));
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, "PcpFakeConnector");
    }

    /// <summary>Short and outside the test output tree: a unix socket path is capped near 104 bytes.</summary>
    private string NewProjectDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pzs" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _dirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _dirs)
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch (IOException)
            {
                // Best effort: a leaked temp dir is not a test failure.
            }
        }
    }
}
