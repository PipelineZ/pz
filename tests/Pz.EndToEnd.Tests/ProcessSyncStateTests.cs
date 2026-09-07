using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;
using Pz.PackageManagement.Restore;

namespace Pz.EndToEnd.Tests;

/// <summary>The engine-level proof that a process-hosted feed connector round-trips its sync-state
/// token: run 1 lands rows and persists the token the connector minted, run 2 receives that token as
/// <c>PriorSyncState</c> and mints one that visibly builds on it. The fixture's token is
/// <c>"&lt;prior&gt;+&lt;rows&gt;"</c>, so <c>"0+4"</c> then <c>"0+4+4"</c> is the whole assertion.
/// A second fact runs the same dataset over the same fixture WITHOUT <c>--sync-state</c> and proves
/// no sync state is written, so the token above is the connector's doing, not the host's.
///
/// <para>Linux only: the staged package's entrypoint is a <c>#!/bin/sh</c> wrapper, and
/// <c>File.SetUnixFileMode</c> below is platform-gated by the analyzer.</para></summary>
[SupportedOSPlatform("linux")]
[Trait("Category", "Pcp")]
public sealed class ProcessSyncStateTests : IDisposable
{
    private const string PackageId = "LocalFilesPcp";
    private const string PackageVersion = "1.0.0";
    private const string ProcessConnector = "localfiles-pcp";

    private readonly List<string> _dirs = [];

    [SkippableFact]
    public void Feed_connector_over_pcp_persists_its_token_and_gets_it_back_next_run()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var dir = NewProjectDir();
        WriteProject(dir);
        WriteProcessPackage(dir, feedMode: true);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        var plan = File.ReadAllText(Path.Combine(dir, ".pz", "target", "plan.json"));
        Assert.Equal("arrow_stream", StrategyOf(plan, "SourceLoad"));
        Assert.Contains("read=feed", plan, StringComparison.Ordinal);

        var firstRunId = Directory.EnumerateDirectories(Path.Combine(dir, ".pz", "runs")).Select(Path.GetFileName).Single();
        var (token1, runId1) = ReadSyncState(dir);
        Assert.Equal("0+4", token1);
        Assert.Equal(firstRunId, runId1);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        var (token2, runId2) = ReadSyncState(dir);
        Assert.Equal("0+4+4", token2);
        Assert.NotEqual(runId1, runId2);
    }

    [SkippableFact]
    public void Same_fixture_without_feed_mode_resolves_full_and_writes_no_sync_state()
    {
        Skip.If(OperatingSystem.IsWindows(), "this test stages a #!/bin/sh wrapper as the package entrypoint, which is POSIX-only");

        var dir = NewProjectDir();
        WriteProject(dir);
        WriteProcessPackage(dir, feedMode: false);

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", dir]).Invoke());

        var plan = File.ReadAllText(Path.Combine(dir, ".pz", "target", "plan.json"));
        Assert.Contains("read=full", plan, StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Combine(dir, ".pz", "state", "sync-state.json")));
    }

    // --- assertions -----------------------------------------------------------------------------

    private static (string Token, string RunId) ReadSyncState(string projectDir)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(projectDir, ".pz", "state", "sync-state.json")));
        var entry = doc.RootElement.GetProperty("syncState").GetProperty("files.orders");
        return (entry.GetProperty("token").GetString()!, entry.GetProperty("runId").GetString()!);
    }

    private static string StrategyOf(string planJson, string kind)
    {
        using var plan = JsonDocument.Parse(planJson);
        return plan.RootElement.GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("kind").GetString() == kind)
            .GetProperty("strategy").GetString()!;
    }

    // --- project construction ------------------------------------------------------------------

    /// <summary>A feed dataset (no <c>sync:</c> block, so <c>mode: auto</c>, which the connector
    /// resolves to Feed) feeding an append sink with <c>duplicates: accept</c> -- the only pairing the
    /// planner's delivery-guarantee pass allows a feed to drive without a merge key (feed x replace
    /// is refused, feed x append needs the consent). Every write option lives in YAML so the sink()
    /// call carries none (PZ0341 forbids declaring the same option on both surfaces).</summary>
    private static void WriteProject(string dir)
    {
        File.WriteAllText(Path.Combine(dir, "project.yml"),
            "name: pcp_sync_state\n" +
            "version: 0.1.0\n" +
            "connectors:\n" +
            $"  - package: {PackageId}\n    version: {PackageVersion}\n" +
            "engine:\n" +
            "  threads: 2\n");

        File.WriteAllText(Path.Combine(dir, "connections.yml"), $"""
            files:
              connector: {ProcessConnector}
              entities:
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

    /// <summary>What <c>pz restore</c> would leave behind for a <c>runtime: "process"</c> package: a
    /// manifest, an executable entrypoint (a wrapper script over the fixture, with <c>--sync-state</c>
    /// baked in for feed mode -- the production spawn path has no argv seam for a switch), and a lock
    /// file the drift check accepts. The manifest's capability list must equal what the fixture's
    /// Hello reports in that mode or the handshake refuses.</summary>
    private static void WriteProcessPackage(string projectDir, bool feedMode)
    {
        var packageDir = Path.Combine(projectDir, ".pz", "packages", PackageId, PackageVersion);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        File.WriteAllText(entrypoint,
            $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\" \"$@\"{(feedMode ? " --sync-state" : "")}\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        var manifest = new Dictionary<string, object?>
        {
            ["name"] = ProcessConnector,
            ["protocolMajorMin"] = 1,
            ["protocolMajorMax"] = 1,
            ["capabilities"] = feedMode
                ? new[] { "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "SyncState" }
                : new[] { "NativeScan", "NativeCopy", "ReplaceWrites", "BoundedWindow", "PartitionedRead" },
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
                new LockedPackage(PackageId, PackageVersion, "sha512-sync-state-fixture", new LockedAssets([], [])),
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

    /// <summary>Short and outside the test output tree: the run-scoped socket root lives under
    /// <c>&lt;project&gt;/.pz/runs/&lt;id&gt;/sockets</c> and a unix socket path is capped near 104
    /// bytes.</summary>
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
