using System.Text.Json;
using Pz.Cli;
using Pz.TestSupport;

namespace Pz.EndToEnd.Tests;

/// <summary>End-to-end: restore FakeSourceConnector from a local NuGet feed, then `pz run`
/// executes it for real through the ALC-based ConnectorHost — proving connectors install from NuGet
/// reproducibly and RUN, not just resolve/lock.</summary>
[Collection("m2-local-feed")]
public sealed class RestoreRunTests(M2LocalFeedFixture feed) : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-m2-e2e-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Restored_connector_executes_through_pz_run()
    {
        Directory.CreateDirectory(_work);
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), $"""
            name: m2_e2e
            version: 0.1.0

            connectors:
              - package: FakeSourceConnector
                version: 1.2.3
              - package: Pz.Connector.LocalFiles
                version: 0.1.0

            engine:
              threads: 2
            """);

        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            gen:
              connector: fakesource
              entities:
                numbers:
                  read:
                    rows: 5
                    columns:
                      id: bigint
                      name: varchar
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "total.sql"),
            "INSERT INTO {{ sink('out', 'total', strategy: 'replace', format: 'csv', path: 'out/') }}\n"
            + "select count(*) as n, sum(id) as total from {{ source('gen', 'numbers') }}\n");

        File.AppendAllText(Path.Combine(_work, "connections.yml"), """

            out:
              connector: localfiles
            """);

        var restoreExit = CliApp.Build().Parse(["restore", "--project", _work, "--feeds", feed.FeedDir]).Invoke();
        Assert.Equal(ExitCodes.Ok, restoreExit);
        Assert.True(File.Exists(Path.Combine(_work, "pz.lock.json")));
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "packages", "FakeSourceConnector", "1.2.3", "lib", "FakeSourceConnector.dll")));

        var runExit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, runExit);

        var csvPath = Path.Combine(_work, "out", "total.csv");
        Assert.True(File.Exists(csvPath));

        var lines = await File.ReadAllLinesAsync(csvPath);
        Assert.Equal(2, lines.Length); // header + one data row
        var fields = lines[1].Split(',');
        Assert.Equal(5, int.Parse(fields[0]));
        Assert.Equal(15, int.Parse(fields[1])); // sum(1..5)

        var runsDir = Path.Combine(_work, ".pz", "runs");
        var runDir = Directory.EnumerateDirectories(runsDir).Single();
        using var runResults = JsonDocument.Parse(await File.ReadAllBytesAsync(Path.Combine(runDir, "run_results.json")));
        Assert.Equal("success", runResults.RootElement.GetProperty("status").GetString());
        foreach (var node in runResults.RootElement.GetProperty("nodes").EnumerateArray())
        {
            Assert.Equal("success", node.GetProperty("status").GetString());
        }
    }
}

/// <summary>Packs the FakeSourceConnector/FakeTransitiveDep fixture projects into a local feed directory
/// once per test class, via the shared <see cref="Pz.TestSupport.LocalFeed"/> packing helper (see its own
/// doc comment for the hoist rationale).</summary>
public sealed class M2LocalFeedFixture : IDisposable
{
    public string FeedDir { get; }
    private readonly string _workRoot;

    public M2LocalFeedFixture()
    {
        _workRoot = Path.Combine(Path.GetTempPath(), "pz-m2-e2e-tests", "feed-" + Guid.NewGuid().ToString("N"));
        FeedDir = Path.Combine(_workRoot, "feed");
        Directory.CreateDirectory(FeedDir);
        LocalFeed.Pack(FeedDir, "FakeTransitiveDep", version: null);
        LocalFeed.Pack(FeedDir, "FakeSourceConnector", "1.2.3", versionProperty: "FakeSourceConnectorVersion");
    }

    public void Dispose()
    {
        try { Directory.Delete(_workRoot, recursive: true); } catch { /* best effort */ }
    }
}

[CollectionDefinition("m2-local-feed")]
public class M2LocalFeedCollection : ICollectionFixture<M2LocalFeedFixture>;
