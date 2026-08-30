using System.Text.Json;
using Pz.Cli;
using Pz.TestSupport;

namespace Pz.EndToEnd.Tests;

/// <summary>End-to-end: restore FakeSourceConnector (a runtime "dotnet" package) from a local NuGet
/// feed, then `pz run` refuses it with PZ0360 — external connectors are hosted out of process only,
/// and the refusal must be a clean PZ-coded config error after a successful, lock-verified restore
/// (the process-runtime install-and-RUN proof is <see cref="DeltaRsRestoreTests"/>).</summary>
[Collection("m2-local-feed")]
public sealed class RestoreRunTests(M2LocalFeedFixture feed) : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-m2-e2e-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Restored_dotnet_runtime_connector_is_refused_by_pz_run_with_PZ0360()
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

        var stderr = new StringWriter();
        var originalError = Console.Error;
        Console.SetError(stderr);
        int runExit;
        try { runExit = CliApp.Build().Parse(["run", "--project", _work]).Invoke(); }
        finally { Console.SetError(originalError); }

        Assert.Equal(ExitCodes.ConfigError, runExit);
        Assert.Contains("PZ0360", stderr.ToString());
        Assert.Contains("FakeSourceConnector", stderr.ToString());
        // Refused at registry construction: no node ever executed, so no sink output was written.
        Assert.False(File.Exists(Path.Combine(_work, "out", "total.csv")));
        await Task.CompletedTask;
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
