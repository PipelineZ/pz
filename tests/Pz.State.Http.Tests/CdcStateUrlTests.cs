using System.Text.Json;
using Pz.Cli;
using Pz.Engine.State;
using Pz.TestSupport.State;

namespace Pz.State.Http.Tests;

/// <summary>`pz cdc status --state-url`: the stored token comes from the HTTP store even when an ambient
/// backend names another store. A localfiles cdc dataset reports "admin unsupported" but still reads the
/// stored token, which is exactly the read under test, with no cdc-capable server needed.</summary>
[Collection(ProcessEnvironment.CollectionName)]
public sealed class CdcStateUrlTests : IAsyncDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-cdc-state-url", Guid.NewGuid().ToString("N"));
    private readonly FakeStateServer _server = new();

    [Fact]
    public void Status_reads_the_token_from_the_state_url_over_an_ambient_sqlserver_backend()
    {
        WriteProject();
        SyncStore().Set("files.orders", new SyncState("tok-from-http", "run-0"));

        // An ambient backend pointing nowhere: without --state-url outranking it, the verb would try this
        // server and fail. (Outranking project.yml's state: block is pinned by StateUrlOverrideTests.)
        Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", "sqlserver");
        Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING",
            "Server=tcp:127.0.0.1,1;Database=none;User Id=x;Password=x;Connect Timeout=1;TrustServerCertificate=true");
        try
        {
            var stdout = CaptureOut(() => CliApp.Build().Parse(
                ["cdc", "status", "--project", _work, "--log-format", "json", "--state-url", _server.Url]).Invoke(),
                out var exit, out var stderr);

            Assert.True(exit == 0, $"expected exit 0, got {exit}; stderr: {stderr}");
            var line = Assert.Single(stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries));
            using var doc = JsonDocument.Parse(line);
            Assert.Equal("cdc_status", doc.RootElement.GetProperty("event").GetString());
            Assert.Equal("files.orders", doc.RootElement.GetProperty("dataset").GetString());
            Assert.False(doc.RootElement.GetProperty("adminSupported").GetBoolean());
            Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("healthy").ValueKind);
            Assert.True(doc.RootElement.GetProperty("hasStoredToken").GetBoolean());
            Assert.DoesNotContain("tok-from-http", stdout);
            Assert.Contains("note: state backend:", stderr);
            Assert.Contains(_server.Requests, r => r.Url.AbsolutePath.Contains("/state/sync-state", StringComparison.Ordinal));
        }
        finally
        {
            Environment.SetEnvironmentVariable("PZ_STATE_BACKEND", null);
            Environment.SetEnvironmentVariable("PZ_STATE_CONNECTION_STRING", null);
        }
    }

    private SyncStateStore SyncStore() => new(new HttpKeyedStateStore<SyncState>(
        new HttpStateEndpoint(_server.Url, null), "sync-state", SyncStateStore.ReadEntry, SyncStateStore.WriteEntry));

    private void WriteProject()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: cdc_state_url
            version: 0.1.0
            engine:
              threads: 1
            """);
        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            files:
              connector: localfiles
              root: "./data"
              entities:
                orders:
                  read:
                    format: csv
                    sync:
                      mode: cdc
            """);
    }

    private static string CaptureOut(Func<int> action, out int exit, out string stderr)
    {
        var stdout = new StringWriter();
        var stderrWriter = new StringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        Console.SetOut(stdout);
        Console.SetError(stderrWriter);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }

        stderr = stderrWriter.ToString();
        return stdout.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        await _server.DisposeAsync();
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }
}

/// <summary>Classes that set process environment variables or swap the console run alone.</summary>
[CollectionDefinition(CollectionName, DisableParallelization = true)]
public sealed class ProcessEnvironment
{
    public const string CollectionName = "process-environment";
}
