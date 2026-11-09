using System.Text.Json;
using Pz.Cli;
using Pz.Connectors.TestKit;

namespace Pz.EndToEnd.Tests;

/// <summary>The case the connector cannot see: a raw-mode http dataset whose incremental is declared only in
/// pipeline SQL (a <c>watermark()</c> floor and an inclusive constant ceiling). On its first run no watermark reaches
/// the connector, so an <c>on_status</c> skip rule is admitted and fires; the engine must fail the SourceLoad with
/// PZ0540, store no watermark, and leave the sink unwritten.</summary>
[Collection("console-redirection")]
public sealed class HttpSkipRuleEndToEndTests : IAsyncLifetime
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-http-skip", Guid.NewGuid().ToString("N"));
    private readonly StubHttpServer _server = new();

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void A_first_run_of_a_sql_declared_incremental_whose_read_a_rule_skipped_fails_with_PZ0540()
    {
        _server.Map("/items", _ => new StubResponse(400, """{"error":"No data for period"}"""));
        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: http_skip_e2e
            version: 0.1.0
            engine:
              threads: 2
            """);
        File.WriteAllText(Path.Combine(_work, "connections.yml"), $"""
            api:
              connector: http
              base_url: {_server.BaseUrl}
              entities:
                items:
                  read:
                    path: /items
                    on_status:
                      - status: 400
                        body_contains: no data
                        action: skip

            lake:
              connector: localfiles
            """);
        File.WriteAllText(Path.Combine(_work, "pipelines", "items_load.sql"),
            "INSERT INTO {{ sink('lake', 'items_out', strategy: 'append', duplicates: 'accept', format: 'parquet', path: 'out/') }}\n" +
            "select payload, pz_fetched_at from {{ source('api', 'items') }}\n" +
            "where pz_fetched_at > {{ watermark('api', 'items') }}\n" +
            "  and pz_fetched_at <= timestamp '2100-01-01 00:00:00'\n");

        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();

        Assert.Equal(ExitCodes.NodeFailures, exit);
        var runDir = Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")).Single();
        using var results = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
        var byKind = results.RootElement.GetProperty("nodes").EnumerateArray()
            .ToDictionary(n => n.GetProperty("kind").GetString()!);
        var load = byKind["SourceLoad"];
        Assert.Equal("failed", load.GetProperty("status").GetString());
        Assert.Equal("PZ0540", load.GetProperty("error").GetProperty("code").GetString());
        Assert.Contains("on_status[0] (skip) matched HTTP 400", load.GetProperty("error").GetProperty("message").GetString());
        Assert.Equal("skipped", byKind["SinkWrite"].GetProperty("status").GetString());
        Assert.Single(_server.Requests); // permanent: not retried
        Assert.False(Directory.Exists(Path.Combine(_work, "out")));
        var watermarks = Path.Combine(_work, ".pz", "state", "watermarks.json");
        Assert.True(!File.Exists(watermarks) || !File.ReadAllText(watermarks).Contains("api.items"));
    }
}
