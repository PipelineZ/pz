using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public class HttpIncrementalTests
{
    private static readonly Dictionary<string, object?> Options = new()
    {
        ["path"] = "/items",
        ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}", ["state"] = "all" },
        ["cursor"] = "updated_at",
        ["cursor_type"] = "timestamp",
    };

    private static async Task<StubHttpServer> RunAsync(DatasetSpec spec)
    {
        var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """[{"id":1,"updated_at":"2026-07-02T08:00:00Z"}]"""));
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() }), CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }

        return server;
    }

    [Fact]
    public async Task First_run_omits_the_watermark_param_entirely()
    {
        await using var server = await RunAsync(new DatasetSpec("s", "d", Options));
        var query = server.Requests.Single().Url.Query;
        Assert.DoesNotContain("since", query);
        Assert.Contains("state=all", query);
    }

    [Fact]
    public async Task Second_run_renders_timestamp_watermark_as_iso8601_z()
    {
        await using var server = await RunAsync(new DatasetSpec("s", "d", Options)
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-07-01T10:30:00.123456",
        });
        Assert.Contains("since=2026-07-01T10%3A30%3A00Z", server.Requests.Single().Url.Query);
    }

    [Fact]
    public async Task Cursor_name_mismatch_is_a_named_permanent_error()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => RunAsync(
            new DatasetSpec("s", "d", Options) { WatermarkCursor = "modified", WatermarkValue = "1" }));
        Assert.False(ex.IsTransient);
        Assert.Contains("modified", ex.Message);
        Assert.Contains("updated_at", ex.Message);
    }

    [Fact]
    public async Task Raw_incremental_execution_without_cursor_options_is_permanent()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => RunAsync(
            new DatasetSpec("s", "d", new Dictionary<string, object?> { ["path"] = "/items" })
                { WatermarkCursor = "updated_at", WatermarkValue = "1" }));
        Assert.False(ex.IsTransient);
        Assert.Contains("cursor", ex.Message);
    }

    [Fact]
    public async Task Contract_mode_with_watermark_cursor_in_columns_renders_iso8601()
    {
        var contractOptions = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
            ["columns"] = new Dictionary<string, string> { ["updated_at"] = "timestamp" },
        };

        await using var server = await RunAsync(new DatasetSpec("s", "d", contractOptions)
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-07-01T10:30:00.123456",
        });
        Assert.Contains("since=2026-07-01T10%3A30%3A00Z", server.Requests.Single().Url.Query);
    }

    [Fact]
    public async Task Contract_mode_with_missing_watermark_cursor_column_throws()
    {
        var contractOptions = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => RunAsync(
            new DatasetSpec("s", "d", contractOptions)
            {
                WatermarkCursor = "updated_at",
                WatermarkValue = "2026-07-01T10:30:00.123456",
            }));
        Assert.False(ex.IsTransient);
        Assert.Contains("updated_at", ex.Message);
        Assert.Contains("columns", ex.Message);
    }
}
