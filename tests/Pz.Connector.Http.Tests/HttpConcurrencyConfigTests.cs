using System.Text.Json;
using Json.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpConcurrencyConfigTests
{
    private static Dictionary<string, object?> Pagination(string strategy, object? concurrency)
    {
        var block = new Dictionary<string, object?> { ["strategy"] = strategy };
        switch (strategy)
        {
            case "cursor":
                block["pointer"] = "/next";
                block["param"] = "after";
                break;
            case "next_url":
                block["pointer"] = "/next";
                break;
        }

        if (concurrency is not null)
        {
            block["concurrency"] = concurrency;
        }

        return block;
    }

    private static Dictionary<string, object?> Plain(Dictionary<string, object?>? pagination,
        Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/orders", ["items"] = "/data" };
        if (pagination is not null)
        {
            options["pagination"] = pagination;
        }

        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return options;
    }

    private static Dictionary<string, object?> ForEachDataset(object? forEachConcurrency = null,
        Dictionary<string, object?>? datasetPagination = null, Dictionary<string, object?>? listPagination = null)
    {
        var block = new Dictionary<string, object?>
        {
            ["path"] = "/orders",
            ["items"] = "/data",
            ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
        };
        if (forEachConcurrency is not null)
        {
            block["concurrency"] = forEachConcurrency;
        }

        if (listPagination is not null)
        {
            block["pagination"] = listPagination;
        }

        var options = new Dictionary<string, object?> { ["path"] = "/orders/{{ item.id }}/lines", ["for_each"] = block };
        if (datasetPagination is not null)
        {
            options["pagination"] = datasetPagination;
        }

        return options;
    }

    private static string Message(Dictionary<string, object?> options, string? cursor = null)
        => Assert.Throws<PzConnectorException>(() => Parse(options, cursor)).Message;

    private static HttpDatasetConfig Parse(Dictionary<string, object?> options, string? cursor = null)
        => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options) { WatermarkCursor = cursor });

    [Fact]
    public void Defaults_are_one()
    {
        Assert.Equal(1, Parse(Plain(Pagination("page", null))).PageConcurrency);
        Assert.Equal(1, Parse(Plain(null)).PageConcurrency);
        Assert.Equal(1, Parse(ForEachDataset()).ForEach!.Concurrency);
    }

    [Fact]
    public void Accepted_values_parse()
    {
        Assert.Equal(16, Parse(Plain(Pagination("page", 16L))).PageConcurrency);
        Assert.Equal(4, Parse(ForEachDataset(4L)).ForEach!.Concurrency);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(17L)]
    [InlineData("x")]
    public void Range_is_1_to_16(object value)
    {
        Assert.Contains("pagination.concurrency must be an integer from 1 to 16",
            Message(Plain(Pagination("page", value))));
        Assert.Contains("for_each.concurrency must be an integer from 1 to 16",
            Message(ForEachDataset(value)));
    }

    [Theory]
    [InlineData("offset")]
    [InlineData("cursor")]
    [InlineData("next_url")]
    [InlineData("link_header")]
    public void Page_concurrency_needs_page_strategy(string strategy)
        => Assert.Contains("pagination.concurrency above 1 needs strategy: page",
            Message(Plain(Pagination(strategy, 2L))));

    [Fact]
    public void Page_concurrency_of_one_is_allowed_with_any_strategy()
        => Assert.Equal(1, Parse(Plain(Pagination("cursor", 1L))).PageConcurrency);

    [Fact]
    public void Page_concurrency_not_with_for_each()
    {
        const string expected = "pagination.concurrency is not supported with for_each; use for_each.concurrency";

        Assert.Contains(expected, Message(ForEachDataset(datasetPagination: Pagination("page", 2L))));
        Assert.Contains(expected, Message(ForEachDataset(listPagination: Pagination("page", 2L))));
    }

    [Fact]
    public void Page_concurrency_not_with_delta_pointer()
        => Assert.Contains("pagination.concurrency cannot be combined with delta_pointer",
            Message(Plain(Pagination("page", 2L), new() { ["delta_pointer"] = "/delta" })));

    [Fact]
    public void For_each_concurrency_allowed_incremental()
    {
        var config = Parse(ForEachDataset(4L), cursor: "updated");

        Assert.Equal(4, config.ForEach!.Concurrency);
    }

    [Fact]
    public void Schema_has_concurrency()
    {
        var text = new HttpConnector().DatasetConfigSchema;
        var schema = JsonSchema.FromText(text);

        Assert.Equal(3, text.Split("\"concurrency\"").Length - 1);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>("""
            {"path":"/o","pagination":{"strategy":"page","concurrency":16}}
            """)).IsValid);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>("""
            {"path":"/o/{{ item.id }}","for_each":{"path":"/o","fields":{"id":"/id"},"concurrency":4,
             "pagination":{"strategy":"page","concurrency":2}}}
            """)).IsValid);

        foreach (var bad in new[]
        {
            """{"path":"/o","pagination":{"strategy":"page","concurrency":0}}""",
            """{"path":"/o","pagination":{"strategy":"page","concurrency":17}}""",
            """{"path":"/o","for_each":{"path":"/o","fields":{"id":"/id"},"concurrency":0}}""",
            """{"path":"/o","for_each":{"path":"/o","fields":{"id":"/id"},"concurrency":17}}""",
            """{"path":"/o","for_each":{"path":"/o","fields":{"id":"/id"},"pagination":{"strategy":"page","concurrency":17}}}""",
        })
        {
            Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(bad)).IsValid, bad);
        }
    }
}
