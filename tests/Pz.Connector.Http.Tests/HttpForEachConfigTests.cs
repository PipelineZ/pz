using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpForEachConfigTests
{
    private const string IncrementalMessage =
        "for_each on an incremental dataset cannot use {0}: a cut-short item would be skipped for good once the watermark passes it";

    private static DatasetSpec Spec(Dictionary<string, object?> options, string? cursor = null)
        => new("s", "d", options) { WatermarkCursor = cursor };

    private static Dictionary<string, object?> ForEach(Dictionary<string, object?>? extra = null)
    {
        var block = new Dictionary<string, object?>
        {
            ["path"] = "/orders",
            ["items"] = "/data",
            ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
        };
        foreach (var (k, v) in extra ?? [])
        {
            block[k] = v;
        }

        return block;
    }

    private static Dictionary<string, object?> Dataset(Dictionary<string, object?>? forEachExtra = null,
        Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}/lines",
            ["for_each"] = ForEach(forEachExtra),
        };
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return options;
    }

    private static string Message(Dictionary<string, object?> options, string? cursor = null)
        => Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(options, cursor))).Message;

    [Fact]
    public void Minimal_for_each_parses()
    {
        var config = HttpDatasetConfig.Parse(Spec(Dataset()));

        Assert.True(config.IsForEach);
        var forEach = config.ForEach!;
        Assert.Equal("/orders", forEach.List!.Path);
        Assert.Equal("/data", forEach.List!.ItemsPointer);
        Assert.Equal([("id", "/id")], forEach.Fields);
        Assert.Null(forEach.MaxItems);
        Assert.False(forEach.OnMissingSkip);
        Assert.Equal("/orders/{{ item.id }}/lines", config.Path);
    }

    [Fact]
    public void Dataset_without_for_each_is_not_for_each()
    {
        var config = HttpDatasetConfig.Parse(Spec(new() { ["path"] = "/x" }));

        Assert.False(config.IsForEach);
        Assert.Null(config.ForEach);
    }

    [Fact]
    public void Fields_required_and_named()
    {
        var missing = ForEach();
        missing.Remove("fields");
        Assert.Contains("for_each.fields is required",
            Message(new() { ["path"] = "/x", ["for_each"] = missing }));

        Assert.Contains("for_each.fields must declare at least one field",
            Message(Dataset(new() { ["fields"] = new Dictionary<string, object?>() }, new() { ["path"] = "/x" })));

        var message = Message(Dataset(new()
        {
            ["fields"] = new Dictionary<string, object?>
            {
                ["Order"] = "/a", ["pz_id"] = "/b", ["9x"] = "/c", ["ok"] = "no-slash",
            },
        }, new() { ["path"] = "/x" }));
        Assert.Contains("for_each.fields name 'Order' is not valid", message);
        Assert.Contains("for_each.fields name 'pz_id' is not valid", message);
        Assert.Contains("for_each.fields name '9x' is not valid", message);
        Assert.Contains("'for_each.fields.ok' is not a valid JSON pointer", message);
    }

    [Fact]
    public void List_request_refuses_item_only_keys()
    {
        var message = Message(Dataset(new()
        {
            ["file"] = new Dictionary<string, object?> { ["format"] = "csv" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "varchar" },
            ["cursor"] = "id",
            ["delta_pointer"] = "/delta",
            ["for_each"] = ForEach(),
            ["run_date"] = new Dictionary<string, object?> { ["offset_days"] = -1L },
            ["query"] = new Dictionary<string, object?> { ["after"] = "{{ item.id }}" },
        }));

        foreach (var key in new[] { "file", "columns", "cursor", "delta_pointer", "for_each", "run_date" })
        {
            Assert.Contains($"for_each.{key} is not allowed in the list request", message);
        }

        Assert.Contains("query option 'after': '{{ item.id }}' is only valid in the for_each item request", message);
    }

    [Theory]
    [InlineData("/o/{{ item.id }}?x=1")]
    [InlineData("/o/{{ item.id }}#frag")]
    public void Item_path_refuses_query_and_fragment(string path)
    {
        var message = Message(Dataset(extra: new() { ["path"] = path }));

        Assert.Contains("path cannot contain '?' or '#' in a for_each dataset; put query parameters in query:", message);
    }

    [Fact]
    public void Item_refs_must_name_fields()
    {
        var message = Message(Dataset(extra: new() { ["path"] = "/o/{{ item.nope }}" }));

        Assert.Contains("unknown item field 'nope'", message);
    }

    [Fact]
    public void Item_refs_are_allowed_in_the_item_query_and_body()
    {
        var config = HttpDatasetConfig.Parse(Spec(Dataset(extra: new()
        {
            ["query"] = new Dictionary<string, object?> { ["order"] = "{{ item.id }}" },
            ["method"] = "post",
            ["body"] = new Dictionary<string, object?> { ["id"] = "{{ item.id }}" },
        })));

        Assert.True(config.IsForEach);
    }

    [Fact]
    public void Item_refs_without_for_each_are_refused()
    {
        var message = Message(new() { ["path"] = "/o/{{ item.id }}" });

        Assert.Contains("is only valid in the for_each item request", message);
    }

    [Fact]
    public void Not_with_delta_or_export()
    {
        Assert.Contains("'for_each' cannot be combined with 'delta_pointer'",
            Message(Dataset(extra: new() { ["delta_pointer"] = "/delta" })));

        Assert.Contains("'for_each' cannot be combined with 'export'",
            Message(Dataset(extra: new()
            {
                ["file"] = new Dictionary<string, object?> { ["format"] = "csv" },
                ["export"] = new Dictionary<string, object?>
                {
                    ["job_id"] = "/id", ["status_path"] = "/jobs/{job_id}", ["status_pointer"] = "/s",
                    ["ready"] = new List<object?> { "done" }, ["download_pointer"] = "/url",
                },
            })));
    }

    [Fact]
    public void Incremental_refuses_caps()
    {
        var message = Message(Dataset(new() { ["max_items"] = 10L, ["max_pages"] = 2L },
            new() { ["max_pages"] = 3L }), cursor: "updated_at");

        var failures = message.Split("; ");
        foreach (var key in new[] { "max_items", "for_each.max_pages", "max_pages" })
        {
            Assert.Single(failures, f => f.Contains(string.Format(IncrementalMessage, key), StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData("list_query")]
    [InlineData("item_path")]
    [InlineData("item_body")]
    [InlineData("cursor")]
    public void Sql_declared_incremental_refuses_caps(string where)
    {
        // A SQL-declared incremental has no WatermarkCursor on its first run or a full refresh; a watermark
        // reference or a raw cursor still marks it incremental.
        var forEach = new Dictionary<string, object?> { ["max_items"] = 5L };
        var extra = new Dictionary<string, object?>();
        switch (where)
        {
            case "list_query":
                forEach["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" };
                break;
            case "item_path":
                extra["path"] = "/orders/{{ item.id }}/lines/{{ window_upper }}";
                break;
            case "item_body":
                extra["method"] = "post";
                extra["body"] = new Dictionary<string, object?>
                    { ["filter"] = new Dictionary<string, object?> { ["after"] = "{{watermark}}" } };
                break;
            default:
                extra["cursor"] = "updated_at";
                extra["cursor_type"] = "timestamp";
                break;
        }

        Assert.Contains(string.Format(IncrementalMessage, "max_items"), Message(Dataset(forEach, extra)));
    }

    [Fact]
    public void Non_incremental_allows_caps()
    {
        var config = HttpDatasetConfig.Parse(Spec(Dataset(new() { ["max_items"] = 10L, ["max_pages"] = 2L },
            new() { ["max_pages"] = 3L })));

        Assert.Equal(10, config.ForEach!.MaxItems);
        Assert.Equal(2, config.ForEach.List!.MaxPages);
        Assert.Equal(3, config.MaxPages);
    }

    [Fact]
    public void Max_items_must_be_a_positive_integer()
    {
        Assert.Contains("for_each.max_items must be a positive integer, got '0'",
            Message(Dataset(new() { ["max_items"] = 0L })));
    }

    [Fact]
    public void On_missing_values()
    {
        Assert.False(HttpDatasetConfig.Parse(Spec(Dataset(new() { ["on_missing"] = "error" }))).ForEach!.OnMissingSkip);
        Assert.True(HttpDatasetConfig.Parse(Spec(Dataset(new() { ["on_missing"] = "skip" }))).ForEach!.OnMissingSkip);

        Assert.Contains("for_each.on_missing must be 'error' or 'skip', got 'ignore'",
            Message(Dataset(new() { ["on_missing"] = "ignore" })));
    }

    [Fact]
    public void Raw_cursor_pz_item_reserved()
    {
        Assert.Contains("'cursor' must not use the reserved 'pz_' prefix",
            Message(Dataset(extra: new() { ["cursor"] = "pz_item", ["cursor_type"] = "varchar" })));
    }

    [Fact]
    public void For_each_must_be_a_map()
    {
        Assert.Contains("'for_each' must be a map",
            Message(new() { ["path"] = "/x", ["for_each"] = "nope" }));
    }

    [Fact]
    public void List_request_errors_name_the_block()
    {
        var block = ForEach();
        block.Remove("path");

        Assert.Contains("for_each: option 'path' is required",
            Message(new() { ["path"] = "/x", ["for_each"] = block }));
    }

    [Fact]
    public void Schema_has_for_each()
    {
        var text = new HttpConnector().DatasetConfigSchema;
        var schema = JsonSchema.FromText(text);

        Assert.Contains("\"for_each\"", text);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>("""
            {"path":"/o/{{ item.id }}","for_each":{"path":"/orders","items":"/data",
             "pagination":{"strategy":"page"},"fields":{"id":"/id"},"max_items":5,"on_missing":"skip",
             "max_pages":2,"query":{"a":"b"}}}
            """)).IsValid);

        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"path":"/o","fields":{"id":"/id"},"cursor":"id"}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"path":"/o","fields":{"id":"/id"},"on_missing":"ignore"}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"path":"/o","fields":{"id":"/id"},"max_items":0}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"path":"/o"}}""")).IsValid);
    }
}
