using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http.Tests;

public class HttpItemBindingTests
{
    private const string Label = "http source 's.d'";

    private static HttpConnectionConfig Connection()
    {
        var errors = new List<string>();
        var config = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "https://api.example.com/v1",
        }), errors);
        Assert.Empty(errors);
        return config!;
    }

    private static DatasetSpec Spec(Dictionary<string, object?> options, string? watermark = null,
        string? cursor = null) => new("s", "d", options) { WatermarkValue = watermark, WatermarkCursor = cursor };

    private static HttpPageRequest BuildFor(DatasetSpec spec)
    {
        var config = HttpDatasetConfig.Parse(spec);
        var bindings = HttpFirstRequest.Bindings(spec, config, "2026-11-09", Label);
        return HttpFirstRequest.Build(Connection(), config, null, bindings, Label);
    }

    private static readonly string[] Known = ["watermark", "window_upper", "run_date"];

    private static ItemValues Item(string recordJson, params string[] fields)
        => ItemValues.From(JsonNode.Parse(recordJson), [.. fields.Select(f => (f, "/" + f))]);

    private static Dictionary<string, BindingValue> Bindings(string? watermark = null) => new()
    {
        ["watermark"] = new(watermark, null),
        ["window_upper"] = new(null, null),
        ["run_date"] = new("2026-11-09", null),
    };

    // A parsed dataset reshaped with item-referencing templates: offline validation of item references
    // needs the for_each fields, which the dataset options alone do not declare.
    private static HttpDatasetConfig ItemConfig(string path, Dictionary<string, string>? query = null,
        object? body = null, string[]? fields = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/x" };
        if (body is not null)
        {
            options["method"] = "post";
            options["body"] = "placeholder";
        }

        var config = HttpDatasetConfig.Parse(Spec(options)) with
        {
            Path = path,
            Query = query ?? [],
        };
        if (body is null)
        {
            return config;
        }

        var errors = new List<string>();
        var parsed = HttpBody.Parse(body, null, null, Known, errors, fields ?? ["id"]);
        Assert.Empty(errors);
        return config with { Body = parsed };
    }

    private static HttpPageRequest BuildItem(HttpDatasetConfig config, ItemValues item, string? watermark = null)
        => HttpFirstRequest.Build(Connection(), config, null, Bindings(watermark), Label, item);

    [Fact]
    public void Path_item_value_is_escaped()
    {
        var request = BuildItem(ItemConfig("/orders/{{ item.order_id }}/lines"),
            Item("""{"order_id":"a/b c"}""", "order_id"));

        Assert.Equal("https://api.example.com/v1/orders/a%2Fb%20c/lines", request.Url.AbsoluteUri);
    }

    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    public void Dot_segments_are_refused(string value)
    {
        var ex = Assert.Throws<PzConnectorException>(() => BuildItem(ItemConfig("/orders/{{ item.order_id }}"),
            Item($$"""{"order_id":"{{value}}"}""", "order_id")));

        Assert.False(ex.IsTransient);
        Assert.Contains("order_id", ex.Message);
    }

    [Fact]
    public void Query_item_value_escaped_once()
    {
        var request = BuildItem(ItemConfig("/orders", new() { ["id"] = "{{ item.id }}" }),
            Item("""{"id":"x&y=z/"}""", "id"));

        Assert.Equal("id=x%26y%3Dz%2F", request.Url.Query.TrimStart('?'));
        Assert.DoesNotContain("%25", request.Url.AbsoluteUri);
    }

    [Theory]
    [InlineData("123", """{"id":123}""")]
    [InlineData("12.50", """{"id":12.50}""")]
    [InlineData("true", """{"id":true}""")]
    [InlineData("\"o-1\"", """{"id":"o-1"}""")]
    public void Json_body_whole_value_keeps_type(string json, string expected)
    {
        var config = ItemConfig("/search", body: new Dictionary<string, object?> { ["id"] = "{{ item.id }}" });

        var request = BuildItem(config, Item($$"""{"id":{{json}}}""", "id"));

        Assert.Equal(expected, request.JsonBody!.ToJsonString());
    }

    [Fact]
    public void Json_body_embedded_is_text()
    {
        var config = ItemConfig("/search", body: new Dictionary<string, object?> { ["ref"] = "ord-{{ item.id }}" });

        var request = BuildItem(config, Item("""{"id":123}""", "id"));

        Assert.Equal("""{"ref":"ord-123"}""", request.JsonBody!.ToJsonString());
    }

    [Fact]
    public void Number_text_is_preserved()
    {
        var item = Item("""{"id":1e3}""", "id");

        var get = BuildItem(ItemConfig("/o/{{ item.id }}", new() { ["n"] = "{{ item.id }}" }), item);
        Assert.Equal("https://api.example.com/v1/o/1e3?n=1e3", get.Url.AbsoluteUri);

        var post = BuildItem(ItemConfig("/o", body: new Dictionary<string, object?> { ["id"] = "{{ item.id }}" }), item);
        Assert.Equal("""{"id":1e3}""", post.JsonBody!.ToJsonString());
    }

    [Theory]
    [InlineData("{{ watermark }}")]
    [InlineData("a{{b")]
    [InlineData("{{ item.id }}")]
    public void Binding_lookalike_values_stay_literal(string value)
    {
        var item = ItemValues.From(new JsonObject { ["id"] = value }, [("id", "/id")]);

        var get = BuildItem(ItemConfig("/o/{{ item.id }}", new() { ["q"] = "{{ item.id }}" }), item, watermark: "42");
        Assert.Equal($"https://api.example.com/v1/o/{Uri.EscapeDataString(value)}?q={Uri.EscapeDataString(value)}",
            get.Url.AbsoluteUri);

        var post = BuildItem(ItemConfig("/o", body: new Dictionary<string, object?>
        {
            ["whole"] = "{{ item.id }}",
            ["embedded"] = "<{{ item.id }}>",
        }), item, watermark: "42");
        Assert.Equal(value, post.JsonBody!["whole"]!.GetValue<string>());
        Assert.Equal($"<{value}>", post.JsonBody!["embedded"]!.GetValue<string>());

        var text = HttpTemplate.TryExpand("{{ item.id }}|{{ watermark }}", Bindings("42"), item,
            (_, v) => v.Value!, (_, n) => HttpTemplate.ItemText(n), out var result, out var error);
        Assert.True(text);
        Assert.Null(error);
        Assert.Equal($"{value}|42", result);
    }

    [Fact]
    public void Engine_and_item_refs_together()
    {
        var config = ItemConfig("/o/{{ item.id }}", new()
        {
            ["since"] = "{{ watermark }}",
            ["key"] = "{{ item.id }}-{{ watermark }}",
            ["day"] = "{{ run_date }}",
        });
        var item = Item("""{"id":"o 7"}""", "id");

        var withWatermark = BuildItem(config, item, watermark: "42");
        Assert.Equal("https://api.example.com/v1/o/o%207?since=42&key=o%207-42&day=2026-11-09",
            withWatermark.Url.AbsoluteUri);

        var firstRun = BuildItem(config, item);
        Assert.Equal("https://api.example.com/v1/o/o%207?day=2026-11-09", firstRun.Url.AbsoluteUri);
    }

    [Theory]
    [InlineData("""{}""")]
    [InlineData("""{"id":null}""")]
    [InlineData("""{"id":{}}""")]
    [InlineData("""{"id":[]}""")]
    public void Missing_or_object_field_referenced_fails(string record)
    {
        var item = Item(record, "id");

        var expanded = HttpTemplate.TryExpand("/o/{{ item.id }}", Bindings(), item,
            (_, v) => v.Value!, (_, n) => HttpTemplate.ItemText(n), out var result, out var error);
        Assert.False(expanded);
        Assert.Null(result);
        Assert.Contains("'id'", error);

        var path = Assert.Throws<PzConnectorException>(() => BuildItem(ItemConfig("/o/{{ item.id }}"), item));
        Assert.False(path.IsTransient);
        Assert.Contains("'id'", path.Message);

        var query = Assert.Throws<PzConnectorException>(() =>
            BuildItem(ItemConfig("/o", new() { ["id"] = "{{ item.id }}" }), item));
        Assert.Contains("'id'", query.Message);

        var body = Assert.Throws<PzConnectorException>(() =>
            BuildItem(ItemConfig("/o", body: new Dictionary<string, object?> { ["id"] = "{{ item.id }}" }), item));
        Assert.Contains("'id'", body.Message);
    }

    [Fact]
    public void An_unreferenced_null_field_is_fine()
    {
        var item = Item("""{"id":"o1","note":null}""", "id", "note");

        var request = BuildItem(ItemConfig("/o/{{ item.id }}"), item);

        Assert.Equal("https://api.example.com/v1/o/o1", request.Url.AbsoluteUri);
    }

    [Fact]
    public void Item_refs_validate_offline()
    {
        string[] fields = ["order_id", "id"];

        Assert.True(HttpTemplate.TryValidate("/o/{{ item.order_id }}/{{ watermark }}", Known, fields, out var ok));
        Assert.Null(ok);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.nope }}", Known, fields, out var unknown));
        Assert.Contains("'nope'", unknown);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.id }}", Known, null, out var notHere));
        Assert.Contains("item.id", notHere);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.Id }}", Known, ["Id"], out var upper));
        Assert.Contains("'Id'", upper);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.pz_x }}", Known, ["pz_x"], out var reserved));
        Assert.Contains("'pz_x'", reserved);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.id }}/{{ bogus }}", Known, fields, out var engine));
        Assert.Contains("'bogus'", engine);

        Assert.False(HttpTemplate.TryValidate("/o/{{ item.id }}/{{", Known, fields, out var malformed));
        Assert.Contains("malformed", malformed);

        // The dataset options carry no for_each fields, so an item reference there is refused offline.
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new Dictionary<string, object?>
        {
            ["path"] = "/o/{{ item.id }}",
        })));
        Assert.Contains("item.id", ex.Message);
        Assert.DoesNotContain("malformed", ex.Message);
    }

    [Fact]
    public void Item_references_are_listed()
    {
        Assert.False(HttpTemplate.HasItemReferences("/o/{{ watermark }}"));
        Assert.True(HttpTemplate.HasItemReferences("/o/{{item.a}}"));
        Assert.Equal(["a", "b"], HttpTemplate.ItemReferences("{{ item.a }}/{{ item.b }}/{{ item.a }}/{{ run_date }}"));
    }

    [Fact]
    public void Identity_names_up_to_three_fields_flattened_and_capped()
    {
        var item = Item("""{"a":"x\r\ny","b":12.50,"c":true,"d":"ignored"}""", "a", "b", "c", "d");
        Assert.Equal("item a=x  y, b=12.50, c=true", item.Identity());

        var capped = Item($$"""{"e":"{{new string('z', 50)}}"}""", "e");
        Assert.Equal($"item e={new string('z', 40)}", capped.Identity());
    }

    [Fact]
    public void Form_body_item_value_is_encoded_once()
    {
        var options = new Dictionary<string, object?> { ["path"] = "/x", ["method"] = "post", ["body"] = "p" };
        var config = HttpDatasetConfig.Parse(Spec(options));
        var errors = new List<string>();
        var body = HttpBody.Parse(new Dictionary<string, object?> { ["id"] = "{{ item.id }}" }, "form", null,
            Known, errors, ["id"]);
        Assert.Empty(errors);

        var request = BuildItem(config with { Body = body }, Item("""{"id":"a&b=c"}""", "id"));

        Assert.Equal("id=a%26b%3Dc", request.TextBody);
    }

    [Fact]
    public void Text_body_item_value_is_plain_text()
    {
        var options = new Dictionary<string, object?> { ["path"] = "/x", ["method"] = "post", ["body"] = "p" };
        var config = HttpDatasetConfig.Parse(Spec(options));
        var errors = new List<string>();
        var body = HttpBody.Parse("id={{ item.id }}&wm={{ watermark }}", null, null, Known, errors, ["id"]);
        Assert.Empty(errors);

        var request = BuildItem(config with { Body = body }, Item("""{"id":"{{ watermark }}"}""", "id"), "42");

        Assert.Equal("id={{ watermark }}&wm=42", request.TextBody);
    }

    [Fact]
    public void Templates_without_item_refs_are_unchanged()
    {
        var get = BuildFor(Spec(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ run_date }}/a b",
            ["cursor"] = "updated",
            ["cursor_type"] = "timestamp",
            ["query"] = new Dictionary<string, object?>
            {
                ["since"] = "{{ watermark }}",
                ["until"] = "{{ window_upper }}",
                ["q"] = "x&y=z/",
                ["day"] = "d-{{ run_date }}",
            },
        }, watermark: "2026-01-02T03:04:05.678", cursor: "updated"));
        Assert.Equal(
            "https://api.example.com/v1/orders/2026-11-09/a%20b?since=2026-01-02T03%3A04%3A05Z&q=x%26y%3Dz%2F&day=d-2026-11-09",
            get.Url.AbsoluteUri);
        Assert.Null(get.JsonBody);

        var post = BuildFor(Spec(new Dictionary<string, object?>
        {
            ["path"] = "/search",
            ["method"] = "post",
            ["cursor"] = "seq",
            ["cursor_type"] = "bigint",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
            ["body"] = new Dictionary<string, object?>
            {
                ["after"] = "{{ watermark }}",
                ["label"] = "from {{ watermark }} on {{ run_date }}",
                ["upper"] = "{{ window_upper }}",
                ["size"] = 500L,
                ["nested"] = new Dictionary<string, object?> { ["d"] = "{{ run_date }}" },
            },
        }, watermark: "42", cursor: "seq"));
        Assert.Equal("https://api.example.com/v1/search?since=42", post.Url.AbsoluteUri);
        Assert.Equal("""{"after":42,"label":"from 42 on 2026-11-09","upper":null,"size":500,"nested":{"d":"2026-11-09"}}""",
            post.JsonBody!.ToJsonString());
    }
}
