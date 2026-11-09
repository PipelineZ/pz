using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http.Tests;

public class HttpPostConfigTests
{
    private static DatasetSpec Spec(Dictionary<string, object?> options, string? watermark = null,
        string? cursor = null) => new("s", "d", options) { WatermarkValue = watermark, WatermarkCursor = cursor };

    private static Dictionary<string, object?> Post(object? body, Dictionary<string, object?>? extra = null)
    {
        var o = new Dictionary<string, object?> { ["path"] = "/search", ["method"] = "post", ["body"] = body };
        foreach (var (k, v) in extra ?? [])
        {
            o[k] = v;
        }

        return o;
    }

    private static Dictionary<string, BindingValue> Bind(string? value, string? type) => new()
    {
        ["watermark"] = new(value, type),
        ["window_upper"] = new(null, type),
    };

    private static JsonNode? Render(HttpBody body, string? wm, string? type)
    {
        var node = body.Render(Bind(wm, type), HttpPartition.FormatBinding, out var error);
        Assert.Null(error);
        return node;
    }

    private static HttpBody BodyOf(object? body, Dictionary<string, object?>? extra = null)
        => HttpDatasetConfig.Parse(Spec(Post(body, extra))).Body!;

    private static string[] Failures(Dictionary<string, object?> options)
        => Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(options))).Message
            .Split("; ");

    [Fact]
    public void Post_with_a_map_body_parses()
    {
        var config = HttpDatasetConfig.Parse(Spec(Post(new Dictionary<string, object?> { ["size"] = 500L, ["q"] = "x" })));

        Assert.Equal(HttpMethod.Post, config.Method);
        Assert.NotNull(config.Body);
        Assert.True(config.Body!.IsJson);
        Assert.Equal("application/json", config.Body.MediaType);
    }

    [Fact]
    public void Yaml_types_are_kept_in_json()
    {
        var body = BodyOf(new Dictionary<string, object?>
        {
            ["n"] = 123L, ["s"] = "0123", ["f"] = 1.5, ["b"] = true,
            ["list"] = new List<object?> { 1L, "a" },
        });

        Assert.Equal("""{"n":123,"s":"0123","f":1.5,"b":true,"list":[1,"a"]}""",
            Render(body, null, null)!.ToJsonString());
    }

    [Fact]
    public void Kwargs_types_are_kept_too()
    {
        var body = BodyOf(new Dictionary<string, object?>
        {
            ["i"] = 7, ["d"] = 2.5, ["b"] = false, ["nil"] = null,
            ["nested"] = new Dictionary<string, object?> { ["x"] = new[] { 1, 2 } },
        });

        Assert.Equal("""{"i":7,"d":2.5,"b":false,"nil":null,"nested":{"x":[1,2]}}""",
            Render(body, null, null)!.ToJsonString());
    }

    [Fact]
    public void Whole_value_binding_is_typed_by_cursor_type()
    {
        var body = BodyOf(new Dictionary<string, object?> { ["after"] = "{{ watermark }}" });

        Assert.Equal("""{"after":42}""", Render(body, "42", "bigint")!.ToJsonString());
        Assert.Equal("""{"after":"2026-01-01T00:00:00Z"}""",
            Render(body, "2026-01-01T00:00:00.5Z", "timestamp")!.ToJsonString());
        Assert.Equal("""{"after":"abc"}""", Render(body, "abc", "varchar")!.ToJsonString());
    }

    [Fact]
    public void Numeric_whole_value_binding_keeps_every_digit_and_exponent()
    {
        var body = BodyOf(new Dictionary<string, object?> { ["after"] = "{{ watermark }}" });
        var big = "123456789012345678901234567890";

        Assert.Equal($$"""{"after":{{big}}}""", Render(body, big, "bigint")!.ToJsonString());
        var exp = Render(body, "1E+30", "double")!["after"];
        Assert.Equal(JsonValueKind.Number, exp!.GetValueKind());
    }

    [Theory]
    [InlineData("hunter2-not-a-number", "bigint")]
    [InlineData("NaN", "double")]
    [InlineData("Infinity", "decimal")]
    public void Numeric_whole_value_binding_that_is_not_a_number_fails_without_the_value(string value, string type)
    {
        var body = BodyOf(new Dictionary<string, object?> { ["after"] = "{{ watermark }}" });

        body.Render(Bind(value, type), HttpPartition.FormatBinding, out var error);

        Assert.NotNull(error);
        Assert.Contains("'watermark'", error);
        Assert.Contains($"'{type}'", error);
        Assert.DoesNotContain(value, error);
    }

    [Fact]
    public void Malformed_template_at_render_time_does_not_echo_the_template()
    {
        var result = HttpBody.ExpandString("secret-text {{ Bad", Bind("1", "int"),
            HttpPartition.FormatBinding, out var error);

        Assert.Null(result);
        Assert.Contains("malformed binding template", error);
        Assert.DoesNotContain("secret-text", error);
    }

    [Fact]
    public void Yaml_plain_null_is_sent_as_the_string_null()
    {
        var body = BodyOf(new Dictionary<string, object?> { ["v"] = "null" });

        Assert.Equal("""{"v":"null"}""", Render(body, null, null)!.ToJsonString());
    }

    [Fact]
    public void Embedded_binding_renders_into_the_string()
    {
        var body = BodyOf(new Dictionary<string, object?> { ["q"] = "updated_at:>{{ watermark }}" });

        Assert.Equal("updated_at:>2026-01-01T00:00:00Z",
            (string?)Render(body, "2026-01-01T00:00:00Z", "timestamp")!["q"]);
    }

    [Fact]
    public void Null_binding_makes_the_member_null()
    {
        var body = BodyOf(new Dictionary<string, object?>
        {
            ["whole"] = "{{ watermark }}", ["embedded"] = "x>{{ watermark }}", ["keep"] = "y",
        });

        Assert.Equal("""{"whole":null,"embedded":null,"keep":"y"}""",
            Render(body, null, "bigint")!.ToJsonString());
    }

    [Fact]
    public void Render_returns_a_fresh_tree_each_call()
    {
        var body = BodyOf(new Dictionary<string, object?> { ["a"] = "{{ watermark }}" });

        var first = Render(body, "1", "int");
        var second = Render(body, "2", "int");

        Assert.Equal("""{"a":1}""", first!.ToJsonString());
        Assert.Equal("""{"a":2}""", second!.ToJsonString());
    }

    [Fact]
    public void String_body_with_a_null_binding_fails_naming_initial()
    {
        var body = BodyOf("{ q(since: \"{{ watermark }}\") }");

        var ex = Assert.Throws<PzConnectorException>(
            () => body.RenderText(Bind(null, "timestamp"), HttpPartition.FormatBinding));

        Assert.Contains("watermark", ex.Message);
        Assert.Contains("initial:", ex.Message);
        Assert.False(ex.IsTransient);
        Assert.DoesNotContain("since", ex.Message);
    }

    [Fact]
    public void String_body_renders_text()
    {
        var body = BodyOf("{ q(since: \"{{ watermark }}\") }");

        Assert.Equal("{ q(since: \"5\") }", body.RenderText(Bind("5", "int"), HttpPartition.FormatBinding));
    }

    [Fact]
    public void Form_body_renders_encoded_pairs_and_requires_a_flat_map()
    {
        var form = BodyOf(new Dictionary<string, object?>
            {
                ["a b"] = "x&y", ["n"] = 3L, ["b"] = true, ["f"] = 1.5,
            },
            new() { ["body_format"] = "form" });

        Assert.Equal("application/x-www-form-urlencoded", form.MediaType);
        Assert.Equal("a%20b=x%26y&n=3&b=true&f=1.5",
            form.RenderText(Bind(null, null), HttpPartition.FormatBinding));

        var failures = Failures(Post(new Dictionary<string, object?>
        {
            ["nested"] = new Dictionary<string, object?> { ["x"] = 1L },
        }, new() { ["body_format"] = "form" }));
        Assert.Contains(failures, f => f.Contains("flat map"));
    }

    [Fact]
    public void Content_type_overrides_for_a_string_body()
    {
        var body = BodyOf("{ q }", new() { ["content_type"] = "application/graphql" });

        Assert.Equal("application/graphql", body.MediaType);
        Assert.False(body.IsJson);
    }

    [Theory]
    [InlineData("body")]
    [InlineData("body_format")]
    [InlineData("content_type")]
    [InlineData("pagination.location")]
    public void Post_only_options_require_post(string option)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/x", ["method"] = "get" };
        switch (option)
        {
            case "body": options["body"] = "x"; break;
            case "body_format": options["body_format"] = "json"; break;
            case "content_type": options["content_type"] = "text/plain"; break;
            default:
                options["pagination"] = new Dictionary<string, object?>
                {
                    ["strategy"] = "page", ["location"] = "body",
                };
                break;
        }

        var failures = Failures(options);

        var expected = option == "pagination.location" ? "pagination.location: body" : option;
        Assert.Contains(failures, f => f.Contains($"'{expected}'") && f.Contains("'method: post'"));
    }

    [Theory]
    [InlineData("bdy")]
    [InlineData("Body")]
    [InlineData("cookie")]
    public void Post_with_an_unknown_pagination_location_is_refused(string location)
    {
        // A POST API that reads paging only from the body would answer page 1 forever if the typo were ignored.
        var failures = Failures(Post(new Dictionary<string, object?> { ["q"] = "x" }, new()
        {
            ["pagination"] = new Dictionary<string, object?>
            {
                ["strategy"] = "page", ["location"] = location, ["param"] = "/page",
            },
        }));

        Assert.Contains(failures,
            f => f.Contains($"pagination 'location' must be 'query', 'body' or 'header', got '{location}'"));
    }

    [Theory]
    [InlineData("bdy")]
    [InlineData("Body")]
    [InlineData("cookie")]
    public void Get_with_an_unknown_pagination_location_is_refused(string location)
    {
        // Ignoring it sent the position in the query string to an API that may read it elsewhere, which then
        // answers page 1 forever; `pz run` skips the schema check that would have caught the typo.
        var failures = Failures(new Dictionary<string, object?>
        {
            ["path"] = "/x",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page", ["location"] = location },
        });

        Assert.Single(failures, f => f.Contains($"pagination 'location' must be 'query', 'body' or 'header', got '{location}'"));
    }

    [Fact]
    public void An_unknown_for_each_list_location_is_refused_by_its_path()
    {
        var failures = Failures(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders", ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page", ["location"] = "qurey" },
            },
        });

        Assert.Contains(failures, f => f.Contains("pagination 'location' must be 'query', 'body' or 'header', got 'qurey'"));
    }

    [Fact]
    public void Post_with_delta_pointer_is_refused()
    {
        var failures = Failures(Post("x", new() { ["delta_pointer"] = "/@odata.deltaLink" }));

        Assert.Contains(failures, f => f.Contains("delta_pointer") && f.Contains("method: post"));
    }

    [Fact]
    public void Unknown_binding_in_body_is_reported_like_query()
    {
        var bodyFailures = Failures(Post(new Dictionary<string, object?> { ["q"] = "{{ wm }}" }));
        var queryFailures = Failures(new()
        {
            ["path"] = "/x", ["query"] = new Dictionary<string, object?> { ["q"] = "{{ wm }}" },
        });

        Assert.Contains(bodyFailures, f => f.Contains("unknown binding 'wm' — accepted: watermark, window_upper"));
        Assert.Contains(queryFailures, f => f.Contains("unknown binding 'wm' — accepted: watermark, window_upper"));
    }

    [Fact]
    public void Malformed_binding_in_body_does_not_echo_the_body()
    {
        var failures = Failures(Post(new Dictionary<string, object?> { ["q"] = "secret {{ Bad" }));

        Assert.Contains(failures, f => f.Contains("malformed binding"));
        Assert.DoesNotContain(failures, f => f.Contains("secret"));
    }

    [Fact]
    public void Method_must_be_get_or_post_and_errors_aggregate()
    {
        var failures = Failures(new()
        {
            ["path"] = "x", ["method"] = "put", ["error_pointer"] = "no-slash",
        });

        Assert.Contains(failures, f => f.Contains("'method' must be 'get' or 'post'"));
        Assert.Contains(failures, f => f.Contains("'error_pointer'"));
        Assert.Contains(failures, f => f.Contains("'path'"));
    }

    [Fact]
    public void Error_pointer_must_be_a_valid_pointer()
    {
        var config = HttpDatasetConfig.Parse(Spec(new() { ["path"] = "/x", ["error_pointer"] = "/errors" }));
        Assert.Equal("/errors", config.ErrorPointer);

        var failures = Failures(new() { ["path"] = "/x", ["error_pointer"] = "errors" });
        Assert.Contains(failures, f => f.Contains("'error_pointer' is not a valid JSON pointer"));
    }

    [Fact]
    public void ToContent_carries_the_media_type()
    {
        var body = BodyOf("{ q }", new() { ["content_type"] = "application/graphql" });

        using var content = HttpBody.ToContent(null, "{ q }", body);

        Assert.Equal("application/graphql", content.Headers.ContentType!.MediaType);
    }

    [Fact]
    public void Schema_accepts_the_post_options_and_rejects_a_bad_method()
    {
        var schema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);

        var valid = JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/q","method":"post","body":{"a":1},"body_format":"json","content_type":"text/plain","error_pointer":"/errors"}""");
        Assert.True(schema.Evaluate(valid).IsValid);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/q","method":"post","body":"{ q }"}""")).IsValid);

        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/q","method":"put"}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/q","body_format":"xml"}""")).IsValid);
    }

    [Fact]
    public void Get_dataset_parses_exactly_as_before()
    {
        var config = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/issues",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
            ["cursor"] = "updated_at",
            ["cursor_type"] = "timestamp",
        }));

        Assert.Equal(HttpMethod.Get, config.Method);
        Assert.False(config.IsPost);
        Assert.Null(config.Body);
        Assert.Null(config.ErrorPointer);
    }
}
