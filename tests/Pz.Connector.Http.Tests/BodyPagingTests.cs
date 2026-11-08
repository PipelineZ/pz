using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class BodyPagingTests
{
    private static JsonNode J(string json) => JsonNode.Parse(json)!;

    private static Dictionary<string, object?> Pagination(string strategy, params (string, object?)[] extra)
    {
        var d = new Dictionary<string, object?> { ["strategy"] = strategy, ["location"] = "body" };
        foreach (var (k, v) in extra)
        {
            d[k] = v;
        }

        return d;
    }

    private static BodyPaging Parsed(Dictionary<string, object?> pagination, string body)
    {
        var errors = new List<string>();
        var paging = BodyPaging.Parse(pagination, (string)pagination["strategy"]!, J(body), "/items", errors);
        Assert.Empty(errors);
        return Assert.IsAssignableFrom<BodyPaging>(paging);
    }

    private static string Json(JsonNode? node) => node!.ToJsonString();

    [Fact]
    public void Page_writes_start_and_size_then_increments()
    {
        var paging = Parsed(Pagination("page", ("param", "/page"), ("size_param", "/size"), ("size", 50)), "{}");
        var first = paging.First(J("{}"));
        Assert.Equal("""{"page":1,"size":50}""", Json(first));
        Assert.Equal("""{"page":2,"size":50}""", Json(paging.Next(first, J("{}"), 50)));
        Assert.True(paging.StopsOnEmptyPage);
        Assert.Null(paging.Next(first, J("{}"), 0));
    }

    [Fact]
    public void Offset_advances_by_rows_returned()
    {
        var paging = BodyPaging.Parse(Pagination("offset", ("param", "/from"), ("size_param", "/size"), ("size", 100)),
            "offset", J("{}"), "/hits/hits", []) ?? throw new InvalidOperationException();
        var first = paging.First(J("{}"));
        Assert.Equal("""{"from":0,"size":100}""", Json(first));
        var response = J("""{"hits":{"hits":[1,2,3]}}""");
        var second = paging.Next(first, response, 3);
        Assert.Equal("""{"from":3,"size":100}""", Json(second));
        Assert.Equal("""{"from":6,"size":100}""", Json(paging.Next(second!, response, 3)));
        Assert.Null(paging.Next(second!, J("""{"hits":{"hits":[]}}"""), 0));
    }

    [Fact]
    public void Cursor_copies_the_token_into_variables()
    {
        var paging = Parsed(Pagination("cursor", ("param", "/variables/after"),
            ("pointer", "/data/x/pageInfo/endCursor")), """{"query":"q","variables":{}}""");
        var body = J("""{"query":"q","variables":{}}""");
        Assert.Equal("""{"query":"q","variables":{"after":"c1"}}""",
            Json(paging.Next(body, J("""{"data":{"x":{"pageInfo":{"endCursor":"c1"}}}}"""), 5)));
        Assert.False(paging.StopsOnEmptyPage);
        Assert.Null(paging.Next(body, J("""{"data":{"x":{"pageInfo":{}}}}"""), 5));
        Assert.Null(paging.Next(body, J("""{"data":{"x":{"pageInfo":{"endCursor":null}}}}"""), 5));
        Assert.Null(paging.Next(body, J("""{"data":{"x":{"pageInfo":{"endCursor":""}}}}"""), 5));
    }

    [Fact]
    public void Cursor_copies_a_non_scalar_token_as_json()
    {
        var paging = Parsed(Pagination("cursor", ("param", "/after"), ("pointer", "/t")), "{}");
        var next = paging.Next(J("{}"), J("""{"t":[1,"a"]}"""), 1);
        Assert.Equal("""{"after":[1,"a"]}""", Json(next));
    }

    [Fact]
    public void Next_reads_its_position_from_the_current_body()
    {
        var paging = Parsed(Pagination("offset", ("param", "/from")), """{"from":0}""");
        Assert.Equal("""{"from":202}""", Json(paging.Next(J("""{"from":200}"""), J("""{"items":[1,2]}"""), 2)));
    }

    [Fact]
    public void Pointer_that_cannot_be_set_is_a_validation_error()
    {
        var errors = new List<string>();
        var paging = BodyPaging.Parse(Pagination("page", ("param", "/a/b")), "page", J("{}"), "/items", errors);
        Assert.Null(paging);
        Assert.Contains(errors, e => e.Contains("pagination 'param'"));
    }

    [Fact]
    public void Location_body_requires_post_and_a_map_body()
    {
        var errors = new List<string>();
        Assert.Null(BodyPaging.Parse(Pagination("page", ("param", "/page")), "page", null, "/items", errors));
        Assert.Contains(errors, e => e.Contains("location: body") && e.Contains("JSON map"));

        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/x", ["method"] = "post", ["body"] = "raw",
            ["pagination"] = Pagination("page", ("param", "/page")),
        });
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(spec));
        Assert.Contains("JSON map", ex.Message);
    }

    [Fact]
    public void Link_header_and_next_url_ignore_location()
    {
        var errors = new List<string>();
        Assert.Null(BodyPaging.Parse(Pagination("next_url", ("pointer", "/next")), "next_url", J("{}"), "/items", errors));
        Assert.Null(BodyPaging.Parse(Pagination("link_header"), "link_header", J("{}"), "/items", errors));
        Assert.Empty(errors);

        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/x", ["method"] = "post", ["body"] = new Dictionary<string, object?> { ["q"] = "1" },
            ["pagination"] = Pagination("next_url", ("pointer", "/next")),
        });
        var config = HttpDatasetConfig.Parse(spec);
        Assert.Null(config.BodyPaging);
        Assert.NotNull(config.PageStrategyFactory);
    }

    [Fact]
    public void Config_exposes_body_paging_for_a_post_with_a_map_body()
    {
        var spec = new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/x", ["method"] = "post", ["body"] = new Dictionary<string, object?> { ["q"] = "1" },
            ["pagination"] = Pagination("page", ("param", "/page"), ("size", 10), ("stop_on_short_page", true)),
        });
        var config = HttpDatasetConfig.Parse(spec);
        Assert.IsType<BodyPagePaging>(config.BodyPaging);
        Assert.True(config.StopOnShortPage);
        Assert.Equal(10, config.PageSize);
    }

    [Fact]
    public void Original_body_is_never_mutated()
    {
        var template = J("""{"q":"x"}""");
        var paging = Parsed(Pagination("page", ("param", "/page"), ("size_param", "/size"), ("size", 5)), """{"q":"x"}""");
        var body = paging.First(template);
        for (var i = 0; i < 2; i++)
        {
            body = paging.Next(body, J("{}"), 5)!;
        }

        Assert.Equal("""{"q":"x"}""", Json(template));
        Assert.Equal("""{"q":"x","page":3,"size":5}""", Json(body));
    }

    [Fact]
    public void Pointer_writer_unescapes_segments_and_refuses_missing_parents()
    {
        var root = J("""{"a/b":{"c~d":1},"arr":[0]}""");
        Assert.True(JsonPointerWriter.TrySet(root, "/a~1b/c~0d", JsonValue.Create(2)));
        Assert.True(JsonPointerWriter.TrySet(root, "/arr/0", JsonValue.Create(9)));
        Assert.False(JsonPointerWriter.TrySet(root, "/arr/1", JsonValue.Create(9)));
        Assert.False(JsonPointerWriter.TrySet(root, "/x/y", JsonValue.Create(9)));
        Assert.Equal("""{"a/b":{"c~d":2},"arr":[9]}""", Json(root));
    }
}
