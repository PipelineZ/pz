using System.Text.Json.Nodes;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connectors.Toolkit.Tests.Paging;

public class PagingStrategyTests
{
    private static HttpResponseMessage Response(string? linkHeader = null)
    {
        var response = new HttpResponseMessage(System.Net.HttpStatusCode.OK);
        if (linkHeader is not null)
        {
            response.Headers.TryAddWithoutValidation("Link", linkHeader);
        }

        return response;
    }

    [Fact]
    public void Page_params_stamps_start_and_size_on_the_first_request()
    {
        var strategy = new PageParamsStrategy("page", start: 1, sizeParam: "per_page", size: 50);
        var first = strategy.FirstRequestUri(new Uri("https://api.example.com/items?q=x"));
        Assert.Equal("https://api.example.com/items?q=x&page=1&per_page=50", first.ToString());
    }

    [Fact]
    public void Page_params_first_request_omits_size_when_not_configured()
    {
        var strategy = new PageParamsStrategy("offset", start: 0, sizeParam: null, size: null);
        var first = strategy.FirstRequestUri(new Uri("https://api.example.com/items"));
        Assert.Equal("https://api.example.com/items?offset=0", first.ToString());
    }

    [Fact]
    public void First_request_is_identity_for_link_header_and_cursor_strategies()
    {
        var uri = new Uri("https://api.example.com/items?cursor=%2A");
        Assert.Equal(uri, ((IPageStrategy)new LinkHeaderStrategy()).FirstRequestUri(uri));
        Assert.Equal(uri, ((IPageStrategy)new CursorTokenStrategy("/meta/next", "cursor")).FirstRequestUri(uri));
    }

    [Fact]
    public void Page_params_increments_and_stamps_size()
    {
        var strategy = new PageParamsStrategy("page", start: 1, sizeParam: "per_page", size: 50);
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1&per_page=50"),
            Response(), body: new JsonArray());
        Assert.Equal("https://api.example.com/items?page=2&per_page=50", next!.ToString());
        var third = strategy.NextRequestUri(next, Response(), body: new JsonArray());
        Assert.Contains("page=3", third!.Query);
    }

    [Fact]
    public void Page_params_derives_the_next_page_from_the_current_request_on_a_fresh_instance()
    {
        // A resumed attempt re-enters mid-crawl from a persisted next-link with a FRESH strategy
        // instance: the next page must come from the current request's own param value, not
        // restart at start + 1.
        var strategy = new PageParamsStrategy("page", start: 1, sizeParam: "per_page", size: 50);
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=7&per_page=50"),
            Response(), body: new JsonArray());
        Assert.Equal("https://api.example.com/items?page=8&per_page=50", next!.ToString());
    }

    [Fact]
    public void Page_params_falls_back_to_the_counter_when_the_current_request_lacks_the_param()
    {
        var strategy = new PageParamsStrategy("page", start: 1, sizeParam: null, size: null);
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?q=x"),
            Response(), body: new JsonArray());
        Assert.Equal("https://api.example.com/items?q=x&page=2", next!.ToString());
    }

    [Fact]
    public void Link_header_follows_rel_next_and_stops_without_it()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.github.com/repos/x/y/issues"),
            Response("<https://api.github.com/repos/x/y/issues?page=2>; rel=\"next\", " +
                     "<https://api.github.com/repos/x/y/issues?page=5>; rel=\"last\""), null);
        Assert.Equal("https://api.github.com/repos/x/y/issues?page=2", next!.ToString());
        Assert.Null(strategy.NextRequestUri(next, Response("<https://x/y>; rel=\"prev\""), null));
        Assert.Null(strategy.NextRequestUri(next, Response(), null));
    }

    [Fact]
    public void Link_header_follows_rel_next_with_params_in_reversed_order()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1"),
            Response("<https://api.example.com/items?page=2>; type=\"text/html\"; rel=\"next\""), null);
        Assert.Equal("https://api.example.com/items?page=2", next!.ToString());
    }

    [Fact]
    public void Link_header_follows_unquoted_rel_next()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1"),
            Response("<https://api.example.com/items?page=3>; rel=next"), null);
        Assert.Equal("https://api.example.com/items?page=3", next!.ToString());
    }

    [Fact]
    public void Link_header_follows_multi_relation_value_containing_next()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1"),
            Response("<https://api.example.com/items?page=4>; rel=\"next last\""), null);
        Assert.Equal("https://api.example.com/items?page=4", next!.ToString());
    }

    [Fact]
    public void Link_header_resolves_relative_uri_against_current_request()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1"),
            Response("</items?page=2>; rel=\"next\""), null);
        Assert.Equal("https://api.example.com/items?page=2", next!.ToString());
    }

    [Fact]
    public void Link_header_returns_null_on_malformed_link_value()
    {
        var strategy = new LinkHeaderStrategy();
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1"),
            Response("garbage; rel=\"next\""), null);
        Assert.Null(next);
    }

    [Fact]
    public void Page_params_preserves_fragment()
    {
        var strategy = new PageParamsStrategy("page", start: 1, sizeParam: null, size: null);
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items?page=1#frag"),
            Response(), body: new JsonArray());
        Assert.Equal("https://api.example.com/items?page=2#frag", next!.AbsoluteUri);
    }

    [Fact]
    public void Cursor_token_extracts_and_stops_on_missing_or_empty()
    {
        var strategy = new CursorTokenStrategy("/meta/next", "cursor");
        var body = JsonNode.Parse("""{ "meta": { "next": "abc xyz" } }""");
        var next = strategy.NextRequestUri(new Uri("https://api.example.com/items"), Response(), body);
        Assert.Equal("https://api.example.com/items?cursor=abc%20xyz", next!.AbsoluteUri);
        Assert.Null(strategy.NextRequestUri(next, Response(), JsonNode.Parse("""{ "meta": {} }""")));
        Assert.Null(strategy.NextRequestUri(next, Response(), JsonNode.Parse("""{ "meta": { "next": "" } }""")));
        Assert.Null(strategy.NextRequestUri(next, Response(), null));
    }
}
