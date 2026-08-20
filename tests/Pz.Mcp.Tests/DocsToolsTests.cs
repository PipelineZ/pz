using System.Net;
using System.Text.Json;
using Pz.Mcp.Docs;
using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>The pz_docs_* tools and the catalog behind them. Every test runs over a stub handler,
/// never a network: these are the only tools that reach outward, so their offline behaviour is
/// exactly what needs pinning.</summary>
public class DocsToolsTests
{
    private const string Index = """
        # PipelineZ (pz)

        > A CLI for batch ETL/ELT.

        ## Concepts

        - [The data plane](https://pipelinez.dev/concepts/data-plane/): How bytes move through pz.
        - [Delivery guarantees](https://pipelinez.dev/concepts/delivery-guarantees/): What replay costs you.

        ## How-to guides

        - [Use Google Cloud Storage](https://pipelinez.dev/how-to/gcs/)

        ## Full text

        - [All pages, full markdown](https://pipelinez.dev/llms-full.txt)
        """;

    private const string FullText = """
        # PipelineZ (pz) — complete documentation

        ===== pz-doc: concepts/data-plane | https://pipelinez.dev/concepts/data-plane/ =====

        # The data plane

        DuckDB is the hub. Sources land into staging.

        ## Two tiers

        The native tier hands DuckDB a SQL fragment.

        ===== pz-doc: concepts/delivery-guarantees | https://pipelinez.dev/concepts/delivery-guarantees/ =====

        # Delivery guarantees

        An append sink is at-least-once; PZ0214 demands consent.

        ===== pz-doc: how-to/gcs | https://pipelinez.dev/how-to/gcs/ =====

        # Use Google Cloud Storage

        Set the endpoint override.
        """;

    private sealed class StubHandler(Func<string, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Calls++;
            return Task.FromResult(respond(request.RequestUri!.AbsolutePath));
        }
    }

    private static HttpResponseMessage Text(string body) =>
        new(HttpStatusCode.OK) { Content = new StringContent(body) };

    private static (DocsCatalog Catalog, StubHandler Handler) Catalog()
    {
        var handler = new StubHandler(path => path switch
        {
            "/llms.txt" => Text(Index),
            "/llms-full.txt" => Text(FullText),
            _ => new HttpResponseMessage(HttpStatusCode.NotFound),
        });
        return (new DocsCatalog(new HttpClient(handler), "https://pipelinez.dev"), handler);
    }

    private static JsonElement Parse(string envelope) => JsonDocument.Parse(envelope).RootElement;

    [Fact]
    public void ParseIndex_reads_title_description_group_and_slug()
    {
        var pages = DocsCatalog.ParseIndex(Index, "https://pipelinez.dev");

        Assert.Equal(3, pages.Count);
        var first = pages[0];
        Assert.Equal("concepts/data-plane", first.Slug);
        Assert.Equal("The data plane", first.Title);
        Assert.Equal("How bytes move through pz.", first.Description);
        Assert.Equal("Concepts", first.Group);
        Assert.Equal("How-to guides", pages[2].Group);
    }

    [Fact]
    public void ParseIndex_keeps_a_page_that_has_no_description()
    {
        // A page whose body opens with an image or table produces no summary. Dropping the line for
        // that would hide a real page from every agent that lists the docs.
        var pages = DocsCatalog.ParseIndex(Index, "https://pipelinez.dev");

        var gcs = Assert.Single(pages, p => p.Slug == "how-to/gcs");
        Assert.Equal(string.Empty, gcs.Description);
    }

    [Fact]
    public void ParseIndex_excludes_the_full_text_pointer()
    {
        // llms.txt links to llms-full.txt as an ordinary list item, but it is the corpus, not a page.
        var pages = DocsCatalog.ParseIndex(Index, "https://pipelinez.dev");

        Assert.DoesNotContain(pages, p => p.Slug.Contains("llms", StringComparison.Ordinal));
    }

    [Fact]
    public void ParseFullText_splits_every_page_on_the_delimiter()
    {
        var bodies = DocsCatalog.ParseFullText(FullText);

        Assert.Equal(3, bodies.Count);
        Assert.Contains("DuckDB is the hub", bodies["concepts/data-plane"]);
        // The next delimiter must not bleed into the previous page.
        Assert.DoesNotContain("Delivery guarantees", bodies["concepts/data-plane"]);
    }

    [Fact]
    public async Task Catalog_fetches_each_endpoint_at_most_once()
    {
        var (catalog, handler) = Catalog();

        await catalog.AllWithBodiesAsync(CancellationToken.None);
        await catalog.AllWithBodiesAsync(CancellationToken.None);
        await catalog.IndexAsync(CancellationToken.None);

        Assert.Equal(2, handler.Calls);
    }

    [Fact]
    public void Search_ranks_a_title_match_above_a_body_match()
    {
        var pages = new[]
        {
            new DocPage("a", "Unrelated", "", "u", "") { Body = "the data plane appears only in prose here" },
            new DocPage("b", "The data plane", "", "d", "") { Body = "nothing relevant" },
        };

        var hits = DocsSearch.Rank(pages, "data plane", 10);

        Assert.Equal("b", hits[0].Page.Slug);
    }

    [Fact]
    public void Search_finds_an_error_code_in_body_text()
    {
        // The case that motivates lexical over semantic search: an agent looking up "PZ0214".
        var pages = new[]
        {
            new DocPage("g", "Delivery guarantees", "", "u", "")
            {
                Body = "An append sink is at-least-once; PZ0214 demands consent.",
            },
        };

        var hits = DocsSearch.Rank(pages, "PZ0214", 10);

        Assert.Single(hits);
        Assert.Contains("PZ0214", hits[0].Excerpts[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Search_breaks_score_ties_by_slug_so_repeated_queries_agree()
    {
        var pages = new[]
        {
            new DocPage("zebra", "Match", "", "z", ""),
            new DocPage("alpha", "Match", "", "a", ""),
        };

        var hits = DocsSearch.Rank(pages, "match", 10);

        Assert.Equal(["alpha", "zebra"], hits.Select(h => h.Page.Slug));
    }

    [Fact]
    public async Task List_returns_every_page()
    {
        var (catalog, _) = Catalog();

        var result = Parse(await DocsTools.ListAsync(catalog, CancellationToken.None));

        Assert.True(result.GetProperty("ok").GetBoolean());
        Assert.Equal(3, result.GetProperty("docs").GetArrayLength());
    }

    [Fact]
    public async Task Get_returns_the_page_markdown()
    {
        var (catalog, _) = Catalog();

        var result = Parse(await DocsTools.GetAsync(catalog, "concepts/data-plane", CancellationToken.None));

        Assert.True(result.GetProperty("ok").GetBoolean());
        var doc = result.GetProperty("doc");
        Assert.Equal("concepts/data-plane", doc.GetProperty("slug").GetString());
        Assert.Contains("DuckDB is the hub", doc.GetProperty("markdown").GetString());
    }

    [Fact]
    public async Task Get_tolerates_a_slug_with_surrounding_slashes()
    {
        var (catalog, _) = Catalog();

        var result = Parse(await DocsTools.GetAsync(catalog, "/concepts/data-plane/", CancellationToken.None));

        Assert.True(result.GetProperty("ok").GetBoolean());
    }

    [Fact]
    public async Task Get_reports_an_unknown_slug_as_a_request_error_not_a_network_error()
    {
        var (catalog, _) = Catalog();

        var result = Parse(await DocsTools.GetAsync(catalog, "concepts/nope", CancellationToken.None));

        Assert.False(result.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0608", result.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task Search_reports_an_empty_query_rather_than_returning_nothing()
    {
        var (catalog, _) = Catalog();

        var result = Parse(await DocsTools.SearchAsync(catalog, "   ", 10, CancellationToken.None));

        Assert.False(result.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0608", result.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public async Task An_unreachable_site_is_PZ0607_and_names_the_url()
    {
        // The offline case. It must be a real error naming the host, never an empty result that
        // reads like "there is no such documentation".
        var handler = new StubHandler(_ => throw new HttpRequestException("no route to host"));
        var catalog = new DocsCatalog(new HttpClient(handler), "https://docs.example.invalid");

        var result = Parse(await DocsTools.ListAsync(catalog, CancellationToken.None));

        Assert.False(result.GetProperty("ok").GetBoolean());
        var error = result.GetProperty("errors")[0];
        Assert.Equal("PZ0607", error.GetProperty("code").GetString());
        Assert.Contains("docs.example.invalid", error.GetProperty("message").GetString()!, StringComparison.Ordinal);
        Assert.Contains("PZ_DOCS_URL", error.GetProperty("next_step").GetString()!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_http_error_status_is_also_PZ0607()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.ServiceUnavailable));
        var catalog = new DocsCatalog(new HttpClient(handler), "https://pipelinez.dev");

        var result = Parse(await DocsTools.ListAsync(catalog, CancellationToken.None));

        Assert.False(result.GetProperty("ok").GetBoolean());
        Assert.Equal("PZ0607", result.GetProperty("errors")[0].GetProperty("code").GetString());
    }

    [Fact]
    public void A_mirror_url_overrides_the_public_site()
    {
        var catalog = new DocsCatalog(new HttpClient(new StubHandler(_ => Text(""))), "https://mirror.internal/docs/");

        // Trailing slash trimmed, so paths concatenate to one slash rather than two.
        Assert.Equal("https://mirror.internal/docs", catalog.BaseUrl);
    }

    /// <summary>Mirroring copies the site's files; it does not rewrite the canonical host inside them.
    /// So a mirror's llms.txt still links to pipelinez.dev, and the slug has to come from the path
    /// rather than from whatever prefix the fetch happened to use. Getting this wrong keys the whole
    /// catalog by absolute URL, which no caller can guess -- every <c>pz_docs_get</c> then answers
    /// PZ0608 for a page that is right there in the listing.</summary>
    [Fact]
    public async Task Slugs_stay_relative_when_a_mirror_serves_canonically_linked_pages()
    {
        var handler = new StubHandler(path => path switch
        {
            "/docs/llms.txt" => Text(Index),
            "/docs/llms-full.txt" => Text(FullText),
            _ => new HttpResponseMessage(HttpStatusCode.NotFound),
        });
        var catalog = new DocsCatalog(new HttpClient(handler), "https://mirror.internal/docs");

        var index = await catalog.IndexAsync(CancellationToken.None);
        Assert.Equal(
            ["concepts/data-plane", "concepts/delivery-guarantees", "how-to/gcs"],
            index.Select(p => p.Slug).Order(StringComparer.Ordinal));

        // The full-text corpus keys off the same relative slugs, so bodies attach across the mirror too.
        var page = await catalog.GetAsync("concepts/data-plane", CancellationToken.None);
        Assert.NotNull(page);
        Assert.Contains("DuckDB is the hub", page.Body);
    }
}
