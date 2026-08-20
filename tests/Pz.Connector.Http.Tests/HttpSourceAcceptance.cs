using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpSourceAcceptance : SourceConnectorAcceptanceTests, IAsyncLifetime
{
    // A FIXED-length (not id-dependent) padding value for /checkpoint's "note" column, so
    // every one of its 500 rows costs EXACTLY the same estimated bytes in ArrowBatchBuilder --
    // see the /checkpoint route below for why exact, uniform per-row width matters here.
    private static readonly string CheckpointNotePadding = new('p', 70);

    private StubHttpServer _server = null!;

    public Task InitializeAsync()
    {
        _server = new StubHttpServer();
        // 3 pages x 40 rows, then an empty page: >= 100 rows, >= 2 batches under the 4KB target.
        _server.Map("/small", req =>
        {
            var page = req.Url.Query.Contains("page=") ? int.Parse(req.Url.Query.Split("page=")[1].Split('&')[0]) : 1;
            if (page > 3)
            {
                return new StubResponse(200, "[]");
            }

            var rows = Enumerable.Range((page - 1) * 40, 40)
                .Select(i => $$"""{"id":{{i}},"note":"row-{{i}}-padding-padding"}""");
            return new StubResponse(200, "[" + string.Join(',', rows) + "]");
        });
        _server.Map("/flaky", _ => new StubResponse(503, """{"err":"down"}"""));
        // 10 pages x 50 rows (500 rows), chained via Link headers. link_header (not
        // "page") is required here: it derives `next` from the CURRENT response's own Link
        // header, so it stays correct when a resumed read starts mid-crawl -- unlike
        // PageParamsStrategy, which is a stateless counter from a fixed `start` and would
        // recompute the same page it just resumed onto (self-collision -> "pagination is not
        // advancing"). The 70-char, id-INDEPENDENT note padding is chosen so every row costs
        // EXACTLY the same estimated bytes (id: 8+0.125; note: 4+70+0.125 = 82.25/row total) --
        // 49 rows = 4030.25 bytes (< the 4KB SmallBatchTargetBytes), 50 rows = 4112.5 (over), so
        // ArrowBatchBuilder's byte-threshold batch boundary lands on EXACTLY the page boundary,
        // every page. A continuation-link checkpoint can only ever be offered on an EXACT
        // rows-yielded/page-boundary match (HttpPartition.DrainBoundaries) -- with a mismatched
        // batch/page granularity (e.g. row content whose byte size doesn't divide evenly into the
        // target), no batch would ever land exactly on a page boundary and the checkpoint would
        // never fire, silently Skip-ing the acceptance fact instead of exercising it.
        _server.Map("/checkpoint", req =>
        {
            var page = req.Url.Query.Contains("page=") ? int.Parse(req.Url.Query.Split("page=")[1].Split('&')[0]) : 1;
            var rows = Enumerable.Range((page - 1) * 50, 50)
                .Select(i => $$"""{"id":{{i}},"note":"{{CheckpointNotePadding}}"}""");
            var body = "[" + string.Join(',', rows) + "]";
            if (page >= 10)
            {
                return new StubResponse(200, body); // terminal: no Link header
            }

            var nextLink = $"{_server.BaseUrl}checkpoint?page={page + 1}";
            return new StubResponse(200, body,
                new Dictionary<string, string> { ["Link"] = $"<{nextLink}>; rel=\"next\"" });
        });
        _server.Map("/windowed", req =>
        {
            var q = req.Url.Query;
            if (q.Contains("page=") && int.Parse(q.Split("page=")[1].Split('&')[0]) > 1)
            {
                return new StubResponse(200, "[]");
            }

            long after = q.Contains("after=") ? long.Parse(q.Split("after=")[1].Split('&')[0]) : -1;
            long before = q.Contains("before=") ? long.Parse(q.Split("before=")[1].Split('&')[0]) : 10;
            var rows = Enumerable.Range(0, 11).Where(i => i > after && i <= before)
                .Select(i => $$"""{"id":{{i}}}""");
            return new StubResponse(200, "[" + string.Join(',', rows) + "]");
        });
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _server.DisposeAsync();

    protected override ISourceConnector CreateSource() => new HttpConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["base_url"] = _server.BaseUrl.ToString(),
    });

    protected override DatasetSpec SmallDataset => new("stub", "small", new Dictionary<string, object?>
    {
        ["path"] = "/small",
        ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
    });

    protected override DatasetSpec? TransientFailureDataset => new("stub", "flaky",
        new Dictionary<string, object?> { ["path"] = "/flaky" });

    protected override DatasetSpec? BoundedWindowDataset => new("stub", "windowed",
        new Dictionary<string, object?>
        {
            ["path"] = "/windowed",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
            ["query"] = new Dictionary<string, object?>
            {
                ["after"] = "{{ watermark }}",
                ["before"] = "{{ window_upper }}",
            },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "3",
            WatermarkUpperBound = "7",
        };

    // CheckpointKeyColumn stays the default (0): "id" is the leading contract column and is
    // unique across all 500 rows of /checkpoint.
    protected override DatasetSpec? CheckpointDataset => new("stub", "checkpoint", new Dictionary<string, object?>
    {
        ["path"] = "/checkpoint",
        ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["note"] = "varchar" },
    });
}
