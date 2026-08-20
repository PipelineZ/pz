using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

/// <summary>Continuation-link checkpoints. <see cref="HttpPartition"/> offers
/// the raw next-page link as an opaque resume token once every row of the page(s) it covers has
/// been YIELDED to the engine (not merely appended to the buffering <c>ArrowBatchBuilder</c>).
/// These tests drive <see cref="HttpPartition"/> directly via its public surface (OpenAsync -&gt;
/// PlanReadAsync -&gt; cast to <see cref="ICheckpointingPartition"/>), with
/// <see cref="BatchOptions.MaxRowsPerBatch"/> = 1 where the per-row granularity matters, so the
/// exact yield-by-yield checkpoint visibility is observable and pinned with concrete expectations.</summary>
public sealed class CheckpointTests
{
    private static Dictionary<string, object?> ContractOptions(string path) => new()
    {
        ["path"] = path,
        ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
    };

    private static async Task<(ISource Source, ICheckpointingPartition Partition)> OpenAsync(
        StubHttpServer server, DatasetSpec spec)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        var connector = new HttpConnector();
        var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        return (source, Assert.IsAssignableFrom<ICheckpointingPartition>(partitions[0]));
    }

    /// <summary>Maps a 3-page, 2-rows-per-page feed at <paramref name="path"/> (no leading '/' needed
    /// in the returned link strings): page 1 -&gt; ids 1,2 + Link to page 2; page 2 -&gt; ids 3,4 +
    /// Link to page 3; page 3 (terminal) -&gt; ids 5,6, no Link header.</summary>
    private static (string Page2Link, string Page3Link) MapThreePages(StubHttpServer server, string path)
    {
        var trimmed = path.TrimStart('/');
        var page2Link = $"{server.BaseUrl}{trimmed}?page=2";
        var page3Link = $"{server.BaseUrl}{trimmed}?page=3";
        server.Map(path, req =>
        {
            var q = req.Url.Query;
            if (q.Contains("page=3"))
            {
                return new StubResponse(200, """[{"id":5},{"id":6}]"""); // terminal: no Link header
            }

            if (q.Contains("page=2"))
            {
                return new StubResponse(200, """[{"id":3},{"id":4}]""",
                    new Dictionary<string, string> { ["Link"] = $"<{page3Link}>; rel=\"next\"" });
            }

            return new StubResponse(200, """[{"id":1},{"id":2}]""",
                new Dictionary<string, string> { ["Link"] = $"<{page2Link}>; rel=\"next\"" });
        });

        return (page2Link, page3Link);
    }

    private static long IdAt(RecordBatch batch, int row) => ((Int64Array)batch.Column(0)).GetValue(row)!.Value;

    [Fact]
    public async Task Checkpoint_only_advances_once_every_row_of_the_covering_page_is_yielded()
    {
        await using var server = new StubHttpServer();
        var (page2Link, page3Link) = MapThreePages(server, "/paged1");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged1", ContractOptions("/paged1")));
        await using var _ = source;

        var seen = new List<(long Id, bool Offered, string? Token)>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
        {
            long id;
            using (batch)
            {
                id = IdAt(batch, 0);
            }

            var offered = partition.TryGetCheckpoint(out var token);
            seen.Add((id, offered, token));
        }

        // Row 1 is page 1's first row: nothing to resume from yet. Row 2 completes page 1 (both
        // its rows now yielded) -- the page1->page2 boundary is confirmed the instant this
        // happens, since HttpPartition enqueues that boundary the moment page 1 is FETCHED
        // (records.Count known up front), not once its rows finish being appended. Row 3 is page
        // 2's first row: the page2->page3 boundary isn't confirmed yet (page 2 isn't fully
        // yielded), and the just-offered page2Link is not re-offered (dedup) -- false. Row 4
        // completes page 2, confirming the page2->page3 boundary -- page3Link. Rows 5-6 are page
        // 3 (terminal, no further link): nothing new to offer, so both are false.
        Assert.Equal(
            new (long Id, bool Offered, string? Token)[]
            {
                (1, false, null),
                (2, true, page2Link),
                (3, false, null),
                (4, true, page3Link),
                (5, false, null),
                (6, false, null),
            },
            seen);
    }

    [Fact]
    public async Task Default_batch_options_offer_a_checkpoint_at_every_page_boundary()
    {
        // Under default 32MB batches, rows from many pages fold into one batch, the exact-match
        // drain discards every overshot boundary, and NO checkpoint is ever offered — checkpoints
        // would exist only at contrived batch sizes. The connector must flush its builder at each
        // enqueued page boundary so rows-yielded lands exactly on the boundary and the token fires,
        // once per page.
        await using var server = new StubHttpServer();
        var (page2Link, page3Link) = MapThreePages(server, "/paged7");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged7", ContractOptions("/paged7")));
        await using var _ = source;

        var seen = new List<(long RowsSoFar, bool Offered, string? Token)>();
        long rowsSoFar = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                rowsSoFar += batch.Length;
            }

            var offered = partition.TryGetCheckpoint(out var token);
            seen.Add((rowsSoFar, offered, token));
        }

        // One batch per page: pages 1 and 2 each close with their continuation link offered; the
        // terminal page 3 has no further link, so its final flush offers nothing.
        Assert.Equal(
            new (long RowsSoFar, bool Offered, string? Token)[]
            {
                (2, true, page2Link),
                (4, true, page3Link),
                (6, false, null),
            },
            seen);
    }

    [Fact]
    public async Task Batches_never_span_a_page_boundary_so_every_token_aligns()
    {
        // The per-page builder flush closes each page's rows into their own batch, so even a
        // MaxRowsPerBatch larger than a page (3 over 2-row pages) yields page-shaped batches whose
        // tokens all align. DrainBoundaries' exact-match discard rule stays as defense-in-depth, but
        // overshoot is structurally impossible for this connector: a batch cannot contain rows from
        // two pages.
        await using var server = new StubHttpServer();
        var (page2Link, page3Link) = MapThreePages(server, "/paged6");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged6", ContractOptions("/paged6")));
        await using var _ = source;

        var observations = new List<(long RowsSoFar, bool Offered, string? Token)>();
        long rowsSoFar = 0;
        await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 3), CancellationToken.None))
        {
            using (batch)
            {
                rowsSoFar += batch.Length;
            }

            var offered = partition.TryGetCheckpoint(out var token);
            observations.Add((rowsSoFar, offered, token));
        }

        // The 3-row batch cap never fires (each page closes at 2 rows): pages 1 and 2 flush at
        // their boundaries with their continuation links; terminal page 3 (no Link header, no
        // boundary) flushes after the loop with nothing to offer.
        Assert.Equal(
            new (long RowsSoFar, bool Offered, string? Token)[]
            {
                (2, true, page2Link),
                (4, true, page3Link),
                (6, false, null),
            },
            observations);

        // Belt-and-braces: every offered token covers exactly the rows yielded so far — a resume
        // from it re-fetches no row already delivered.
        Assert.All(observations.Where(o => o.Offered), o =>
            Assert.True(o.Token == (o.RowsSoFar == 2 ? page2Link : page3Link)));
    }

    [Fact]
    public async Task Resume_from_a_checkpoint_fetches_only_the_tail()
    {
        await using var server = new StubHttpServer();
        var (page2Link, _) = MapThreePages(server, "/paged2");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged2", ContractOptions("/paged2")));
        await using var _ = source;

        Assert.True(partition.TryResumeFrom(page2Link));

        var ids = new List<long>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                for (var row = 0; row < batch.Length; row++)
                {
                    ids.Add(IdAt(batch, row));
                }
            }
        }

        Assert.Equal([3L, 4L, 5L, 6L], ids);
        Assert.Equal(2, server.Requests.Count);
        Assert.All(server.Requests, r => Assert.True(
            r.Url.Query.Contains("page=2") || r.Url.Query.Contains("page=3"),
            $"resume must never re-request page 1, saw {r.Url}"));
    }

    [Fact]
    public async Task Prefix_read_and_resumed_read_partition_the_full_set_with_no_overlap()
    {
        // Mirrors the TestKit's own Checkpoint_resume_yields_strictly_after_the_token invariant,
        // isolated to HTTP: this is exactly the property that catches an offered token whose
        // coverage is stale (already overshot by rows folded into the same batch that triggered
        // the offer).
        await using var server = new StubHttpServer();
        var (page2Link, _) = MapThreePages(server, "/paged5");

        List<long> prefix;
        string? token;
        {
            var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged5", ContractOptions("/paged5")));
            await using var _ = source;
            prefix = [];
            token = null;
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
            {
                using (batch)
                {
                    prefix.Add(IdAt(batch, 0));
                }

                if (partition.TryGetCheckpoint(out token) && token is not null)
                {
                    break;
                }
            }
        }

        Assert.Equal(page2Link, token);
        Assert.Equal([1L, 2L], prefix);

        List<long> resumed;
        {
            var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged5", ContractOptions("/paged5")));
            await using var _ = source;
            Assert.True(partition.TryResumeFrom(token!));
            resumed = [];
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
            {
                using (batch)
                {
                    resumed.Add(IdAt(batch, 0));
                }
            }
        }

        Assert.Empty(prefix.Intersect(resumed));
        Assert.Equal([1L, 2L, 3L, 4L, 5L, 6L], prefix.Concat(resumed).Order());
    }

    [Fact]
    public async Task Resume_from_a_page_params_checkpoint_continues_from_the_resumed_page()
    {
        // The 'page' strategy computes each next page from state, and a resumed attempt
        // constructs a FRESH strategy instance — the next request after the resumed page must
        // continue from that page's own number, never jump back to start + 1 and re-deliver
        // pages the engine already staged.
        await using var server = new StubHttpServer();
        server.Map("/pparams", req =>
        {
            var q = req.Url.Query;
            if (q.Contains("page=4"))
            {
                return new StubResponse(200, "[]"); // terminal: empty page stops the crawl
            }

            if (q.Contains("page=3"))
            {
                return new StubResponse(200, """[{"id":5},{"id":6}]""");
            }

            if (q.Contains("page=2"))
            {
                return new StubResponse(200, """[{"id":3},{"id":4}]""");
            }

            return new StubResponse(200, """[{"id":1},{"id":2}]""");
        });

        var options = new Dictionary<string, object?>
        {
            ["path"] = "/pparams",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        };

        // First crawl: stop at the checkpoint covering pages 1-2 (a page=3 link).
        string? token = null;
        {
            var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "pparams", options));
            await using var _ = source;
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
            {
                batch.Dispose();
                if (partition.TryGetCheckpoint(out var t) && t is not null && t.Contains("page=3"))
                {
                    token = t;
                    break;
                }
            }
        }

        Assert.NotNull(token);
        var requestsBeforeResume = server.Requests.Count;

        var (resumeSource, resumePartition) = await OpenAsync(server, new DatasetSpec("stub", "pparams", options));
        await using var __ = resumeSource;
        Assert.True(resumePartition.TryResumeFrom(token!));

        var resumed = new List<long>();
        await foreach (var batch in resumePartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                for (var row = 0; row < batch.Length; row++)
                {
                    resumed.Add(IdAt(batch, row));
                }
            }
        }

        Assert.Equal([5L, 6L], resumed);
        Assert.All(server.Requests.Skip(requestsBeforeResume), r => Assert.True(
            r.Url.Query.Contains("page=3") || r.Url.Query.Contains("page=4"),
            $"resume must never re-request an earlier page, saw {r.Url}"));
    }

    [Fact]
    public async Task Garbage_token_is_refused_never_thrown()
    {
        await using var server = new StubHttpServer();
        MapThreePages(server, "/paged3");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged3", ContractOptions("/paged3")));
        await using var _ = source;

        Assert.False(partition.TryResumeFrom("::not-a-url::"));
    }

    [Fact]
    public async Task Same_token_is_not_re_offered_until_a_later_page_completes()
    {
        await using var server = new StubHttpServer();
        var (page2Link, _) = MapThreePages(server, "/paged4");

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "paged4", ContractOptions("/paged4")));
        await using var _ = source;

        string? firstToken = null;
        await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
        {
            batch.Dispose();
            if (firstToken is null && partition.TryGetCheckpoint(out var token))
            {
                firstToken = token;
                break; // stop enumerating right after the first offer
            }
        }

        Assert.Equal(page2Link, firstToken);

        // No further batch has been read since: the candidate is unchanged, so re-asking must not
        // re-offer the very same token.
        Assert.False(partition.TryGetCheckpoint(out var repeat));
        Assert.Null(repeat);
    }

    [Fact]
    public async Task Sync_mode_mid_crawl_checkpoint_and_terminal_delta_candidate_survive_resume()
    {
        await using var server = new StubHttpServer();
        var page2Link = $"{server.BaseUrl}syncpaged?page=2";
        var page3Link = $"{server.BaseUrl}syncpaged?page=3";
        var deltaLink = $"{server.BaseUrl}syncpaged?$deltatoken=abc";

        server.Map("/syncpaged", req =>
        {
            var q = req.Url.Query;
            if (q.Contains("page=3"))
            {
                return new StubResponse(200,
                    $$"""{"value":[{"id":5},{"id":6}],"@odata.deltaLink":"{{deltaLink}}"}""");
            }

            if (q.Contains("page=2"))
            {
                return new StubResponse(200, """{"value":[{"id":3},{"id":4}]}""",
                    new Dictionary<string, string> { ["Link"] = $"<{page3Link}>; rel=\"next\"" });
            }

            return new StubResponse(200, """{"value":[{"id":1},{"id":2}]}""",
                new Dictionary<string, string> { ["Link"] = $"<{page2Link}>; rel=\"next\"" });
        });

        var options = new Dictionary<string, object?>
        {
            ["path"] = "/syncpaged",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
            ["items"] = "/value",
            ["delta_pointer"] = "/@odata.deltaLink",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        };

        var (source, partition) = await OpenAsync(server, new DatasetSpec("stub", "syncpaged", options));
        await using var _ = source;

        string? midCrawlToken = null;
        await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
        {
            batch.Dispose();
            if (midCrawlToken is null && partition.TryGetCheckpoint(out var token) && token is not null)
            {
                midCrawlToken = token;
            }
        }

        Assert.Equal(page2Link, midCrawlToken);

        var syncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(partition);
        Assert.True(syncPartition.TryGetSyncStateCandidate(out var candidate));
        Assert.Equal(deltaLink, candidate);

        // Resume from the mid-crawl checkpoint on a FRESH partition, then read to the end: the
        // terminal page's delta candidate must still be captured after a resume, not only on a
        // from-scratch read.
        var (resumeSource, resumePartition) = await OpenAsync(server, new DatasetSpec("stub", "syncpaged", options));
        await using var __ = resumeSource;
        Assert.True(resumePartition.TryResumeFrom(midCrawlToken!));

        await foreach (var batch in resumePartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }

        var resumeSyncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(resumePartition);
        Assert.True(resumeSyncPartition.TryGetSyncStateCandidate(out var resumedCandidate));
        Assert.Equal(deltaLink, resumedCandidate);
    }
}
