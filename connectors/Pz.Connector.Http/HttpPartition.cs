using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Bindings;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Http;
using Pz.Connectors.Toolkit.Json;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

/// <summary>One dataset's universal-tier read: request → classify → decode → project → yield,
/// following pagination until exhausted. Never retries, never sleeps — classification is this
/// class's whole resilience duty (the engine owns policy). Batches yielded become
/// engine-owned; nothing here retains them.</summary>
internal sealed class HttpPartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate = null)
    : IDatasetPartition, ISyncStatePartition, ICheckpointingPartition
{
    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    /// <summary>The for_each item this read fetches for: its fields expand into the request and land on
    /// every row. Null for a dataset without for_each.</summary>
    public ItemValues? Item { get; init; }

    private ItemCells? _itemCells;

    // In sync mode the query IS the sync token, whose param name is server-defined, so messages drop
    // the whole query rather than masking the authenticator's params by name.
    private readonly HttpRequestSender _sender = new(client, bareClient, connection, config.Body, gate, time,
        $"http source '{spec.Source}.{spec.Dataset}'", redactWholeQuery: config.IsSyncMode);

    // Set inside FetchPageCoreAsync when config.IsSyncMode: the delta pointer is resolved against
    // every page's body, and since only the terminal page of a Graph-style feed carries a
    // deltaLink (earlier pages carry nextLink), assigning whenever it resolves naturally leaves
    // the LAST page's value here — the candidate the engine polls post-enumeration over the
    // connector state channel (ISyncStatePartition).
    private string? _syncCandidate;

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = _syncCandidate;
        return candidate is not null;
    }

    // The continuation link is the checkpoint token. The engine persists a
    // token only after the rows it covers are durably staged, and ArrowBatchBuilder buffers rows
    // across pages — so the candidate advances ONLY when every row of a page has been YIELDED
    // (not merely appended to the builder). _pageBoundaries records "once N rows have been
    // yielded, the next request is <link>"; DrainBoundaries promotes entries as yields catch up.
    //
    // Two properties make this safe rather than merely convenient:
    //  1. A boundary is enqueued the instant a page is FETCHED (using records.Count, known before
    //     any of its rows are appended), not once its rows finish being appended/yielded. A
    //     continuation link is inherently PAGE-granular — resuming from it re-fetches the WHOLE
    //     next page, never a row offset within one. Batches are byte-sized while pages are
    //     row-counted, so the two rarely align; if the boundary were enqueued only after this
    //     page's own foreach exits, a batch whose byte threshold fires PART-WAY into the page
    //     that follows (extremely common) would already have folded some of ITS rows into the very
    //     batch that drains this boundary, by which point the queue entry would not even exist yet
    //     to drain. Enqueuing the predicted total up front closes that gap.
    //  2. DrainBoundaries only promotes a boundary on an EXACT match (rows yielded so far equals
    //     the boundary's threshold exactly), never merely "at or past" it. A single batch can span
    //     several whole pages (small pages, large batches); if yielded-so-far has already
    //     overshot a boundary, that boundary's link would re-fetch rows already folded into the
    //     batch that overshot it — so it is discarded, not promoted, and only a LATER boundary that
    //     lands exactly (if any) becomes the candidate.
    //
    // Tokens are engine-opaque and never logged (they can embed server-side state and, for
    // token-in-query auth, secrets — same rule as the sync delta link).
    private readonly Queue<(long RowsYielded, HttpPageRequest Next)> _pageBoundaries = new();
    // Cumulative rows appended into the builder so far, across all pages -- bulk-updated by
    // records.Count the instant each page is fetched (property 1 above), not per row.
    private long _rowsAppended;
    private long _rowsYielded;
    private string? _checkpointCandidate;
    private string? _lastReturnedCheckpoint;
    private HttpPageRequest? _resume;

    // SHA-256 digest (HttpPageRequest.KeyDigest) of every request (method, URL and body) already sent during THIS
    // attempt. The "next == current" check only sees a page that points straight back at itself; a feed that
    // alternates A -> B -> A -> B (or any longer ring) advances on every single step and would be crawled forever.
    // Membership here is the general form of "pagination is not advancing".
    private readonly HashSet<string> _requested = new(StringComparer.Ordinal);

    /// <summary>Bound on an attempt's page count when the dataset declares no <c>max_pages</c>. A feed
    /// that keeps serving fresh, non-empty pages with fresh continuation tokens is indistinguishable
    /// from a legitimately enormous one, so the only defence against a never-ending crawl (an
    /// ever-growing staging table and a run that never finishes) is a ceiling. Set high enough that no
    /// real feed reaches it; hitting it is an error naming <c>max_pages</c>, never a silent
    /// truncation — an implicit ceiling means something is wrong, whereas an explicit
    /// <c>max_pages</c> means the author asked for a slice.
    ///
    /// Settable only as a test seam — production never assigns it, and reaching 50 000 pages for real
    /// takes long enough that a test proving the ceiling would otherwise have to spend a minute
    /// getting there.</summary>
    internal static int UnboundedPageCeiling = 50_000;

    // Per-attempt truncation-guard state. Ordinal of the incremental cursor within ProjectRow's
    // output (raw envelope: fixed slot 3, or 4 after a for_each dataset's pz_item; contract mode:
    // the cursor column's position in the Columns key order), or -1 when the read is not
    // incremental / has no cursor -- guard disabled.
    private bool _truncatedWithMore;
    private bool _sawIncrease;
    private bool _sawDecrease;
    private object? _prevCursor;
    private readonly int _cursorOrdinal = ComputeCursorOrdinal(config, spec);

    private static int ComputeCursorOrdinal(HttpDatasetConfig config, DatasetSpec spec)
    {
        if (spec.WatermarkCursor is null)
        {
            return -1;
        }

        if (config.IsContractMode)
        {
            var i = 0;
            foreach (var name in config.Columns!.Keys)
            {
                if (name == spec.WatermarkCursor)
                {
                    return i;
                }

                i++;
            }

            return -1;
        }

        return config.Cursor is null ? -1 : config.IsForEach ? 4 : 3;
    }

    private void TrackCursorOrder(object? value)
    {
        if (value is null)
        {
            return; // a null cursor value orders nothing; the row still lands
        }

        if (_prevCursor is not null)
        {
            var cmp = Comparer<object>.Default.Compare(value, _prevCursor);
            if (cmp > 0)
            {
                _sawIncrease = true;
            }
            else if (cmp < 0)
            {
                _sawDecrease = true;
            }
        }

        _prevCursor = value;
    }

    public string PartitionId => $"{spec.Source}.{spec.Dataset}";

    public bool TryResumeFrom(string checkpoint)
    {
        // A checkpoint is a continuation the ENDPOINT produced on an earlier attempt and pz merely
        // stored, so it is no more trustworthy on the way back in than it was on the way out: it must
        // still name a host this connection is allowed to talk to, and have this dataset's shape.
        if (!HttpCheckpointToken.TryDecode(checkpoint, config.IsPost, config.Method ?? HttpMethod.Get,
                HttpCheckpointToken.KindOf(config.Body), out var resume)
            || !connection.IsAllowedTarget(resume!.Url))
        {
            return false;
        }

        _resume = resume;
        return true;
    }

    public bool TryGetCheckpoint(out string? checkpoint)
    {
        checkpoint = _checkpointCandidate;
        if (checkpoint is null || checkpoint == _lastReturnedCheckpoint)
        {
            checkpoint = null;
            return false;
        }

        _lastReturnedCheckpoint = checkpoint;
        return true;
    }

    private void DrainBoundaries()
    {
        while (_pageBoundaries.TryPeek(out var boundary) && boundary.RowsYielded <= _rowsYielded)
        {
            _pageBoundaries.Dequeue();
            if (boundary.RowsYielded == _rowsYielded)
            {
                _checkpointCandidate = CheckpointToken(boundary.Next);
            }
            // else: rows-yielded-so-far already overshot this boundary (a batch folded in some of
            // the next page's rows too) -- discard silently; offering it now would duplicate rows
            // the engine already has.
        }
    }

    private string CheckpointToken(HttpPageRequest next) => HttpCheckpointToken.Encode(next, config.IsPost);

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = config.IsContractMode ? config.Columns! : RawEnvelope.Columns(config);
        var builder = new ArrowBatchBuilder(ContractProjector.BuildSchema(columns),
            options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        var strategy = config.PageStrategyFactory?.Invoke();
        _itemCells = ItemCells.For(config, Item, Label);
        // Read once: every request this partition builds renders the same {{ run_date }}, even
        // across midnight.
        var runDate = config.RunDate.Render(time.GetUtcNow());
        // A resume link wins over sync-token replay -- it is a LATER position inside the same
        // crawl, whereas PriorSyncState (inside BuildFirstRequest) replays a
        // whole-feed delta token from a PRIOR run. _syncCandidate capture from the terminal page
        // is unaffected either way.
        HttpPageRequest? request = _resume ?? BuildFirstRequest(strategy, runDate);
        EnsureAllowedTarget(request.Url, "the first request");
        var page = 0;
        // An empty page ends the crawl only for a strategy with no other end (page numbers). For
        // link-header/cursor feeds an empty page in the middle is a gap: Graph delta feeds and
        // filtered queries both serve one, and stopping there drops every remaining row with no
        // error and, on an incremental read, advances the watermark past rows never fetched. Body
        // paging replaces the URL strategy, so its own rule applies (a body cursor feed is a feed
        // with gaps too).
        var stopsOnEmptyPage = config.BodyPaging?.StopsOnEmptyPage ?? strategy?.StopsOnEmptyPage ?? true;

        while (request is not null)
        {
            ct.ThrowIfCancellationRequested();
            if (!_requested.Add(request.KeyDigest))
            {
                throw new PzConnectorException(
                    $"{Label}: pagination is not advancing — '{Redact(request.Url)}' was already requested" +
                    $"{SameBody(request)} during this crawl; check the pagination options", isTransient: false);
            }

            if (config.MaxPages is null && page >= UnboundedPageCeiling)
            {
                throw new PzConnectorException(
                    $"{Label}: pagination did not terminate within {UnboundedPageCeiling} pages — the " +
                    "feed keeps offering another page; set 'max_pages' if this feed really is this " +
                    "large, or fix the pagination options", isTransient: false);
            }

            var (records, next) = await FetchPageAsync(strategy, request, ct).ConfigureAwait(false);
            // pz_fetched_at is the request-time UTC when the PAGE was fetched — one
            // timestamp shared by every row landed from this page, not stamped per row.
            var fetchedAt = time.GetUtcNow();
            var currentPage = page;
            page++;
            // The page counter restarts at 0 on every ATTEMPT (see the resume-link comment above
            // seeding `uri`), so max_pages bounds each attempt's fetch count, not the partition's
            // lifetime total across retries.
            var stopsAfterThisPage = (records.Count == 0 && (stopsOnEmptyPage || next is null))
                || (config.MaxPages is { } cap && page >= cap)
                // Opt-in: some APIs clamp an out-of-range page number to the last real page instead
                // of serving an empty array, so the feed never actually ends on its own -- a page
                // whose row count falls short of the requested size is the only signal left.
                || (config.StopOnShortPage && config.PageSize is { } size && records.Count < size);

            var boundaryEnqueued = false;
            if (!stopsAfterThisPage && next is not null && !next.SameAs(request))
            {
                // Enqueue NOW, using records.Count (known immediately, before any of this page's
                // rows are appended) rather than waiting for this page's own foreach to exit --
                // see property 1 in the _pageBoundaries comment above. Skipped when the loop is
                // about to stop or throw: there is nothing beyond what's already appended for this
                // attempt, so no later position exists to gate a checkpoint behind.
                _pageBoundaries.Enqueue((_rowsAppended + records.Count, next));
                boundaryEnqueued = true;
            }

            if (stopsAfterThisPage && records.Count > 0 && config.MaxPages is { } hitCap && page >= hitCap
                && next is not null && !next.SameAs(request))
            {
                // Stopped by the cap with a real further page pending -- not a natural completion.
                _truncatedWithMore = true;
            }

            foreach (var record in records)
            {
                var row = ProjectRow(record, currentPage, fetchedAt);
                if (_cursorOrdinal >= 0)
                {
                    TrackCursorOrder(row[_cursorOrdinal]);
                }

                builder.AppendRow(row);
                if (builder.TryTakeBatch(out var batch))
                {
                    // Capture the length BEFORE yielding: the engine takes ownership of (and may
                    // dispose) the batch the instant control returns to it, so reading
                    // batch.Length after the yield would race a disposed buffer. Update + drain
                    // BEFORE yielding (not after): the engine may call TryGetCheckpoint the moment
                    // it receives this batch, and the iterator is merely suspended at the yield —
                    // its fields (read by TryGetCheckpoint from outside) must already reflect this
                    // batch's contribution by then, not only once the next MoveNextAsync resumes it.
                    _rowsYielded += batch!.Length;
                    DrainBoundaries();
                    yield return batch!;
                    ct.ThrowIfCancellationRequested();
                }
            }

            _rowsAppended += records.Count;

            // Checkpoint cadence is once per page: flush the builder at every enqueued page boundary
            // so rows-yielded lands EXACTLY on the boundary's threshold and DrainBoundaries
            // promotes the token. Without
            // this, the default 32MB/122880-row batch spans many pages, every boundary is overshot
            // and discarded, and checkpoints rarely fire in practice. Batches therefore never span
            // page boundaries, so the drain's overshoot-discard branch is defense-in-depth only.
            // Skipped when no boundary was enqueued (terminal/capped page, no pagination): there is
            // no token to align, so the tail keeps accumulating toward the post-loop flush.
            if (boundaryEnqueued && builder.Flush() is { } pageTail)
            {
                _rowsYielded += pageTail.Length;
                DrainBoundaries();
                yield return pageTail;
                ct.ThrowIfCancellationRequested();
            }

            if (stopsAfterThisPage)
            {
                break;
            }

            if (next is not null && next.SameAs(request))
            {
                throw new PzConnectorException(
                    $"{Label}: pagination is not advancing (next request equals '{Redact(request.Url)}'" +
                    $"{SameBody(request)}); check the pagination options", isTransient: false);
            }

            if (next is not null)
            {
                EnsureAllowedTarget(next.Url, "the next-page link the endpoint returned");
            }

            request = next;
        }

        // A truncated incremental crawl may only advance the watermark if the landed rows provably
        // form a contiguous prefix -- i.e. cursor values were
        // monotone non-decreasing with at least one strictly-increasing pair. Any decrease, or no
        // increase at all (all-equal values, single-row crawls), means advancement would skip rows
        // and NOT advancing would re-fetch the same head forever: fail loudly instead.
        if (_truncatedWithMore && _cursorOrdinal >= 0)
        {
            // A resumed attempt (TryResumeFrom set _resume) never saw the prior attempt's
            // prefix -- _sawIncrease/_sawDecrease only reflect THIS
            // attempt's own pages, so an ascending-looking tail proves nothing about the full crawl's
            // ordering across attempts. Truncating a resumed attempt is unprovable regardless of what
            // this attempt's own cursor values did; fail the same way natural monotonicity failures
            // do, every time.
            if (_resume is not null)
            {
                throw new PzConnectorException(
                    $"{Label}: max_pages truncated a resumed crawl — a resumed attempt cannot prove the " +
                    "full crawl's ordering; the watermark cannot advance without risking skipped rows; " +
                    "remove max_pages (slices run to completion) or point at an ascending endpoint",
                    isTransient: false);
            }

            if (_sawDecrease || !_sawIncrease)
            {
                var declared = config.CursorOrder == "asc"
                    ? " ('cursor_order: asc' is declared but the feed did not deliver ascending values)"
                    : "";
                throw new PzConnectorException(
                    $"{Label}: max_pages truncated a crawl whose cursor values are not ascending{declared} — " +
                    "the watermark cannot advance without skipping rows; remove max_pages (slices run to " +
                    "completion) or point at an ascending endpoint", isTransient: false);
            }
        }

        if (builder.Flush() is { } final)
        {
            _rowsYielded += final.Length;
            DrainBoundaries();
            yield return final;
        }
    }

    // Names the body without showing it: a repeated POST is identified by "same body", never by content.
    private static string SameBody(HttpPageRequest request)
        => request.JsonBody is not null || request.TextBody is not null ? " with the same body" : "";

    private HttpPageRequest BuildFirstRequest(IPageStrategy? strategy, string runDate)
    {
        // Delta-link replay is a GET by construction ('delta_pointer' is refused with POST).
        if (TryGetSyncReplay() is { } replay)
        {
            return new HttpPageRequest(HttpMethod.Get, replay, null, null);
        }

        var bindings = HttpFirstRequest.Bindings(spec, config, runDate, Label);
        return HttpFirstRequest.Build(connection, config, strategy, bindings, Label, Item);
    }

    private Uri? TryGetSyncReplay()
    {
        // Delta-link replay: a sync-mode dataset with a stored prior token skips path/query
        // construction entirely and re-issues the opaque URL verbatim (it already embeds
        // whatever server-side state the feed needs — query params, tokens, everything).
        if (config.IsSyncMode && spec.PriorSyncState is { Length: > 0 } priorToken)
        {
            // The stored token is a URL the ENDPOINT minted. A corrupt or hostile one must surface as
            // a coded connector error, not as a raw UriFormatException from deep in the read; the
            // caller then checks it against the connection's allowed hosts like any other link.
            if (!Uri.TryCreate(priorToken, UriKind.Absolute, out var replay))
            {
                throw new PzConnectorException(
                    $"{Label}: the stored sync token/delta link is not an absolute URL; re-run with " +
                    "--full-refresh to restart the feed from the beginning", isTransient: false);
            }

            return replay;
        }

        return null;
    }

    internal static string FormatBinding(string name, BindingValue value)
        => HttpFirstRequest.FormatBinding(name, value);

    private object?[] ProjectRow(JsonNode? record, int page, DateTimeOffset fetchedAt)
    {
        if (config.IsContractMode)
        {
            return _itemCells is { } cells
                ? cells.Place(ContractProjector.ProjectRow(record, cells.RecordColumns, Label), 0)
                : ContractProjector.ProjectRow(record, config.Columns!, Label);
        }

        var cursorSlot = config.IsForEach ? 4 : 3;
        var row = new object?[config.Cursor is null ? cursorSlot : cursorSlot + 1];
        row[0] = record?.ToJsonString() ?? "null";
        row[1] = page;
        row[2] = fetchedAt;
        if (config.IsForEach)
        {
            row[3] = _itemCells?.Json;
        }

        if (_itemCells is { CursorFromItem: true })
        {
            row[cursorSlot] = _itemCells.Cursor;
        }
        else if (config.Cursor is { } cursor)
        {
            JsonPointer.TryResolve(record, config.CursorPointer, out var node);
            row[cursorSlot] = ContractProjector.ConvertScalar(node, config.CursorType!, cursor, Label);
        }

        return row;
    }

    /// <summary>Thin gate-routing wrapper: with no gate configured, calls straight through. With a
    /// gate, the ENTIRE
    /// request/classify/decode body runs as the gate's op callback (classification into
    /// PzConnectorException happens INSIDE <see cref="FetchPageCoreAsync"/>, so the gate always sees
    /// fully-classified transient/permanent exceptions). Every page fetch is idempotent: a GET replay
    /// (including sync-mode delta-link replay) never advances the provider cursor — pz only advances
    /// its own watermark/sync-state after a post-commit persist. A POST on `read:` is a read the
    /// author asserts has no side effects, so it is replayed on the same terms.</summary>
    private Task<(IReadOnlyList<JsonNode?> Records, HttpPageRequest? Next)> FetchPageAsync(
        IPageStrategy? strategy, HttpPageRequest request, CancellationToken ct)
        => _sender.RunAsync("http.get_page", idempotent: true,
            innerCt => FetchPageCoreAsync(strategy, request, innerCt), ct);

    private async Task<(IReadOnlyList<JsonNode?> Records, HttpPageRequest? Next)> FetchPageCoreAsync(
        IPageStrategy? strategy, HttpPageRequest request, CancellationToken ct)
    {
        var result = await _sender.SendAsync(request, streamBody: false, credentialsOnlyToOrigin: false, ct)
            .ConfigureAwait(false);
        var (response, sendUri, sent) = result;

        using (result)
        {
            var status = (int)response.StatusCode;
            var text = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                if (config.IsSyncMode && status == 410 && spec.PriorSyncState is { Length: > 0 })
                {
                    // Only an actual replay (a stored prior token) makes a 410 mean "the token
                    // expired" — a first-run sync read has no token to expire, so a 410 there falls
                    // through to the generic classification below (bad path/endpoint).
                    // The delta link/sync token itself embeds server-side state (and, for
                    // token-in-query auth, a secret) — always surface the redacted URI, never
                    // the raw stored token.
                    throw new PzConnectorException(
                        $"{Label}: the sync token/delta link has expired (HTTP 410) at '{Target(sent, sendUri)}'; " +
                        "re-run with --full-refresh to restart the feed from the beginning", isTransient: false);
                }

                throw _sender.StatusError(result, text);
            }

            JsonNode? body;
            try
            {
                body = JsonNode.Parse(text);
            }
            catch (JsonException ex)
            {
                throw new PzConnectorException(
                    $"{Label}: response from {Target(sent, sendUri)} is not valid JSON", isTransient: false,
                    innerException: ex);
            }

            if (config.ErrorPointer is { } errorPointer)
            {
                ThrowIfErrorReported(body, errorPointer, Target(sent, sendUri), Label, config.IsSyncMode, Redact);
            }

            if (!JsonPointer.TryResolve(body, config.ItemsPointer, out var itemsNode))
            {
                throw new PzConnectorException(
                    $"{Label}: items pointer '{config.ItemsPointer}' not found in response from " +
                    $"{Target(sent, sendUri)}", isTransient: false);
            }

            // Sync mode: resolve delta_pointer against THIS page's body. A Graph-style feed's
            // non-terminal pages carry nextLink (delta_pointer doesn't resolve there), so only the
            // terminal page assigns — the loop calling this once per page in order leaves the LAST
            // page's value as the run's candidate.
            if (config.IsSyncMode
                && JsonPointer.TryResolve(body, config.DeltaLinkPointer!, out var deltaNode)
                && deltaNode is JsonValue deltaValue
                && deltaValue.TryGetValue(out string? deltaLink)
                && !string.IsNullOrEmpty(deltaLink))
            {
                _syncCandidate = deltaLink;
            }

            IReadOnlyList<JsonNode?> records = itemsNode switch
            {
                JsonArray array => [.. array],
                JsonObject single => [single],
                null => [],
                _ => throw new PzConnectorException(
                    $"{Label}: items pointer '{config.ItemsPointer}' resolves to neither an array " +
                    "nor an object", isTransient: false),
            };

            // Proactive throttle hint: parsed from the SUCCESS response's own
            // rate-limit headers, reported to the gate so the NEXT paced operation on this instance
            // waits if the provider says the budget is exhausted. No-op without a gate.
            if (gate is not null &&
                RateLimitHeaders.TryParse(response, time.GetUtcNow(), out var remaining, out var resetAt))
            {
                gate.ReportBudget(remaining, resetAt);
            }

            return (records, NextRequest(strategy, request, response, body, records.Count));
        }
    }

    // Body paging moves the body and keeps the URL; a URL strategy moves the URL and re-sends the
    // same body; neither means the crawl ends here.
    private HttpPageRequest? NextRequest(IPageStrategy? strategy, HttpPageRequest request,
        HttpResponseMessage response, JsonNode? body, int recordsReturned)
    {
        if (config.BodyPaging is { } paging)
        {
            return request.JsonBody is { } current && paging.Next(current, body, recordsReturned) is { } nextBody
                ? request with { JsonBody = nextBody }
                : null;
        }

        return strategy?.NextRequestUri(request.Url, response, body) is { } nextUrl
            ? request with { Url = nextUrl }
            : null;
    }

    /// <summary>Fails the page when <paramref name="pointer"/> resolves to anything but <c>null</c>,
    /// <c>false</c>, an empty array or an empty object: a 200 that reports errors (GraphQL, including
    /// partial data alongside errors) would otherwise land wrong or incomplete rows silently. The
    /// message carries the first error's <c>message</c> field (or the value itself when it is a
    /// string), redacted like a response snippet and cut to 200 characters. <paramref name="stripUrlQueries"/>
    /// drops the whole query of any URL in that text first (for a read whose queries are secrets with
    /// server-defined names).</summary>
    internal static void ThrowIfErrorReported(JsonNode? body, string pointer, string target, string label,
        bool stripUrlQueries, Func<string, string> redact)
    {
        if (!JsonPointer.TryResolve(body, pointer, out var reported)
            || reported is null
            || reported is JsonArray { Count: 0 }
            || reported is JsonObject { Count: 0 }
            || (reported is JsonValue flag && flag.TryGetValue(out bool b) && !b))
        {
            return;
        }

        var first = reported is JsonArray array ? array[0] : reported;
        var detail = first switch
        {
            JsonObject obj when obj["message"] is JsonValue m && m.TryGetValue(out string? msg) => msg,
            JsonValue v when v.TryGetValue(out string? str) => str,
            _ => null,
        };

        var suffix = "";
        if (!string.IsNullOrEmpty(detail))
        {
            // Redact before cutting, so a cut can never leave half a secret that no longer matches.
            var flat = redact((stripUrlQueries ? HttpRequestSender.StripUrlQueries(detail) : detail).ReplaceLineEndings(" "));
            suffix = $": {(flat.Length <= 200 ? flat : flat[..200] + "…")}";
        }

        throw new PzConnectorException(
            $"{label}: the response from {target} reports an error at 'error_pointer' '{pointer}'{suffix}",
            isTransient: false);
    }

    private string Target(HttpMethod method, Uri uri) => _sender.Target(method, uri);

    private string Redact(Uri uri) => _sender.Redact(uri);

    private void EnsureAllowedTarget(Uri candidate, string what) => _sender.EnsureAllowedTarget(candidate, what);

    /// <summary>Masks any authenticator secret-query-param value found in arbitrary text.</summary>
    private string Redact(string text) => connection.RedactSecretParams(text);
}
