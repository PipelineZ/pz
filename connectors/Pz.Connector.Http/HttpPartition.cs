using System.Globalization;
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
internal sealed class HttpPartition(HttpClient client, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate = null)
    : IDatasetPartition, ISyncStatePartition, ICheckpointingPartition
{
    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

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
    private readonly Queue<(long RowsYielded, string NextLink)> _pageBoundaries = new();
    // Cumulative rows appended into the builder so far, across all pages -- bulk-updated by
    // records.Count the instant each page is fetched (property 1 above), not per row.
    private long _rowsAppended;
    private long _rowsYielded;
    private string? _checkpointCandidate;
    private string? _lastReturnedCheckpoint;
    private string? _resumeLink;

    // Every URI already requested during THIS attempt. The "next == current" check only sees a page
    // that points straight back at itself; a feed that alternates A -> B -> A -> B (or
    // any longer ring) advances on every single step and would be crawled forever. Membership here
    // is the general form of "pagination is not advancing".
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

    private const int MaxRedirects = 5;

    // Per-attempt truncation-guard state. Ordinal of the incremental cursor within ProjectRow's
    // output (raw envelope: fixed slot 3; contract mode:
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

        return config.Cursor is null ? -1 : 3;
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
        // A checkpoint is a continuation link the ENDPOINT produced on an earlier attempt and pz
        // merely stored, so it is no more trustworthy on the way back in than it was on the way out:
        // it must still name a host this connection is allowed to talk to.
        if (!Uri.TryCreate(checkpoint, UriKind.Absolute, out var target)
            || !connection.IsAllowedTarget(target))
        {
            return false;
        }

        _resumeLink = checkpoint;
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
                _checkpointCandidate = boundary.NextLink;
            }
            // else: rows-yielded-so-far already overshot this boundary (a batch folded in some of
            // the next page's rows too) -- discard silently; offering it now would duplicate rows
            // the engine already has.
        }
    }

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var columns = config.IsContractMode ? config.Columns! : RawEnvelope.Columns(config);
        var builder = new ArrowBatchBuilder(ContractProjector.BuildSchema(columns),
            options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        var strategy = config.PageStrategyFactory?.Invoke();
        // A resume link wins over sync-token replay -- it is a LATER position inside the same
        // crawl, whereas PriorSyncState (inside BuildFirstUri) replays a
        // whole-feed delta token from a PRIOR run. _syncCandidate capture from the terminal page
        // is unaffected either way.
        Uri? uri = _resumeLink is { } resume ? new Uri(resume) : BuildFirstUri(strategy);
        EnsureAllowedTarget(uri, "the first request");
        var page = 0;
        // An empty page ends the crawl only for a strategy with no other end (page numbers). For
        // link-header/cursor feeds an empty page in the middle is a gap: Graph delta feeds and
        // filtered queries both serve one, and stopping there drops every remaining row with no
        // error and, on an incremental read, advances the watermark past rows never fetched.
        var stopsOnEmptyPage = strategy?.StopsOnEmptyPage ?? true;

        while (uri is not null)
        {
            ct.ThrowIfCancellationRequested();
            if (!_requested.Add(uri.ToString()))
            {
                throw new PzConnectorException(
                    $"{Label}: pagination is not advancing — '{Redact(uri)}' was already requested " +
                    "during this crawl; check the pagination options", isTransient: false);
            }

            if (config.MaxPages is null && page >= UnboundedPageCeiling)
            {
                throw new PzConnectorException(
                    $"{Label}: pagination did not terminate within {UnboundedPageCeiling} pages — the " +
                    "feed keeps offering another page; set 'max_pages' if this feed really is this " +
                    "large, or fix the pagination options", isTransient: false);
            }

            var (records, next) = await FetchPageAsync(strategy, uri, ct).ConfigureAwait(false);
            // pz_fetched_at is the request-time UTC when the PAGE was fetched — one
            // timestamp shared by every row landed from this page, not stamped per row.
            var fetchedAt = time.GetUtcNow();
            var currentPage = page;
            page++;
            // The page counter restarts at 0 on every ATTEMPT (see the resume-link comment above
            // seeding `uri`), so max_pages bounds each attempt's fetch count, not the partition's
            // lifetime total across retries.
            var stopsAfterThisPage = (records.Count == 0 && (stopsOnEmptyPage || next is null))
                || (config.MaxPages is { } cap && page >= cap);

            var boundaryEnqueued = false;
            if (!stopsAfterThisPage && next is not null && next != uri)
            {
                // Enqueue NOW, using records.Count (known immediately, before any of this page's
                // rows are appended) rather than waiting for this page's own foreach to exit --
                // see property 1 in the _pageBoundaries comment above. Skipped when the loop is
                // about to stop or throw: there is nothing beyond what's already appended for this
                // attempt, so no later position exists to gate a checkpoint behind.
                _pageBoundaries.Enqueue((_rowsAppended + records.Count, next.ToString()));
                boundaryEnqueued = true;
            }

            if (stopsAfterThisPage && records.Count > 0 && config.MaxPages is { } hitCap && page >= hitCap
                && next is not null && next != uri)
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

            if (next is not null && next == uri)
            {
                throw new PzConnectorException(
                    $"{Label}: pagination is not advancing (next request equals '{Redact(uri)}'); " +
                    "check the pagination options", isTransient: false);
            }

            if (next is not null)
            {
                EnsureAllowedTarget(next, "the next-page link the endpoint returned");
            }

            uri = next;
        }

        // A truncated incremental crawl may only advance the watermark if the landed rows provably
        // form a contiguous prefix -- i.e. cursor values were
        // monotone non-decreasing with at least one strictly-increasing pair. Any decrease, or no
        // increase at all (all-equal values, single-row crawls), means advancement would skip rows
        // and NOT advancing would re-fetch the same head forever: fail loudly instead.
        if (_truncatedWithMore && _cursorOrdinal >= 0)
        {
            // A resumed attempt (TryResumeFrom set _resumeLink) never saw the prior attempt's
            // prefix -- _sawIncrease/_sawDecrease only reflect THIS
            // attempt's own pages, so an ascending-looking tail proves nothing about the full crawl's
            // ordering across attempts. Truncating a resumed attempt is unprovable regardless of what
            // this attempt's own cursor values did; fail the same way natural monotonicity failures
            // do, every time.
            if (_resumeLink is not null)
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

    private Uri BuildFirstUri(IPageStrategy? strategy)
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

        if (spec.WatermarkCursor is { } wmCursor)
        {
            if (config.Cursor is null && !config.IsContractMode)
            {
                throw new PzConnectorException(
                    $"{Label}: incremental dataset needs the raw-mode 'cursor' + 'cursor_type' options " +
                    "(or a 'columns' contract declaring the cursor)", isTransient: false);
            }

            if (config.Cursor is { } optCursor && optCursor != wmCursor)
            {
                throw new PzConnectorException(
                    $"{Label}: incremental.cursor is '{wmCursor}' but options.cursor is '{optCursor}'; " +
                    "align the two", isTransient: false);
            }
        }

        string? cursorType;
        if (config.IsContractMode && spec.WatermarkCursor is not null)
        {
            if (!config.Columns!.TryGetValue(spec.WatermarkCursor, out cursorType))
            {
                throw new PzConnectorException(
                    $"{Label}: incremental cursor '{spec.WatermarkCursor}' is not declared in 'columns:' — " +
                    "add it to the contract so its values land and its type is known", isTransient: false);
            }
        }
        else
        {
            cursorType = config.CursorType;
        }
        var bindings = new Dictionary<string, BindingValue>
        {
            ["watermark"] = new(spec.WatermarkValue, cursorType),
            // Upper bound of the engine-resolved (lower, upper] window; null when the dataset is
            // not windowed, in which case TryExpand omits the param.
            ["window_upper"] = new(spec.WatermarkUpperBound, cursorType),
        };

        var pairs = new List<string>();
        foreach (var (name, template) in config.Query)
        {
            bool expanded;
            string? value, error;
            try
            {
                // Defense in depth: HttpDatasetConfig.Parse already validates every query template
                // offline, but a malformed/unknown binding must never surface as an uncoded
                // FormatException from a live read.
                expanded = BindingExpander.TryExpand(template, bindings, FormatBinding, out value, out error);
            }
            catch (FormatException ex)
            {
                throw new PzConnectorException(
                    $"{Label}: query parameter '{name}': malformed binding template '{template}': " +
                    $"{ex.Message}", isTransient: false);
            }

            if (!expanded)
            {
                throw new PzConnectorException($"{Label}: query parameter '{name}': {error}",
                    isTransient: false);
            }

            if (value is null)
            {
                continue; // referenced binding is null (first run / --full-refresh): omit the param
            }

            pairs.Add($"{Uri.EscapeDataString(name)}={Uri.EscapeDataString(value)}");
        }

        // config.Path always starts with '/' (HttpDatasetConfig.Parse validated); stripping it makes
        // the segment relative so it resolves against the FULL slash-terminated base_url (including
        // any path prefix) instead of root-relative to the host.
        var first = new UriBuilder(new Uri(connection.BaseUrl, config.Path[1..]))
        {
            Query = string.Join('&', pairs),
        }.Uri;

        // The strategy's own params (page number, size) go on the first request too — an API's
        // default page size is otherwise in effect for page one, skipping or re-delivering rows
        // the moment page two jumps ahead. Sync-token replays returned above stay verbatim.
        return strategy is null ? first : strategy.FirstRequestUri(first);
    }

    // Formats a bound value for a query param. Serves BOTH the lower bound (watermark) and the
    // upper bound (window_upper). A timestamp cursor renders as ISO-8601 UTC to whole seconds
    // (many APIs reject sub-second precision). Rounding direction differs by bound so the
    // (lower, upper] window is never under-covered:
    //   - lower (watermark, exclusive '>'): truncate DOWN — re-reads at most the boundary second.
    //   - upper (window_upper, inclusive '<='): round UP — a sub-second upper like ...00.5 must
    //     still include rows at ...00.5, so we query '<= ...01'. Rows in (upper, ceil] are
    //     over-extracted this window, but the engine's window-scoped MAX probe caps the watermark
    //     candidate at 'upper', so they re-deliver in the next window: re-delivery, never loss,
    //     even for an empty (lower, floor_sec(upper)] slice (windowed->append requires
    //     accept_duplicates, PZ0214). Truncating the upper DOWN would instead drop the fractional
    //     tail: an empty truncated slice advances the watermark to the full untruncated upper
    //     (empty-slice advancement), silently skipping those rows.
    private static string FormatBinding(string name, BindingValue value)
    {
        if (value.TypeName != "timestamp"
            || !DateTimeOffset.TryParse(value.Value, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var ts))
        {
            return value.Value!;
        }

        var floored = new DateTimeOffset(ts.Year, ts.Month, ts.Day, ts.Hour, ts.Minute, ts.Second, TimeSpan.Zero);
        var rendered = name == "window_upper" && ts > floored ? floored.AddSeconds(1) : floored;
        return rendered.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
    }

    private object?[] ProjectRow(JsonNode? record, int page, DateTimeOffset fetchedAt)
    {
        if (config.IsContractMode)
        {
            return ContractProjector.ProjectRow(record, config.Columns!, Label);
        }

        var row = new object?[config.Cursor is null ? 3 : 4];
        row[0] = record?.ToJsonString() ?? "null";
        row[1] = page;
        row[2] = fetchedAt;
        if (config.Cursor is { } cursor)
        {
            JsonPointer.TryResolve(record, config.CursorPointer, out var node);
            row[3] = ContractProjector.ConvertScalar(node, config.CursorType!, cursor, Label);
        }

        return row;
    }

    /// <summary>Thin gate-routing wrapper: with no gate configured, calls straight through. With a
    /// gate, the ENTIRE
    /// request/classify/decode body runs as the gate's op callback (classification into
    /// PzConnectorException happens INSIDE <see cref="FetchPageCoreAsync"/>, so the gate always sees
    /// fully-classified transient/permanent exceptions). Every page fetch is idempotent: a GET replay
    /// (including sync-mode delta-link replay) never advances the provider cursor — pz only advances
    /// its own watermark/sync-state after a post-commit persist.</summary>
    private Task<(IReadOnlyList<JsonNode?> Records, Uri? Next)> FetchPageAsync(
        IPageStrategy? strategy, Uri uri, CancellationToken ct)
        => gate is null
            ? FetchPageCoreAsync(strategy, uri, ct)
            : gate.ExecuteAsync("http.get_page", idempotent: true,
                innerCt => FetchPageCoreAsync(strategy, uri, innerCt), ct);

    private async Task<(IReadOnlyList<JsonNode?> Records, Uri? Next)> FetchPageCoreAsync(
        IPageStrategy? strategy, Uri uri, CancellationToken ct)
    {
        var (response, sendUri) = await SendFollowingRedirectsAsync(uri, ct).ConfigureAwait(false);

        using (response)
        {
            var status = (int)response.StatusCode;
            var text = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
            {
                if (TransientClassifier.IsTransientStatus(status))
                {
                    var retryAfter = TransientClassifier.ParseRetryAfter(
                        response.Headers.TryGetValues("Retry-After", out var values)
                            ? values.FirstOrDefault()
                            : null,
                        time.GetUtcNow());
                    throw new PzConnectorException($"{Label}: HTTP {status} from {Redact(sendUri)}",
                        isTransient: true, retryAfter);
                }

                if (config.IsSyncMode && status == 410 && spec.PriorSyncState is { Length: > 0 })
                {
                    // Only an actual replay (a stored prior token) makes a 410 mean "the token
                    // expired" — a first-run sync read has no token to expire, so a 410 there falls
                    // through to the normal permanent-error path below (bad path/endpoint).
                    // The delta link/sync token itself embeds server-side state (and, for
                    // token-in-query auth, a secret) — always surface the redacted URI, never
                    // the raw stored token.
                    throw new PzConnectorException(
                        $"{Label}: the sync token/delta link has expired (HTTP 410) at '{Redact(sendUri)}'; " +
                        "re-run with --full-refresh to restart the feed from the beginning", isTransient: false);
                }

                throw new PzConnectorException(
                    $"{Label}: HTTP {status} from {Redact(sendUri)}: {RedactSnippet(text)} " +
                    "(check the endpoint path and auth config)", isTransient: false);
            }

            JsonNode? body;
            try
            {
                body = JsonNode.Parse(text);
            }
            catch (JsonException ex)
            {
                throw new PzConnectorException(
                    $"{Label}: response from {Redact(sendUri)} is not valid JSON", isTransient: false,
                    innerException: ex);
            }

            if (!JsonPointer.TryResolve(body, config.ItemsPointer, out var itemsNode))
            {
                throw new PzConnectorException(
                    $"{Label}: items pointer '{config.ItemsPointer}' not found in response from " +
                    $"{Redact(sendUri)}", isTransient: false);
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

            return (records, strategy?.NextRequestUri(uri, response, body));
        }
    }

    /// <summary>Issues the GET and walks any redirect chain by hand, because
    /// <see cref="HttpSource.CreateClient"/> turns the handler's own redirect following off. Each hop's
    /// target is resolved against the URL actually requested and then checked with
    /// <see cref="EnsureAllowedTarget"/>, so a redirect can never carry this connection's credentials
    /// to a host the endpoint picked; the chain is bounded so a redirect loop fails instead of
    /// spinning. Returns the first non-redirect response together with the URI that produced it —
    /// the post-auth URI, which is what every error message redacts and reports.</summary>
    private async Task<(HttpResponseMessage Response, Uri SendUri)> SendFollowingRedirectsAsync(
        Uri uri, CancellationToken ct)
    {
        var target = uri;
        for (var hop = 0; ; hop++)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, target);
            connection.Authenticator?.Apply(request);
            // Redact the POST-auth URI: an api_key-in-query authenticator just added the secret to
            // request.RequestUri, and that is the URL any error message must never echo unredacted.
            var sendUri = request.RequestUri ?? target;

            HttpResponseMessage response;
            try
            {
                response = await client.SendAsync(request, ct).ConfigureAwait(false);
            }
            catch (HttpRequestException ex)
            {
                throw new PzConnectorException($"{Label}: request to {Redact(sendUri)} failed: {ex.Message}",
                    TransientClassifier.IsTransientException(ex), innerException: ex);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
            {
                throw new PzConnectorException($"{Label}: request to {Redact(sendUri)} timed out",
                    isTransient: true, innerException: ex);
            }

            var redirectStatus = (int)response.StatusCode;
            if (!IsRedirect(redirectStatus))
            {
                return (response, sendUri);
            }

            var location = response.Headers.Location;
            response.Dispose();

            if (location is null)
            {
                throw new PzConnectorException(
                    $"{Label}: HTTP {redirectStatus} from {Redact(sendUri)} has no Location " +
                    "header (check the endpoint path)", isTransient: false);
            }

            if (hop >= MaxRedirects)
            {
                throw new PzConnectorException(
                    $"{Label}: more than {MaxRedirects} redirects starting at {Redact(uri)}; " +
                    "point 'base_url'/'path' at the endpoint's final location", isTransient: false);
            }

            target = new Uri(sendUri, location);
            EnsureAllowedTarget(target, "a redirect from the endpoint");
        }
    }

    private static bool IsRedirect(int status) => status is 301 or 302 or 303 or 307 or 308;

    /// <summary>Refuses any request target the far side chose that is not this connection's own origin
    /// (or an explicitly allow-listed host). Pagination links, redirect targets and stored resume
    /// tokens are all endpoint-controlled, and every request carries the connection's
    /// Authorization/api-key headers — so without this a single hostile response turns pz into a
    /// credential-exfiltration channel. Only the authority is named: the rest of the URL can embed the
    /// secret itself.</summary>
    private void EnsureAllowedTarget(Uri candidate, string what)
    {
        if (connection.IsAllowedTarget(candidate))
        {
            return;
        }

        throw new PzConnectorException(
            $"{Label}: {what} points at '{candidate.GetLeftPart(UriPartial.Authority)}', which is not " +
            $"this connection's host '{connection.BaseUrl.GetLeftPart(UriPartial.Authority)}' — pz will " +
            "not send this connection's credentials to a host the endpoint chose; if this really is " +
            "part of the same API, add the host to 'allow_hosts' on the connection", isTransient: false);
    }

    private static string Snippet(string body)
    {
        var flat = body.ReplaceLineEndings(" ");
        return flat.Length <= 160 ? flat : flat[..160] + "…";
    }

    /// <summary>Redacts a permanent-error response body before it is surfaced in an exception message.
    /// A 4xx body can echo the request URL verbatim (e.g. a Graph-style 403/404 error payload naming the
    /// request), leaking the server-defined sync token/delta link param through the snippet even though
    /// it isn't one of the connector's configured <c>SecretQueryParams</c>. In sync mode, strip the query
    /// string off any URL found in the text wholesale first (same whole-query approach as
    /// <see cref="RedactQuery"/> -- the param name is opaque/server-defined, not maskable by name), then
    /// apply the param-based <see cref="Redact(string)"/> as a second pass for any configured auth secret
    /// the body might also echo.</summary>
    private string RedactSnippet(string text) =>
        Redact(Snippet(config.IsSyncMode ? StripUrlQueries(text) : text));

    private static readonly System.Text.RegularExpressions.Regex UrlWithQuery =
        new(@"https?://[^\s""'?]+\?[^\s""']*", System.Text.RegularExpressions.RegexOptions.Compiled);

    private static string StripUrlQueries(string text) =>
        UrlWithQuery.Replace(text, m =>
        {
            var uri = new Uri(m.Value);
            return RedactQuery(uri);
        });

    /// <summary>Redacts a URI before it is surfaced in an exception message. For a sync-mode
    /// dataset the query IS the sync token — its param name is server-defined/opaque (e.g. Graph's
    /// `$deltatoken`), not necessarily the connector's configured auth param — so the whole query is
    /// replaced wholesale rather than masked param-by-param; scheme/host/path are kept for
    /// diagnosability. Non-sync reads use SecretQueryParams-based masking.</summary>
    private string Redact(Uri uri) => config.IsSyncMode ? RedactQuery(uri) : Redact(uri.ToString());

    private static string RedactQuery(Uri uri)
    {
        var basePart = uri.GetLeftPart(UriPartial.Path);
        return uri.Query.Length > 0 ? $"{basePart}?<redacted>" : basePart;
    }

    /// <summary>Masks any authenticator secret-query-param value found in arbitrary text — the
    /// same rule applied to request URIs, reused here for 4xx response body snippets (a server
    /// echoing the request URL, e.g. in a 403 body, must not leak an api_key-in-query secret).</summary>
    private string Redact(string text)
    {
        foreach (var param in connection.Authenticator?.SecretQueryParams ?? [])
        {
            text = System.Text.RegularExpressions.Regex.Replace(
                text, $"(?<=[?&]){System.Text.RegularExpressions.Regex.Escape(param)}=[^&]*",
                $"{param}=***");
        }

        return text;
    }
}
