using System.Globalization;
using System.Runtime.CompilerServices;

namespace Pz.Connector.Http;

/// <summary>Page read-ahead (<c>pagination.concurrency</c> above 1, numbered pages only): up to N page
/// fetches in flight, consumed strictly in page order, with every crawl decision made by the consumer
/// exactly as the sequential crawl makes it.</summary>
internal sealed partial class HttpPartition
{
    /// <summary>One issued page: its request, the request after it, and either the fetch in flight or a
    /// guard error held until the consumer reaches this page.</summary>
    private sealed record ReadAheadSlot(HttpPageRequest Request, HttpPageRequest? Next, Task<FetchedPage>? Fetch,
        Exception? Held);

    /// <summary>The issuing loop keeps at most <c>PageConcurrency</c> fetches in flight and never issues
    /// past <c>max_pages</c>; the guards a sequential crawl runs before a fetch (already requested, the
    /// page ceiling) run here at issue time, and their errors wait in the slot. The consumer takes slots
    /// in order and applies the stop decision, truncation detection and the rate-limit hint only to the
    /// pages it consumes, so a page fetched past the real end never lands, never reports a hint, and
    /// never fails the read. A stop (or a failure, or the caller abandoning the enumeration) cancels the
    /// fetches still in flight and observes their outcome before returning.</summary>
    private async IAsyncEnumerable<HttpPage> ReadAheadPagesAsync(HttpPageRequest first, bool stopsOnEmptyPage,
        [EnumeratorCancellation] CancellationToken ct)
    {
        using var abandon = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var slots = new Queue<ReadAheadSlot>();
        HttpPageRequest? toIssue = first;
        var issued = 0;
        var page = 0;
        try
        {
            while (true)
            {
                // Before issuing: a cancelled read starts no fetch it would only have to abandon.
                ct.ThrowIfCancellationRequested();
                while (toIssue is not null && slots.Count < config.PageConcurrency
                       && !(config.MaxPages is { } issueCap && issued >= issueCap))
                {
                    var slot = Issue(toIssue, issued, abandon.Token);
                    slots.Enqueue(slot);
                    issued++;
                    toIssue = slot.Held is null ? slot.Next : null;
                }

                // The slot stays queued until its fetch has been awaited, so a failure or cancellation
                // while awaiting it still leaves it to the drain below.
                if (!slots.TryPeek(out var current))
                {
                    yield break;
                }

                if (current.Held is not null)
                {
                    throw current.Held;
                }

                var fetched = await current.Fetch!.ConfigureAwait(false);
                slots.Dequeue();
                page++;
                if (gate is not null && fetched.Budget is { } budget)
                {
                    gate.ReportBudget(budget.Remaining, budget.ResetAt);
                }

                // The sequential crawl's stop decision and truncation detection, unchanged.
                var (request, records, next) = (current.Request, fetched.Records, current.Next);
                // A skipped first request ends the crawl whatever the strategy says about empty pages.
                var stopsAfterThisPage = Missing
                    || (records.Count == 0 && (stopsOnEmptyPage || next is null))
                    || (config.MaxPages is { } cap && page >= cap)
                    || (config.StopOnShortPage && config.PageSize is { } size && records.Count < size);

                if (stopsAfterThisPage && records.Count > 0 && config.MaxPages is { } hitCap && page >= hitCap
                    && next is not null && !next.SameAs(request))
                {
                    _truncatedWithMore = true;
                }

                yield return new HttpPage(request, records,
                    !stopsAfterThisPage && next is not null && !next.SameAs(request) ? next : null);

                if (stopsAfterThisPage || next is null || Missing)
                {
                    yield break;
                }
            }
        }
        finally
        {
            await abandon.CancelAsync().ConfigureAwait(false);
            while (slots.TryDequeue(out var abandoned))
            {
                if (abandoned.Fetch is { } fetch)
                {
                    try
                    {
                        await fetch.ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // A page past the crawl's end, behind its failure or cancelled with it (or the
                        // failed page itself, already surfaced): its outcome is observed so it never
                        // surfaces as an unobserved task exception, and discarded.
                    }
                }
            }
        }
    }

    /// <summary>Runs the pre-fetch guards for the page at <paramref name="index"/> (0-based within this
    /// attempt), computes the request after it, and starts its gated fetch. A guard that fails is held in
    /// the slot instead of thrown, with no fetch issued.</summary>
    private ReadAheadSlot Issue(HttpPageRequest request, int index, CancellationToken ct)
    {
        try
        {
            if (index > 0)
            {
                EnsureAllowedTarget(request.Url, "the next-page link the endpoint returned");
            }
        }
        catch (Exception ex)
        {
            return new ReadAheadSlot(request, null, null, ex);
        }

        if (!_requested.Add(request.KeyDigest))
        {
            return new ReadAheadSlot(request, null, null, AlreadyRequested(request));
        }

        if (config.MaxPages is null && index >= UnboundedPageCeiling)
        {
            return new ReadAheadSlot(request, null, null, PageCeilingReached());
        }

        HttpPageRequest? next;
        try
        {
            next = NextNumberedPage(request, index);
        }
        catch (Exception ex)
        {
            return new ReadAheadSlot(request, null, null, ex);
        }

        var firstOfCrawl = index == 0 && _resume is null;
        return new ReadAheadSlot(request, next, FetchAheadAsync(request, firstOfCrawl, ct), null);
    }

    // An async wrapper, so a gate that throws synchronously still faults the slot's task rather than the
    // issuing loop.
    private async Task<FetchedPage> FetchAheadAsync(HttpPageRequest request, bool firstOfCrawl, CancellationToken ct)
        => await _sender.RunAsync("http.get_page", idempotent: true,
            innerCt => FetchPageCoreAsync(null, request, firstOfCrawl, readAhead: true, innerCt), ct)
            .ConfigureAwait(false);

    /// <summary>The request for the page after <paramref name="request"/>'s, computed from the request
    /// alone. It must equal what the page strategy would produce (so checkpoint tokens match a
    /// sequential read), but the stateful strategy object is never used here: the page number is read
    /// back from the current request, falling back to <c>start</c> plus the pages advanced, as
    /// the Toolkit's <c>PageParamsStrategy</c> does.</summary>
    private HttpPageRequest? NextNumberedPage(HttpPageRequest request, int index)
    {
        if (config.BodyPaging is { } bodyPaging)
        {
            var paging = (BodyPagePaging)bodyPaging;
            return request.JsonBody is { } body ? request with { JsonBody = paging.Advance(body) } : null;
        }

        var numbering = config.PageNumbering
            ?? throw new InvalidOperationException("page read-ahead needs strategy 'page'");
        var nextPage = TryGetPageNumber(request.Url, numbering.Param, out var currentPage)
            ? currentPage + 1
            : numbering.Start + index + 1;
        var next = WithQueryParam(request.Url, numbering.Param, nextPage.ToString(CultureInfo.InvariantCulture));
        if (numbering.SizeParam is not null && numbering.Size is { } size)
        {
            next = WithQueryParam(next, numbering.SizeParam, size.ToString(CultureInfo.InvariantCulture));
        }

        return request with { Url = next };
    }

    /// <summary>Sets or replaces one query parameter exactly as the Toolkit page strategy does (the
    /// parameter moves to the end, name and value RFC 3986-escaped): the URL is a checkpoint token, so
    /// the read-ahead request must be the same string a sequential crawl sends.</summary>
    private static Uri WithQueryParam(Uri uri, string name, string value)
    {
        var encodedName = Uri.EscapeDataString(name);
        var pairs = uri.Query.TrimStart('?')
            .Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Where(p => !p.StartsWith(encodedName + "=", StringComparison.Ordinal) && p != encodedName)
            .Append($"{encodedName}={Uri.EscapeDataString(value)}");
        return new UriBuilder(uri) { Query = string.Join('&', pairs) }.Uri;
    }

    private static bool TryGetPageNumber(Uri uri, string param, out int page)
    {
        var prefix = Uri.EscapeDataString(param) + "=";
        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            if (pair.StartsWith(prefix, StringComparison.Ordinal))
            {
                return int.TryParse(Uri.UnescapeDataString(pair[prefix.Length..]),
                    NumberStyles.Integer, CultureInfo.InvariantCulture, out page);
            }
        }

        page = 0;
        return false;
    }
}
