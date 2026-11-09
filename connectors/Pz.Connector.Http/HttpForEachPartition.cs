using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>A for_each dataset read: crawl the list request page by page and, for each list record in
/// order, read the item request built from that record's fields (its JSON pages to the last one, or in
/// file mode its one download) before the next record starts; with <c>for_each.concurrency</c> above 1,
/// up to that many of one list page's items at once, checkpointed only between list pages (see
/// <c>ReadParallelAsync</c>). The clock is read once, so the list and every item share one
/// {{ run_date }} and one <c>pz_fetched_at</c>. Checkpoints name the list page, the record index and
/// the item crawl's position (see <see cref="HttpCheckpointToken"/>). Never retries,
/// and never sleeps except to wait out the read's shared <c>Retry-After</c> pause when
/// <c>for_each.concurrency</c> is above 1.</summary>
internal sealed partial class HttpForEachPartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate,
    ThrottlePause? pause = null)
    : ICheckpointingPartition, ISkippedReadPartition
{
    private string? _skippedBy;

    /// <summary>The first item an `on_status` skip rule skipped in this read (an `on_missing` 404 does not count),
    /// set when that item finishes: before any checkpoint past it is offered, as
    /// <see cref="ISkippedReadPartition"/> requires.</summary>
    public string? SkippedBy => Volatile.Read(ref _skippedBy);

    /// <summary>Notes an item partition whose read an `on_status` skip rule ended.</summary>
    private void NoteRuleSkip(IDatasetPartition item, ItemValues values)
    {
        if (item is ISkippedReadPartition { SkippedBy: { } rule })
        {
            Interlocked.CompareExchange(ref _skippedBy, $"{rule} ({values.Identity()})", null);
        }
    }

    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    /// <summary>Where a non-fatal notice goes (a cap that stopped the read, items skipped under
    /// `on_missing: skip`); null when the engine subscribed none.</summary>
    public Action<string>? Notice { get; init; }

    /// <summary>The parent of each file-mode item's scratch directory; null means `Path.GetTempPath()`.
    /// Tests point it at a directory of their own.</summary>
    internal string? ScratchRoot { get; set; }

    /// <summary>Handed to each file-mode item (<see cref="HttpFilePartition.IdempotencyScope"/>).</summary>
    internal string? IdempotencyScope { get; init; }

    /// <summary>Where JSON items' batch buffers come from; null means the shared native pool. Tests count
    /// with it.</summary>
    internal MemoryAllocator? Allocator { get; set; }

    public string PartitionId => $"{spec.Source}.{spec.Dataset}";

    // The resume position (list page, record index, item crawl token, anchor and skips so far). Set by
    // TryResumeFrom, already validated.
    private HttpCheckpointToken.ForEachToken? _resume;

    // The checkpoint covering exactly the rows yielded so far, or null. Set before every yield, so it
    // never describes a position the engine's staged rows have not reached.
    private string? _checkpointCandidate;
    private string? _lastReturnedCheckpoint;

    public bool TryResumeFrom(string checkpoint)
    {
        // Every embedded token is a position an endpoint chose: it must still have its request's shape and
        // name a host this connection may reach. File items have no position inside them.
        if (!HttpCheckpointToken.TryDecodeForEach(checkpoint, out var position)
            || (config.ForEach!.List is { } listRequest
                ? !Fits(position.List, listRequest)
                : !config.ForEach.TryValuesOffset(position.List, out _))
            || (position.Item is not null && (config.IsFileMode || !Fits(position.Item, config))))
        {
            return false;
        }

        _resume = position;
        return true;
    }

    /// <summary>Whether <paramref name="token"/> is a resume token <see cref="HttpPartition"/> accepts for a
    /// request of <paramref name="request"/>'s method and body kind.</summary>
    private bool Fits(string token, HttpDatasetConfig request)
        => HttpCheckpointToken.TryDecode(token, request.IsPost, request.Method ?? HttpMethod.Get,
               HttpCheckpointToken.KindOf(request.Body), out var decoded, request.HeaderPaging?.Header)
           && connection.IsAllowedTarget(decoded!.Url);

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

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var forEach = config.ForEach!;
        var now = time.GetUtcNow();
        // The dataset's bindings serve both requests: the list formats the watermark by the dataset's
        // cursor type, and both render the same {{ run_date }}.
        var bindings = HttpFirstRequest.Bindings(spec, config, config.RunDate.Render(now), Label);
        // The list crawl and every item share this read's Retry-After pause: a throttle on any of them
        // holds back the others.
        var list = forEach.List is { } listRequest
            ? new HttpPartition(client, bareClient, connection, listRequest, spec, time, gate, pause)
            {
                Bindings = bindings,
                Notice = Notice,
            }
            : null;

        var resume = _resume;
        if (resume is not null && list is not null && !list.TryResumeFrom(resume.List))
        {
            throw Unusable();
        }

        // A values@N position counts from a list that may have been edited since: the anchor proves it is the
        // same list, and without that proof the read starts over (items read twice, none skipped).
        if (resume is not null && list is null && forEach.TryValuesOffset(resume.List, out var offset) && offset > 0
            && resume.Anchor != forEach.ValuesAnchor)
        {
            Notice?.Invoke($"{Label}: for_each.values changed since the checkpoint was taken; reading the values " +
                "again from the first value (items read before the retry are read again)");
            resume = resume with { List = HttpForEachConfig.ValuesToken, Index = 0, Item = null, Anchor = null };
        }

        if (forEach.Concurrency > 1)
        {
            await foreach (var batch in ReadParallelAsync(list, bindings, now, resume, options, ct).ConfigureAwait(false))
            {
                yield return batch;
            }

            yield break;
        }

        // Items are counted per attempt, like max_pages. Skips carry over from the checkpoint, so a retry
        // resumed past a skipped item still reports it.
        var items = 0;
        var skipped = resume?.Skipped ?? 0;
        var firstSkipped = resume?.FirstSkipped;
        var truncated = 0;
        string? firstTruncated = null;
        var duplicates = 0;
        string? firstDuplicate = null;
        var seen = forEach.Dedupe ? new SeenItems() : null;
        var capped = false;
        var firstPage = true;

        await foreach (var page in ListPagesAsync(list, resume, ct).ConfigureAwait(false))
        {
            var listToken = page.Token;
            var (start, resumeItem) = firstPage && resume is not null ? ResumeAt(resume, page) : (0, null);
            if (firstPage)
            {
                seen?.Seed(page, start, forEach.Fields);
            }

            firstPage = false;

            for (var index = start; index < page.Records.Count; index++)
            {
                var values = ItemValues.From(page.Records[index], forEach.Fields);
                if (seen is not null && !seen.Add(values))
                {
                    duplicates++;
                    firstDuplicate ??= values.Identity();
                    continue;
                }

                if (forEach.MaxItems is { } max && items >= max)
                {
                    Notice?.Invoke($"{Label}: stopped after max_items ({max}) items; more list records were available");
                    capped = true;
                    break;
                }

                items++;
                var anchor = HttpCheckpointToken.Anchor(values);
                var item = ItemRead(values, bindings, now, $"{page.Where}, record {index}", Notice);
                if (index == start && resumeItem is not null && !item.Partition.TryResumeFrom(resumeItem))
                {
                    throw Unusable();
                }

                // Before each batch is yielded, the candidate becomes the position just past it: the item
                // crawl's own checkpoint when it offers one for this batch (a page boundary), else the next
                // item when this batch is the item's last. A batch with neither clears it. Finding out
                // whether a batch is the last reads one batch ahead, so up to two batches are held at once
                // (the one waiting and the one read ahead); for a JSON item that stays within the page
                // already fetched (a page with a further page always offers a checkpoint). An item whose
                // last batch carried the crawl's checkpoint ends without an item-boundary one; a resume
                // there re-requests the item's trailing page.
                await using var batches = item.Partition.ReadAsync(options, ct).GetAsyncEnumerator(ct);
                // The batch read but not yet handed to the engine: still this read's to release, whether the
                // lookahead throws or the engine stops the read at a yield while a batch is read ahead.
                RecordBatch? pending = null;
                try
                {
                    if (await NextAsync(batches, values).ConfigureAwait(false))
                    {
                        pending = batches.Current;
                    }

                    while (pending is { } batch)
                    {
                        if (item.Partition.TryGetCheckpoint(out var inner) && inner is not null)
                        {
                            _checkpointCandidate = HttpCheckpointToken.EncodeForEach(new(listToken, index, inner, anchor,
                                skipped, firstSkipped));
                            pending = null;
                            yield return batch;
                            if (await NextAsync(batches, values).ConfigureAwait(false))
                            {
                                pending = batches.Current;
                            }

                            continue;
                        }

                        var more = await NextAsync(batches, values).ConfigureAwait(false);
                        pending = more ? batches.Current : null;
                        // index + 1 may equal the page's record count; the token keeps this list page, and a
                        // resume from it moves on to the next one.
                        _checkpointCandidate = more
                            ? null
                            : HttpCheckpointToken.EncodeForEach(new(listToken, index + 1, null, anchor, skipped, firstSkipped));
                        yield return batch;
                    }
                }
                finally
                {
                    pending?.Dispose();
                }

                NoteRuleSkip(item.Partition, values);
                if (item.Missing())
                {
                    skipped++;
                    firstSkipped ??= values.Identity();
                }

                if (item.Truncated())
                {
                    truncated++;
                    firstTruncated ??= values.Identity();
                }
            }

            // The cap reached on a page's last record: stop here rather than fetch a list page whose
            // records would not be read.
            if (!capped && forEach.MaxItems is { } cap && items >= cap && page.NextToken is not null)
            {
                Notice?.Invoke($"{Label}: stopped after max_items ({cap}) items; more list records may be available");
                capped = true;
            }

            if (capped)
            {
                break;
            }
        }

        EndNotices(capped, list, truncated, firstTruncated, skipped, firstSkipped, duplicates, firstDuplicate);
    }

    /// <summary>One page of items: a list page's records, or the whole of `for_each.values`. <see cref="Token"/>
    /// is the list position a checkpoint stores for it, <see cref="Where"/> names it in messages, and
    /// <see cref="NextToken"/> is the next list page's position (null on the last page).</summary>
    private sealed record ListPage(IReadOnlyList<JsonNode?> Records, string Token, string Where, string? NextToken);

    /// <summary>The list crawl's pages, or the literal values cut into pages of
    /// <see cref="HttpForEachConfig.ValuesPageSize"/>, from the resume position's page on.</summary>
    private async IAsyncEnumerable<ListPage> ListPagesAsync(HttpPartition? list,
        HttpCheckpointToken.ForEachToken? resume, [EnumeratorCancellation] CancellationToken ct)
    {
        var forEach = config.ForEach!;
        if (list is null)
        {
            var values = forEach.Values!;
            var offset = 0;
            if (resume is not null)
            {
                forEach.TryValuesOffset(resume.List, out offset);
            }

            // An offset at the end (a token naming the page after the last) has nothing left to read.
            var size = forEach.ValuesPageSize;
            while (offset < values.Count)
            {
                var take = Math.Min(size, values.Count - offset);
                var next = offset + take;
                yield return new ListPage([.. values.Skip(offset).Take(take)], HttpForEachConfig.ValuesTokenAt(offset),
                    offset == 0 ? "for_each.values" : $"for_each.values from {offset}",
                    next < values.Count ? HttpForEachConfig.ValuesTokenAt(next) : null);
                offset = next;
            }

            yield break;
        }

        var isPost = forEach.List!.IsPost;
        await foreach (var page in list.PagesAsync(ct).ConfigureAwait(false))
        {
            // A query token the sign-in adds is never stored; a resumed read adds the current one.
            var strip = connection.CheckpointStrippedParams;
            yield return new ListPage(page.Records, HttpCheckpointToken.Encode(page.Request, isPost, strip),
                $"list page '{list.Redact(page.Request.Url)}'",
                page.Next is { } next ? HttpCheckpointToken.Encode(next, isPost, strip) : null);
        }
    }

    /// <summary>The fields tuples (<see cref="ItemValues.ToJsonString"/>) already read in this attempt, kept as
    /// 128-bit prefixes of their SHA-256. A HashSet entry is about 24 bytes plus its bucket, and the set grows by
    /// doubling, so a list costs roughly 30 to 60 bytes per distinct item (about 0.5 GB for ten million). There
    /// is no cap: every distinct item is also a request, which bounds a real list long before memory does.</summary>
    private sealed class SeenItems
    {
        private readonly HashSet<UInt128> _seen = [];
        private readonly Lock _lock = new();

        /// <summary>True the first time a tuple is added.</summary>
        public bool Add(ItemValues values)
        {
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(values.ToJsonString()));
            var key = new UInt128(BinaryPrimitives.ReadUInt64LittleEndian(hash), BinaryPrimitives.ReadUInt64LittleEndian(hash.AsSpan(8)));
            lock (_lock)
            {
                return _seen.Add(key);
            }
        }

        /// <summary>A resumed attempt's first list page: the records before the resume position were read by
        /// an earlier attempt, so a later duplicate of one of them is still a duplicate.</summary>
        public void Seed(ListPage page, int start, IReadOnlyList<(string Name, string Pointer)> fields)
        {
            for (var i = 0; i < Math.Min(start, page.Records.Count); i++)
            {
                Add(ItemValues.From(page.Records[i], fields));
            }
        }
    }

    /// <summary>The notices a read that ran to its end gives: the list's own <c>max_pages</c> (unless
    /// <c>max_items</c> stopped it first), then items cut short by theirs, then skipped items, then duplicate
    /// records `dedupe` skipped.</summary>
    private void EndNotices(bool capped, HttpPartition? list, int truncated, string? firstTruncated, int skipped,
        string? firstSkipped, int duplicates, string? firstDuplicate)
    {
        if (!capped && list is { TruncatedWithMore: true })
        {
            Notice?.Invoke($"{Label}: stopped after for_each.max_pages ({config.ForEach!.List!.MaxPages}) list pages; " +
                "more list pages were available");
        }

        if (truncated > 0)
        {
            Notice?.Invoke($"{Label}: max_pages stopped {truncated} items before their last page (first: {firstTruncated})");
        }

        if (skipped > 0)
        {
            // Without an item skip rule the wording stays exactly what on_missing has always reported.
            var why = !config.HasSkipRule
                ? "that returned 404/410"
                : config.ForEach!.OnMissingSkip
                    ? "whose first request returned 404/410 or matched an on_status skip rule"
                    : "whose first request matched an on_status skip rule";
            Notice?.Invoke($"{Label}: skipped {skipped} items {why} (first: {firstSkipped})");
        }

        if (duplicates > 0)
        {
            Notice?.Invoke($"{Label}: for_each.dedupe skipped {duplicates} duplicate list records " +
                $"(first: {firstDuplicate})");
        }
    }

    /// <summary>Where a resumed attempt starts on its first list page: the stored index and item token
    /// when the anchor record is still where the checkpoint saw it (or the token has no anchor), else
    /// the page's first record with the item started over, since the list has shifted and the stored
    /// index may now name another record. Restarting re-reads items (duplicates a merge sink removes)
    /// rather than reading an item token as another record's or skipping a record. An index at or past
    /// the record count, its anchor matching, starts nothing here: the read continues with the next page.</summary>
    private (int Start, string? Item) ResumeAt(HttpCheckpointToken.ForEachToken resume, ListPage page)
    {
        // A values@N position starts its own page; its anchor names the whole list, already checked in ReadAsync.
        if (resume.Anchor is null
            || (config.ForEach!.List is null && resume.List != HttpForEachConfig.ValuesToken))
        {
            return (resume.Index, resume.Item);
        }

        var at = resume.Item is null ? resume.Index - 1 : resume.Index;
        return at >= 0 && at < page.Records.Count
               && HttpCheckpointToken.Anchor(ItemValues.From(page.Records[at], config.ForEach!.Fields)) == resume.Anchor
            ? (resume.Index, resume.Item)
            : (0, null);
    }

    /// <summary>One item's read: its JSON pages, or in file mode its download (with its own scratch
    /// directory and decompression budget, released when the read ends however it ends). Missing turns
    /// true once the item's first request was answered 404/410 under `on_missing: skip`; Truncated, once
    /// the item's `max_pages` stopped it with a further page pending (never for a file).</summary>
    private (ICheckpointingPartition Partition, Func<bool> Missing, Func<bool> Truncated) ItemRead(ItemValues values,
        IReadOnlyDictionary<string, BindingValue> bindings, DateTimeOffset now, string origin, Action<string>? notice)
    {
        var skipMissing = config.ForEach!.OnMissingSkip;
        if (config.IsFileMode)
        {
            var file = new HttpFilePartition(client, bareClient, connection, config, spec, time, gate, pause)
            {
                Item = values,
                Bindings = bindings,
                FetchedAt = now,
                SkipMissing = skipMissing,
                ItemOrigin = origin,
                Notice = notice,
                ScratchRoot = ScratchRoot,
                IdempotencyScope = IdempotencyScope,
            };
            return (file, () => file.Missing, () => false);
        }

        var pages = new HttpPartition(client, bareClient, connection, config, spec, time, gate, pause)
        {
            Item = values,
            Bindings = bindings,
            FetchedAt = now,
            SkipMissing = skipMissing,
            ItemOrigin = origin,
            Allocator = Allocator,
        };
        return (pages, () => pages.Missing, () => pages.TruncatedWithMore);
    }

    /// <summary>Advances an item's batches, with the item's identity on any failure.</summary>
    private static async Task<bool> NextAsync(IAsyncEnumerator<RecordBatch> batches, ItemValues values)
    {
        try
        {
            return await batches.MoveNextAsync().ConfigureAwait(false);
        }
        catch (PzConnectorException ex)
        {
            throw WithItem(ex, values);
        }
    }

    /// <summary>An accepted checkpoint that a list or item partition then refused. TryResumeFrom applies
    /// their own checks, so this means the two drifted apart.</summary>
    private PzConnectorException Unusable()
        => new($"{Label}: the stored checkpoint no longer fits this dataset; re-run to start the read over",
            isTransient: false);

    /// <summary>The item's failure with its identity appended, classified exactly as before.</summary>
    private static PzConnectorException WithItem(PzConnectorException ex, ItemValues item)
        => new($"{ex.Message} ({item.Identity()})", ex.IsTransient, ex.RetryAfter, innerException: ex,
            code: ex.Code, hint: ex.Hint);
}
