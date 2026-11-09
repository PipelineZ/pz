using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>A for_each dataset read: crawl the list request page by page and, for each list record in
/// order, read the item request built from that record's fields (its JSON pages to the last one, or in
/// file mode its one download) before the next record starts. The clock is read once, so the list and
/// every item share one {{ run_date }} and one <c>pz_fetched_at</c>. Checkpoints name the list page,
/// the record index and the item crawl's position (see <see cref="HttpCheckpointToken"/>). Never retries,
/// never sleeps.</summary>
internal sealed class HttpForEachPartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate)
    : ICheckpointingPartition
{
    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    /// <summary>Where a non-fatal notice goes (a cap that stopped the read, items skipped under
    /// `on_missing: skip`); null when the engine subscribed none.</summary>
    public Action<string>? Notice { get; init; }

    /// <summary>The parent of each file-mode item's scratch directory; null means `Path.GetTempPath()`.
    /// Tests point it at a directory of their own.</summary>
    internal string? ScratchRoot { get; set; }

    public string PartitionId => $"{spec.Source}.{spec.Dataset}";

    // The resume position: the list page's token, the record index on it, and the item crawl's token
    // (null: start the item from its first request). Set by TryResumeFrom, already validated.
    private string? _resumeList;
    private int _resumeIndex;
    private string? _resumeItem;

    // The checkpoint covering exactly the rows yielded so far, or null. Set before every yield, so it
    // never describes a position the engine's staged rows have not reached.
    private string? _checkpointCandidate;
    private string? _lastReturnedCheckpoint;

    public bool TryResumeFrom(string checkpoint)
    {
        // Every embedded token is a position an endpoint chose: it must still have its request's shape and
        // name a host this connection may reach. File items have no position inside them.
        if (!HttpCheckpointToken.TryDecodeForEach(checkpoint, out var listToken, out var index, out var itemToken)
            || !Fits(listToken, config.ForEach!.List)
            || (itemToken is not null && (config.IsFileMode || !Fits(itemToken, config))))
        {
            return false;
        }

        _resumeList = listToken;
        _resumeIndex = index;
        _resumeItem = itemToken;
        return true;
    }

    /// <summary>Whether <paramref name="token"/> is a resume token <see cref="HttpPartition"/> accepts for a
    /// request of <paramref name="request"/>'s method and body kind.</summary>
    private bool Fits(string token, HttpDatasetConfig request)
        => HttpCheckpointToken.TryDecode(token, request.IsPost, request.Method ?? HttpMethod.Get,
               HttpCheckpointToken.KindOf(request.Body), out var decoded)
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
        var list = new HttpPartition(client, bareClient, connection, forEach.List, spec, time, gate)
        {
            Bindings = bindings,
        };

        var resuming = _resumeList is not null;
        if (resuming && !list.TryResumeFrom(_resumeList!))
        {
            throw Unusable();
        }

        // Per attempt, like max_pages: a resumed attempt counts items, and skips, from where it resumed.
        // A skip before the resume position is therefore not reported by the attempt that finishes.
        var items = 0;
        var skipped = 0;
        string? firstSkipped = null;
        var capped = false;
        var firstPage = true;

        await foreach (var page in list.PagesAsync(ct).ConfigureAwait(false))
        {
            var listToken = HttpCheckpointToken.Encode(page.Request, forEach.List.IsPost);
            // An index at or past the page's record count (the page's last item had finished, or the page
            // has since shrunk) starts nothing here: the read continues with the next list page.
            var start = firstPage && resuming ? _resumeIndex : 0;
            var resumeItem = firstPage && resuming ? _resumeItem : null;
            firstPage = false;

            for (var index = start; index < page.Records.Count; index++)
            {
                if (forEach.MaxItems is { } max && items >= max)
                {
                    Notice?.Invoke($"{Label}: stopped after max_items ({max}) items; more list records were available");
                    capped = true;
                    break;
                }

                items++;
                var values = ItemValues.From(page.Records[index], forEach.Fields);
                var item = ItemRead(values, bindings, now,
                    $"list page '{list.RedactUrl(page.Request.Url)}', record {index}");
                if (index == start && resumeItem is not null && !item.Partition.TryResumeFrom(resumeItem))
                {
                    throw Unusable();
                }

                // Before each batch is yielded, the candidate becomes the position just past it: the item
                // crawl's own checkpoint when it offers one for this batch (a page boundary), else the next
                // item when this batch is the item's last. A batch with neither clears it. Finding out
                // whether a batch is the last reads one batch ahead, which for a JSON item stays within
                // the page already fetched (a page with a further page always offers a checkpoint). An item
                // whose last batch carried the crawl's checkpoint ends without an item-boundary one; a resume
                // there re-requests the item's trailing page.
                await using var batches = item.Partition.ReadAsync(options, ct).GetAsyncEnumerator(ct);
                var more = await NextAsync(batches, values).ConfigureAwait(false);
                while (more)
                {
                    var batch = batches.Current;
                    if (item.Partition.TryGetCheckpoint(out var inner) && inner is not null)
                    {
                        _checkpointCandidate = HttpCheckpointToken.EncodeForEach(listToken, index, inner);
                        yield return batch;
                        more = await NextAsync(batches, values).ConfigureAwait(false);
                        continue;
                    }

                    try
                    {
                        more = await NextAsync(batches, values).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Not yet handed to the engine, so still this read's to release.
                        batch.Dispose();
                        throw;
                    }

                    // index + 1 may equal the page's record count; the token keeps this list page, and a
                    // resume from it moves on to the next one.
                    _checkpointCandidate = more ? null : HttpCheckpointToken.EncodeForEach(listToken, index + 1, null);
                    yield return batch;
                }

                if (item.Missing())
                {
                    skipped++;
                    firstSkipped ??= values.Identity();
                }
            }

            if (capped)
            {
                break;
            }
        }

        if (!capped && list.TruncatedWithMore)
        {
            Notice?.Invoke($"{Label}: stopped after for_each.max_pages ({forEach.List.MaxPages}) list pages; " +
                "more list pages were available");
        }

        if (skipped > 0)
        {
            Notice?.Invoke($"{Label}: skipped {skipped} items that returned 404/410 (first: {firstSkipped})");
        }
    }

    /// <summary>One item's read: its JSON pages, or in file mode its download (with its own scratch
    /// directory and decompression budget, released when the read ends however it ends). Missing turns
    /// true once the item's first request was answered 404/410 under `on_missing: skip`.</summary>
    private (ICheckpointingPartition Partition, Func<bool> Missing) ItemRead(ItemValues values,
        IReadOnlyDictionary<string, BindingValue> bindings, DateTimeOffset now, string origin)
    {
        var skipMissing = config.ForEach!.OnMissingSkip;
        if (config.IsFileMode)
        {
            var file = new HttpFilePartition(client, bareClient, connection, config, spec, time, gate)
            {
                Item = values,
                Bindings = bindings,
                FetchedAt = now,
                SkipMissing = skipMissing,
                ItemOrigin = origin,
                Notice = Notice,
                ScratchRoot = ScratchRoot,
            };
            return (file, () => file.Missing);
        }

        var pages = new HttpPartition(client, bareClient, connection, config, spec, time, gate)
        {
            Item = values,
            Bindings = bindings,
            FetchedAt = now,
            SkipMissing = skipMissing,
            ItemOrigin = origin,
        };
        return (pages, () => pages.Missing);
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
