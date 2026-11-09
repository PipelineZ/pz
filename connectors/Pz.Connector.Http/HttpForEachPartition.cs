using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>A for_each dataset read: crawl the list request page by page and, for each list record in
/// order, read the item request built from that record's fields (its JSON pages to the last one, or in
/// file mode its one download) before the next record starts. The clock is read once, so the list and
/// every item share one {{ run_date }} and one <c>pz_fetched_at</c>. Never retries, never sleeps.</summary>
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

    public bool TryResumeFrom(string checkpoint) => false;

    public bool TryGetCheckpoint(out string? checkpoint)
    {
        checkpoint = null;
        return false;
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

        var items = 0;
        var skipped = 0;
        string? firstSkipped = null;
        var capped = false;

        await foreach (var page in list.PagesAsync(ct).ConfigureAwait(false))
        {
            for (var index = 0; index < page.Records.Count; index++)
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

                await using var batches = item.Partition.ReadAsync(options, ct).GetAsyncEnumerator(ct);
                while (true)
                {
                    try
                    {
                        if (!await batches.MoveNextAsync().ConfigureAwait(false))
                        {
                            break;
                        }
                    }
                    catch (PzConnectorException ex)
                    {
                        throw WithItem(ex, values);
                    }

                    yield return batches.Current;
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
    private (IDatasetPartition Partition, Func<bool> Missing) ItemRead(ItemValues values,
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

    /// <summary>The item's failure with its identity appended, classified exactly as before.</summary>
    private static PzConnectorException WithItem(PzConnectorException ex, ItemValues item)
        => new($"{ex.Message} ({item.Identity()})", ex.IsTransient, ex.RetryAfter, innerException: ex,
            code: ex.Code, hint: ex.Hint);
}
