using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>A for_each dataset read over JSON pages: crawl the list request page by page and, for each
/// list record in order, crawl the item request built from that record's fields to its last page before
/// the next record starts. The clock is read once, so the list and every item share one
/// {{ run_date }} and one <c>pz_fetched_at</c>. Never retries, never sleeps.</summary>
internal sealed class HttpForEachPartition(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpDatasetConfig config, DatasetSpec spec, TimeProvider time, IOperationGate? gate)
    : ICheckpointingPartition
{
    private string Label => $"http source '{spec.Source}.{spec.Dataset}'";

    /// <summary>Where a non-fatal notice goes (a cap that stopped the read, items skipped under
    /// `on_missing: skip`); null when the engine subscribed none.</summary>
    public Action<string>? Notice { get; init; }

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
                var item = new HttpPartition(client, bareClient, connection, config, spec, time, gate)
                {
                    Item = values,
                    Bindings = bindings,
                    FetchedAt = now,
                    SkipMissing = forEach.OnMissingSkip,
                    ItemOrigin = $"list page '{list.RedactUrl(page.Request.Url)}', record {index}",
                };

                await using var batches = item.ReadAsync(options, ct).GetAsyncEnumerator(ct);
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

                if (item.Missing)
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

    /// <summary>The item's failure with its identity appended, classified exactly as before.</summary>
    private static PzConnectorException WithItem(PzConnectorException ex, ItemValues item)
        => new($"{ex.Message} ({item.Identity()})", ex.IsTransient, ex.RetryAfter, innerException: ex,
            code: ex.Code, hint: ex.Hint);
}
