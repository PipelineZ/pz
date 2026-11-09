using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Bindings;

namespace Pz.Connector.Http;

/// <summary>The parallel path, for <c>for_each.concurrency</c> above 1. The records of one list page are
/// handed out in record order to up to N workers, each reading its item to the end and writing the
/// batches into a channel of capacity N that the read yields from, so rows of different items interleave
/// while each item's rows keep their order. The next list page is requested only once every item of the
/// current one has finished (a barrier). Positions inside a page are not exact once items interleave, so
/// checkpoints are offered only at barriers: the page's last batch is held back until all of its items
/// have finished, and the token naming the next list page at record 0 is set before it is yielded.</summary>
internal sealed partial class HttpForEachPartition
{
    private async IAsyncEnumerable<RecordBatch> ReadParallelAsync(HttpPartition list,
        IReadOnlyDictionary<string, BindingValue> bindings, DateTimeOffset now, HttpCheckpointToken.ForEachToken? resume,
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var forEach = config.ForEach!;
        // As on the sequential path: items counted per attempt, skips carried over from the checkpoint.
        var items = 0;
        var skipped = resume?.Skipped ?? 0;
        var firstSkipped = resume?.FirstSkipped;
        var truncated = 0;
        string? firstTruncated = null;
        var capped = false;
        var firstPage = true;
        // A file item reports its own notices (a zip with no matching member) from whichever worker runs it;
        // the subscriber sees them one at a time.
        var noticeLock = new Lock();
        Action<string>? notice = Notice is { } sink
            ? message =>
            {
                lock (noticeLock)
                {
                    sink(message);
                }
            }
            : null;

        await foreach (var page in list.PagesAsync(ct).ConfigureAwait(false))
        {
            var (start, resumeItem) = firstPage && resume is not null ? ResumeAt(resume, page) : (0, null);
            firstPage = false;
            // max_items hands out the page's first records only, in record order.
            var available = Math.Max(0, page.Records.Count - start);
            var count = forEach.MaxItems is { } max ? Math.Min(available, max - items) : available;
            items += count;

            await using (var run = new PageRun(this, forEach, page, start, count, resumeItem, bindings, now, list, notice,
                             options, ct))
            {
                RecordBatch? held = null;
                try
                {
                    // The channel completes once every worker has finished. Each batch read holds the one
                    // before it back, so the last one is still here when the page is done.
                    while (!run.Failed && await run.Batches.WaitToReadAsync(ct).ConfigureAwait(false))
                    {
                        while (!run.Failed && run.Batches.TryRead(out var next))
                        {
                            var ready = held;
                            held = next;
                            if (ready is not null)
                            {
                                // Mid-page: no position covers exactly the rows yielded so far.
                                _checkpointCandidate = null;
                                yield return ready;
                            }
                        }
                    }

                    // Waits for every worker; throws the failure of the lowest record that failed.
                    await run.FinishAsync().ConfigureAwait(false);
                    ct.ThrowIfCancellationRequested();

                    skipped += run.Skips.Count;
                    firstSkipped ??= run.Skips.First;
                    truncated += run.Truncations.Count;
                    firstTruncated ??= run.Truncations.First;

                    if (count < available)
                    {
                        Notice?.Invoke($"{Label}: stopped after max_items ({forEach.MaxItems}) items; more list records were available");
                        capped = true;
                    }
                    else if (forEach.MaxItems is { } cap && items >= cap && page.Next is not null)
                    {
                        // The cap reached on the page's last record: stop here rather than fetch a list
                        // page whose records would not be read.
                        Notice?.Invoke($"{Label}: stopped after max_items ({cap}) items; more list records may be available");
                        capped = true;
                    }

                    if (held is { } last)
                    {
                        held = null;
                        // The barrier: every item of this page has finished, so the next list page from
                        // its first record is exactly what remains. None when the read ends here. A page
                        // that yielded nothing sets none: any batch after it belongs to a later page, past
                        // that position, and the later page's own barrier carries these skip counts.
                        _checkpointCandidate = page.Next is { } nextPage && !capped
                            ? HttpCheckpointToken.EncodeForEach(new(HttpCheckpointToken.Encode(nextPage, forEach.List.IsPost,
                                connection.CheckpointStrippedParams),
                                0, null, null, skipped, firstSkipped))
                            : null;
                        yield return last;
                    }
                }
                finally
                {
                    // Not yet handed to the engine, so still this read's to release.
                    held?.Dispose();
                }
            }

            if (capped)
            {
                break;
            }
        }

        EndNotices(capped, list, truncated, firstTruncated, skipped, firstSkipped);
    }

    /// <summary>Items of one list page counted by an outcome (skipped, or cut short by <c>max_pages</c>),
    /// with the identity of the lowest record among them, whatever order the workers finished in.</summary>
    private sealed class ItemTally
    {
        private readonly Lock _lock = new();
        private int _firstIndex = int.MaxValue;

        public int Count { get; private set; }

        public string? First { get; private set; }

        public void Add(int index, string identity)
        {
            lock (_lock)
            {
                Count++;
                if (index < _firstIndex)
                {
                    _firstIndex = index;
                    First = identity;
                }
            }
        }
    }

    /// <summary>The workers of one list page and the channel they fill. Every batch a worker reads is
    /// written to the channel or disposed by that worker; every batch in the channel is read by the
    /// partition or disposed here once the workers are done (<see cref="FinishAsync"/> or
    /// <see cref="DisposeAsync"/>, however the read ends).</summary>
    private sealed class PageRun : IAsyncDisposable
    {
        private readonly HttpForEachPartition _owner;
        private readonly HttpForEachConfig _forEach;
        private readonly HttpPartition.HttpPage _page;
        private readonly int _start;
        private readonly int _count;
        private readonly string? _resumeItem;
        private readonly IReadOnlyDictionary<string, BindingValue> _bindings;
        private readonly DateTimeOffset _now;
        private readonly HttpPartition _list;
        private readonly Action<string>? _notice;
        private readonly BatchOptions _options;
        private readonly Channel<RecordBatch> _channel;
        // Cancelled by the read's own token, by the first failure, and when the read stops early.
        private readonly CancellationTokenSource _stop;
        private readonly Lock _lock = new();
        private readonly Task _workers;
        private (int Index, Exception Error)? _failure;
        private int _handedOut;

        public PageRun(HttpForEachPartition owner, HttpForEachConfig forEach, HttpPartition.HttpPage page, int start, int count, string? resumeItem,
            IReadOnlyDictionary<string, BindingValue> bindings, DateTimeOffset now, HttpPartition list,
            Action<string>? notice, BatchOptions options, CancellationToken ct)
        {
            _owner = owner;
            _forEach = forEach;
            _page = page;
            _start = start;
            _count = count;
            _resumeItem = resumeItem;
            _bindings = bindings;
            _now = now;
            _list = list;
            _notice = notice;
            _options = options;
            var concurrency = forEach.Concurrency;
            _channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(concurrency)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
            _stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            // Fewer records than workers (or none at all) start fewer workers.
            _workers = RunAsync(Math.Min(concurrency, count));
        }

        public ChannelReader<RecordBatch> Batches => _channel.Reader;

        public ItemTally Skips { get; } = new();

        public ItemTally Truncations { get; } = new();

        public bool Failed
        {
            get
            {
                lock (_lock)
                {
                    return _failure is not null;
                }
            }
        }

        private async Task RunAsync(int workers)
        {
            try
            {
                await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(WorkAsync))).ConfigureAwait(false);
            }
            finally
            {
                _channel.Writer.TryComplete();
            }
        }

        /// <summary>Takes the next record in record order until none is left or the page is stopped. Never
        /// throws: a failure is recorded with its record index.</summary>
        private async Task WorkAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                var offset = Interlocked.Increment(ref _handedOut) - 1;
                if (offset >= _count)
                {
                    return;
                }

                var index = _start + offset;
                try
                {
                    await ReadItemAsync(index, _stop.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_stop.IsCancellationRequested)
                {
                    // Stopped by another item's failure or by the read itself: not a failure of this item. Only
                    // a bare cancellation counts: a PzConnectorException is the item's own failure even when it
                    // wraps one (a request timeout), and Fail keeps the lowest record's.
                    return;
                }
                catch (Exception ex)
                {
                    Fail(index, ex);
                    return;
                }
            }
        }

        private async Task ReadItemAsync(int index, CancellationToken ct)
        {
            var values = ItemValues.From(_page.Records[index], _forEach.Fields);
            var item = _owner.ItemRead(values, _bindings, _now,
                $"list page '{_list.Redact(_page.Request.Url)}', record {index}", _notice);
            if (index == _start && _resumeItem is not null && !item.Partition.TryResumeFrom(_resumeItem))
            {
                throw _owner.Unusable();
            }

            // Disposing the item's enumerator ends its read, removing a file item's scratch directory.
            await using (var batches = item.Partition.ReadAsync(_options, ct).GetAsyncEnumerator(ct))
            {
                while (await NextAsync(batches, values).ConfigureAwait(false))
                {
                    var batch = batches.Current;
                    try
                    {
                        await _channel.Writer.WriteAsync(batch, ct).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Never reached the channel, so still this worker's to release.
                        batch.Dispose();
                        throw;
                    }
                }
            }

            if (item.Missing())
            {
                Skips.Add(index, values.Identity());
            }

            if (item.Truncated())
            {
                Truncations.Add(index, values.Identity());
            }
        }

        /// <summary>Keeps the failure of the lowest record and stops the other workers.</summary>
        private void Fail(int index, Exception error)
        {
            lock (_lock)
            {
                if (_failure is null || index < _failure.Value.Index)
                {
                    _failure = (index, error);
                }
            }

            _stop.Cancel();
        }

        /// <summary>Waits for every worker, releases whatever the read did not take, and throws the failure
        /// of the lowest record that failed, if any.</summary>
        public async Task FinishAsync()
        {
            await _workers.ConfigureAwait(false);
            Drain();
            (int Index, Exception Error)? failure;
            lock (_lock)
            {
                failure = _failure;
            }

            if (failure is { } f)
            {
                ExceptionDispatchInfo.Throw(f.Error);
            }
        }

        public async ValueTask DisposeAsync()
        {
            // Stops workers still running when the read ends early; harmless once they are done.
            await _stop.CancelAsync().ConfigureAwait(false);
            await _workers.ConfigureAwait(false);
            Drain();
            _stop.Dispose();
        }

        private void Drain()
        {
            while (_channel.Reader.TryRead(out var batch))
            {
                batch.Dispose();
            }
        }
    }
}
