using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.TestKit.Reference;

/// <summary>Deterministic seeded source: given `rows` (required) and `partitions` (default 1), produces
/// exactly that many rows of the fixed 5-column schema, split evenly across partitions with
/// the remainder folded into the last one. No wall clock anywhere.</summary>
public sealed class InMemorySource : ISource
{
    internal static readonly Schema FixedSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
        new Field("amount", DoubleType.Default, nullable: false),
        new Field("flag", BooleanType.Default, nullable: false),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: false),
    ], null);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(FixedSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        // Test-only escape hatch: an explicit list of per-partition row counts, bypassing the normal
        // even "rows"/"partitions" split below. Exists so concurrency tests can construct one tiny
        // (fails almost instantly) partition alongside one large (healthy, still busy when the tiny one
        // faults) partition — a split the even-division rule below cannot produce.
        if (spec.Options.TryGetValue("partition_sizes", out var sizesValue) && sizesValue is IReadOnlyList<long> sizes)
        {
            var explicitPartitions = new List<IDatasetPartition>(sizes.Count);
            var explicitStart = 0L;
            foreach (var count in sizes)
            {
                explicitPartitions.Add(new InMemoryPartition(explicitStart, count, spec));
                explicitStart += count;
            }

            return new ValueTask<IReadOnlyList<IDatasetPartition>>(explicitPartitions);
        }

        if (!spec.Options.TryGetValue("rows", out var rowsValue) || rowsValue is null)
        {
            throw new PzConnectorException("dataset option 'rows' is required", isTransient: false);
        }

        var rows = Convert.ToInt64(rowsValue, CultureInfo.InvariantCulture);
        var partitionCount = spec.Options.TryGetValue("partitions", out var partitionsValue) && partitionsValue is not null
            ? Convert.ToInt32(partitionsValue, CultureInfo.InvariantCulture)
            : 1;

        if (partitionCount < 1)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': option 'partitions' must be >= 1, got {partitionCount}",
                isTransient: false);
        }

        var baseSize = rows / partitionCount;
        var remainder = rows % partitionCount;
        var partitions = new List<IDatasetPartition>(partitionCount);
        var start = 0L;
        for (var p = 0; p < partitionCount; p++)
        {
            var count = baseSize + (p == partitionCount - 1 ? remainder : 0);
            partitions.Add(new InMemoryPartition(start, count, spec));
            start += count;
        }

        return new ValueTask<IReadOnlyList<IDatasetPartition>>(partitions);
    }

    public ValueTask DisposeAsync() => default;
}

internal sealed class InMemoryPartition(long rangeStart, long rangeCount, DatasetSpec spec) : IDatasetPartition
{
    private static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();

        var failAtBatch = FaultInjection.GetInt(spec.Options, "fail_read_at_batch");
        var failTransient = FaultInjection.GetBool(spec.Options, "fail_transient");

        // Redaction-narrowing test seam: PzConnectorException is a Pz-family type (see
        // MessageRedaction.Redact(Exception)'s trust boundary doc), so a test that wants to prove a
        // FOREIGN exception still gets redacted needs a way to make this fault-injection path throw
        // something other than PzConnectorException.
        var failForeign = FaultInjection.GetBool(spec.Options, "fail_foreign");

        // Test seam: lets a test simulate a connector that echoes a raw engine
        // error (e.g. a DuckDB "LINE <n>: ..." statement-echo) into its PzConnectorException message, to
        // prove the retry loop sanitizes `reason` before publishing `retry_scheduled`. Absent, the
        // fixed message below is used.
        var failMessage = spec.Options.TryGetValue("fail_message", out var failMessageValue) && failMessageValue is string s
            ? s
            : "injected read failure";

        // When present, the injected PzConnectorException below carries this as its
        // RetryAfter (seconds -- see FaultInjection.GetRetryAfter's doc), letting a test prove the
        // engine's breaker/retry catches forward ex.RetryAfter rather than always passing null.
        // Absent: RetryAfter stays null.
        var failRetryAfter = FaultInjection.GetRetryAfter(spec.Options, "fail_retry_after");

        // Opt-in: when present, only the first N reads at failAtBatch actually fault;
        // the (N+1)-th and later calls let the batch through. Absent: every attempt at failAtBatch
        // faults, unconditionally.
        var retryCounter = FaultInjection.GetRetryCounter(spec.Options, "fail_read_retry_limit");

        // Test-only observer: invoked with the cumulative row count this partition has processed so far.
        // Lets a test prove a partition was cancelled early (count << rangeCount) rather than run to
        // completion — there's no other way to see a partition's progress from outside.
        var rowsReadHook = spec.Options.TryGetValue("rows_read_hook", out var hookValue)
            ? hookValue as Action<long>
            : null;

        // Opt-in: an artificial delay applied before each batch is yielded, simulating a
        // slow read (network/disk-bound source). Lets StallAttributionTests prove the channel's
        // consumer side (SourceLoadExecutor's ingest drain) dominates the stall breakdown when the
        // SOURCE, not the sink/ingest, is the bottleneck — the mirror image of the
        // `OnBatchConsumedForTests` seam that slows the ingest side instead. Absent: no delay.
        var readDelayMs = FaultInjection.GetInt(spec.Options, "read_delay_ms");

        // A deterministic delta lever for tests -- when the executor set a
        // watermark on the spec (see SpecBuilder.ForSourceLoad's execution-time overload), skip every row
        // whose `id` (the fixed schema's int64 column) is <= the parsed cursor value, simulating a
        // connector applying `cursor > value` server-side. No real connector's config reads these fields
        // yet; this is InMemory-only, purely to make incremental engine tests deterministic without a
        // wall clock or a real connector's filter pushdown.
        var watermarkThreshold = spec.WatermarkCursor is not null && spec.WatermarkValue is not null
            ? long.Parse(spec.WatermarkValue, CultureInfo.InvariantCulture)
            : (long?)null;

        // Mirrors watermarkThreshold's lower-bound lever above -- when the
        // executor set an upper bound on the spec (see SpecBuilder.ForSourceLoad's bounded-window
        // overload), additionally skip every row whose `id` is > the parsed bound, simulating a
        // BoundedWindow-capable connector applying `cursor <= value` server-side. Combined with the
        // existing `id <= threshold` skip below, this makes the filter `threshold < id <= upperBound`.
        var watermarkUpperBound = spec.WatermarkUpperBound is not null
            ? long.Parse(spec.WatermarkUpperBound, CultureInfo.InvariantCulture)
            : (long?)null;

        // Test seam: simulates a misbehaving connector on the
        // universal tier that ignores DatasetSpec.WatermarkLowerBound/WatermarkUpperBound entirely (ISource
        // implementations MAY honor bounds, not MUST) -- ships every row in the partition's range
        // regardless of the two filters above, so a test can drive over-delivery through the engine's real
        // universal ingest path and prove the engine-side staging trim (not just the
        // candidate-cap/scoped-MAX) is what keeps staging content in-window. Absent: both bound filters
        // below apply.
        var ignoreWatermarkBounds = FaultInjection.GetBool(spec.Options, "ignore_watermark_bounds");

        var builder = new ArrowBatchBuilder(InMemorySource.FixedSchema, options.TargetBatchBytes);
        var batchOrdinal = 0;

        for (var offset = 0L; offset < rangeCount; offset++)
        {
            ct.ThrowIfCancellationRequested();
            var id = rangeStart + offset;
            if (!ignoreWatermarkBounds && watermarkThreshold is { } threshold && id <= threshold)
            {
                continue;
            }

            if (!ignoreWatermarkBounds && watermarkUpperBound is { } upperBound && id > upperBound)
            {
                continue;
            }

            builder.AppendRow([id, $"row-{id}", id * 1.5, id % 2 == 0, Epoch.AddSeconds(id)]);
            rowsReadHook?.Invoke(offset + 1);

            if (builder.TryTakeBatch(out var batch))
            {
                if (readDelayMs is > 0)
                {
                    await Task.Delay(readDelayMs.Value, ct).ConfigureAwait(false);
                }

                if (failAtBatch == batchOrdinal && (retryCounter?.ShouldFail() ?? true))
                {
                    batch!.Dispose();
                    if (failForeign)
                    {
                        throw new InvalidOperationException(failMessage);
                    }

                    throw new PzConnectorException(failMessage, failTransient, failRetryAfter);
                }

                batchOrdinal++;
                yield return batch!;
            }
        }

        var remainder = builder.Flush();
        if (remainder is not null)
        {
            if (readDelayMs is > 0)
            {
                await Task.Delay(readDelayMs.Value, ct).ConfigureAwait(false);
            }

            if (failAtBatch == batchOrdinal && (retryCounter?.ShouldFail() ?? true))
            {
                remainder.Dispose();
                if (failForeign)
                {
                    throw new InvalidOperationException(failMessage);
                }

                throw new PzConnectorException(failMessage, failTransient, failRetryAfter);
            }

            yield return remainder;
        }
    }
}
