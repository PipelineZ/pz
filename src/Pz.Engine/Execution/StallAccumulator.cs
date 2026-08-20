namespace Pz.Engine.Execution;

/// <summary>Per-node channel stall attribution. Threaded through
/// <see cref="SourceLoadExecutor"/> and <see cref="SinkWriteExecutor"/>: each side of the node's internal
/// channel times itself with <see cref="TimeProvider.GetTimestamp"/> deltas (read through
/// <see cref="Timestamp"/> so the measurement source and the conversion frequency in
/// <see cref="ToTimings"/> can never disagree) around the ONE await that already exists there (never
/// per-row) and reports the elapsed ticks here via <see cref="Interlocked.Add"/> — safe when multiple
/// partition-pump tasks call <see cref="AddProducer"/> concurrently (SourceLoad pumps every partition in
/// parallel). Production passes <c>RunContext.EffectiveTime</c>, i.e. <see cref="TimeProvider.System"/>,
/// whose <c>GetTimestamp</c> is byte-for-byte <c>Stopwatch.GetTimestamp</c> (~20-40ns per call, two per
/// BATCH-level await — three-plus orders of magnitude below the I/O being measured); tests inject a fake
/// clock so stall dominance is asserted deterministically instead of over load-sensitive wall
/// time.</summary>
public sealed class StallAccumulator(TimeProvider time)
{
    private long _producerTicks;
    private long _consumerTicks;

    /// <summary>Current timestamp on this accumulator's clock — bracket sites take start/end from here.</summary>
    public long Timestamp => time.GetTimestamp();

    /// <summary>Records ticks spent stalled on the producer side of this node's channel: SourceLoad's
    /// partition-pump blocked on <c>writer.WriteAsync</c> (channel full ⇒ ingest is the bottleneck);
    /// SinkWrite's egress reader blocked on <c>QueryArrowAsync</c>'s <c>MoveNextAsync</c> (DuckDB egress
    /// slow).</summary>
    public void AddProducer(long elapsedTicks) => Interlocked.Add(ref _producerTicks, elapsedTicks);

    /// <summary>Records ticks spent stalled on the consumer side of this node's channel: SourceLoad's
    /// ingest drain blocked waiting for the next batch (channel empty ⇒ source is the bottleneck);
    /// SinkWrite's <c>WriteBatchAsync</c> blocked on the sink (sink slow).</summary>
    public void AddConsumer(long elapsedTicks) => Interlocked.Add(ref _consumerTicks, elapsedTicks);

    /// <summary>Snapshot as wall-clock <see cref="TimeSpan"/>s. Safe to call once, after every producer
    /// and consumer side of the node's channel has finished (no further <see cref="AddProducer"/>/
    /// <see cref="AddConsumer"/> calls are expected once this is read).</summary>
    public NodeTimings ToTimings() => new(
        TicksToTimeSpan(Interlocked.Read(ref _producerTicks)),
        TicksToTimeSpan(Interlocked.Read(ref _consumerTicks)));

    private TimeSpan TicksToTimeSpan(long timestampTicks) =>
        TimeSpan.FromSeconds(timestampTicks / (double)time.TimestampFrequency);
}

/// <summary>Additive per-node timing breakdown. <c>null</c> on <see cref="NodeResult"/> for
/// node kinds with no channel (Pipeline/Check) and for any node whose execution never reached the
/// channel-instrumented path (e.g. the native-scan/native-copy tiers, which bypass the Arrow channel
/// entirely).</summary>
public sealed record NodeTimings(TimeSpan ProducerStall, TimeSpan ConsumerStall);
