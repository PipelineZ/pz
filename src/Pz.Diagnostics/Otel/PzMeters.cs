using System.Diagnostics.Metrics;

namespace Pz.Diagnostics.Otel;

/// <summary>The single <see cref="Meter"/> and its instruments every counter/histogram increment in
/// the engine goes through — BCL-only (<c>System.Diagnostics.DiagnosticSource</c>), so this can live
/// in Pz.Diagnostics without breaking its BCL-only rule. With no
/// <see cref="MeterListener"/>/exporter registered anywhere in the process, <see cref="Counter{T}.Add"/>
/// and <see cref="Histogram{T}.Record"/> are documented BCL no-ops — zero-cost when OTel export isn't
/// configured, mirroring <see cref="PzActivitySource"/>'s guarantee.</summary>
public static class PzMeters
{
    public const string Name = "Pz.Engine";

    private static readonly Meter Meter = new(Name);

    /// <summary>Total rows moved by SourceLoad/SinkWrite nodes; incremented once per node, on
    /// completion, with the node's final row count (never incremented incrementally during progress —
    /// that would double count against the final total).</summary>
    public static readonly Counter<long> RowsMoved = Meter.CreateCounter<long>("pz.rows_moved");

    /// <summary>Bytes moved, incremented at the same every-10-batches progress cadence the engine's
    /// NodeProgress event already uses (delta since the last report, not cumulative — Counter.Add is
    /// itself the accumulation).</summary>
    public static readonly Counter<long> BytesMoved = Meter.CreateCounter<long>("pz.bytes_moved");

    /// <summary>Batch count, incremented at the same progress cadence as <see cref="BytesMoved"/>.</summary>
    public static readonly Counter<long> Batches = Meter.CreateCounter<long>("pz.batches");

    /// <summary>Per-node wall-clock duration in milliseconds, recorded once on node completion.</summary>
    public static readonly Histogram<double> NodeDuration = Meter.CreateHistogram<double>("pz.node.duration", unit: "ms");

    /// <summary>Terminal run outcome, incremented exactly once per run (at RunCompleted) with the
    /// run's final status as the <c>pz.run.status</c> tag ("success" / "completed_with_failures" /
    /// "fatal") — the alertable signal monitoring systems (e.g. Azure Monitor) key on.</summary>
    public static readonly Counter<long> RunsCompleted = Meter.CreateCounter<long>("pz.run.completed");

    /// <summary>Count of universal-tier operation attempts a gate-aware connector's
    /// <c>OperationGate</c> (Pz.Engine) executed for one node, tagged <c>pz.instance</c> (never a
    /// config value — instance key only). Recorded once per node, on success, from the same
    /// <c>OpStats</c> snapshot serialized into <c>NodeResult.Ops</c> — never incremented for
    /// native-tier nodes (no .NET gate) or a failed node.</summary>
    public static readonly Counter<long> OpsExecuted = Meter.CreateCounter<long>("pz.ops.executed");

    /// <summary>Companion to <see cref="OpsExecuted"/>: count of those operation attempts that were
    /// retried by the gate (transient failure, idempotent op, attempts remaining).</summary>
    public static readonly Counter<long> OpsRetried = Meter.CreateCounter<long>("pz.ops.retried");

    /// <summary>Companion to <see cref="OpsExecuted"/>: total milliseconds the gate spent waiting on
    /// pacing (bucket/budget hints) for this node — never retry backoff. Deliberately a
    /// <see cref="Counter{T}"/>, not a <see cref="Histogram{T}"/> like <see cref="NodeDuration"/>: this
    /// accumulates a per-node TOTAL wait, not a per-observation distribution.</summary>
    public static readonly Counter<long> OpsThrottleWait = Meter.CreateCounter<long>("pz.ops.throttle.wait", unit: "ms");
}
