namespace Pz.Diagnostics.Events;

/// <summary>Base of the typed run-event stream. Every derived record carries
/// only primitives (strings/numbers) — never engine types like <c>NodeResult</c>/<c>DagNode</c>/<c>PzError</c>,
/// which would invert Pz.Diagnostics' BCL-only dependency direction. <see cref="At"/> is stamped by the
/// publisher via an injectable <see cref="TimeProvider"/> (default <see cref="TimeProvider.System"/>;
/// tests inject a fixed clock for determinism).</summary>
public abstract record RunEvent(DateTimeOffset At, string RunId);

public sealed record RunStartedEvent(DateTimeOffset At, string RunId, string ProjectName, int NodeCount)
    : RunEvent(At, RunId);

public sealed record NodeStartedEvent(DateTimeOffset At, string RunId, string NodeId, string Kind, string Name)
    : RunEvent(At, RunId);

/// <summary>Deliberately has no `rate` field: rows/sec is derivable presentation a renderer computes
/// from successive events' <see cref="RunEvent.At"/>
/// timestamps, not a value worth baking into the schema contract.</summary>
public sealed record NodeProgressEvent(DateTimeOffset At, string RunId, string NodeId, string Name,
    long Rows, long Bytes, long Batches) : RunEvent(At, RunId);

public sealed record RetryScheduledEvent(DateTimeOffset At, string RunId, string NodeId, string Name,
    int Attempt, int MaxAttempts, long DelayMs, string Reason) : RunEvent(At, RunId);

/// <summary>Fired whenever a source/sink instance's circuit breaker
/// transitions state. Unlike every other event in this stream, it is not part of any single node's
/// lifecycle — <see cref="Instance"/> is the shared instance key (<c>"source:&lt;name&gt;"</c> /
/// <c>"sink:&lt;name&gt;"</c>, same granularity as <c>max_concurrency</c>) a breaker gates every node of,
/// so this can fire before, between, or independent of any one node's own start/completion. A project
/// with no <c>engine.breaker:</c> configured never publishes it at all. <see cref="CoolDownMs"/> is only
/// meaningful for a transition INTO <c>open</c> (the wait the executor gate will honor next); every other
/// transition (<c>open</c> → <c>half_open</c>, <c>half_open</c> → <c>closed</c>) carries <c>0</c>.</summary>
public sealed record BreakerStateChangedEvent(DateTimeOffset At, string RunId, string Instance, string OldState,
    string NewState, string Trigger, long CoolDownMs) : RunEvent(At, RunId);

/// <summary><see cref="Timings"/> is null for node kinds with no channel (Pipeline/Check) and for any
/// node whose execution bypassed the channel-instrumented path (the native-scan/native-copy tiers).
/// Populated for SourceLoad/SinkWrite nodes that went through their Arrow channel.
/// <see cref="Provenance"/> is additive (defaulted, so a construction that omits it still compiles):
/// null for a normally-executed node, otherwise the same wire values as <c>run_results.json</c> —
/// <c>"reused"</c> / <c>"carried_forward"</c>.
/// <see cref="Ops"/> is additive (also defaulted): the gate's operation-stats snapshot
/// for a gate-aware connector's universal-tier success, null when the connector isn't gate-aware, the
/// tier was native, or the node failed — same wire shape as <c>run_results.json</c>'s <c>ops</c>
/// object. <see cref="Partitions"/> is additive (also defaulted): partition-mode
/// extraction stats, null for non-partition-mode nodes — same wire shape as <c>run_results.json</c>'s
/// <c>partitions</c> object.
/// <see cref="Delivery"/> is additive (also defaulted): honest-abort/delivery-resume
/// stats, null unless a non-DiscardsAll sink failed or a checkpoint resume was accepted.
/// <see cref="Cdc"/> is additive (also defaulted): raw per-op change
/// counts for a cdc-shaped SourceLoad's successful collapse, null for every non-cdc dataset.</summary>
public sealed record NodeCompletedEvent(DateTimeOffset At, string RunId, string NodeId, string Kind,
    string Name, string Status, long Rows, long DurationMs, string? ErrorCode, string? ErrorMessage,
    NodeTimingsPayload? Timings, string? Provenance = null, OpStatsPayload? Ops = null,
    PartitionStatsPayload? Partitions = null, DeliveryPayload? Delivery = null,
    CdcPayload? Cdc = null) : RunEvent(At, RunId);

public sealed record RunCompletedEvent(DateTimeOffset At, string RunId, string Status, int Succeeded,
    int Failed, int Skipped, long DurationMs) : RunEvent(At, RunId);

/// <summary>One sweep of <c>.pz/runs</c> at the end of a run.
/// Published by <c>RunCommand</c> (not by <c>RunEventPublisher</c>) — retention is a CLI-side finalize
/// action, not part of any node's lifecycle and not an <c>IRunEvents</c> concern — and it is therefore
/// the LAST event of the stream, arriving after <see cref="RunCompletedEvent"/>. A project with
/// <c>retention: off</c>, and a sweep that freed nothing, publish it not at all.
///
/// Counts and byte totals only: never a run id of a SWEPT directory, never a path. <see cref="Failures"/>
/// is the number of directories the sweep could not delete; the full list stays in `pz clean`'s report,
/// because a run's event stream is not the place to enumerate them.</summary>
public sealed record RetentionSweptEvent(DateTimeOffset At, string RunId, int RunsSwept, long BytesFreed,
    int Failures) : RunEvent(At, RunId);

/// <summary>Fired by the SourceLoad drift gate under warn AND
/// fail, before the node's completion event. Observed carries the full new schema (plus
/// HintsHash) so an event consumer can implement "accept" purely from this event. Column names/types
/// only — never connection config.</summary>
public sealed record SourceDriftDetectedEvent(DateTimeOffset At, string RunId, string NodeId,
    string Connection, string Entity, string Policy, IReadOnlyList<DriftChangePayload> Changes,
    IReadOnlyList<SchemaColumnPayload> Observed, string HintsHash) : RunEvent(At, RunId);

/// <summary>Fired by <c>SinkWriteExecutor</c> for a <c>strategy: merge</c>
/// output whose staged input holds more than one row for at least one merge-key group, before that node's
/// own completion event. The in-batch collapse keeps one connector-determined survivor per key (physical
/// staging order, NOT cursor order — the documented Absorb contract), so a stale row can win over a newer
/// one while the watermark still advances past both; this event makes that collapse loud instead of
/// silent. A warning, never a failure: event-log-shaped inputs legitimately carry duplicate keys. Key
/// column names and counts only — never row values.</summary>
public sealed record MergeKeyDuplicatesDetectedEvent(DateTimeOffset At, string RunId, string NodeId,
    string Output, IReadOnlyList<string> Keys, long DuplicateGroups, long ExtraRows) : RunEvent(At, RunId);

/// <summary>Fired by the SourceLoad integer-inference
/// lint when a schema-inferred (contract-less csv/json) read stages a DOUBLE column whose non-null
/// values are all finite integers with at least one beyond 2^53 — DuckDB auto-detect's shape for a
/// &gt;int64 integer column, so digits may already have been silently lost. A warning, never a
/// failure: genuinely floating-point data can look integral. The remedy is a <c>columns:</c> contract
/// (<c>bigint</c>/<c>ubigint</c>/<c>hugeint</c>), which loads such values losslessly and fails loudly
/// on overflow. Column names only — never row values.</summary>
public sealed record LossyIntegerInferenceDetectedEvent(DateTimeOffset At, string RunId, string NodeId,
    string Connection, string Entity, IReadOnlyList<string> Columns) : RunEvent(At, RunId);

/// <summary>Fired by the SourceLoad ambiguous-date
/// lint when a schema-inferred csv read's sniffed date/timestamp format is a day-first/month-first
/// family (e.g. <c>%d/%m/%Y</c>) and no staged value's day exceeds 12 — every value was ambiguous, so
/// the sniffer's field-order pick was a guess and a month-first source is misread on every row. A
/// warning, never a failure: the data may genuinely be day-first. The escape hatch when it is not:
/// normalize the source to ISO 8601, or declare the column <c>varchar</c> in a <c>columns:</c>
/// contract and parse it explicitly in SQL. Column names and the picked format only — never row
/// values.</summary>
public sealed record AmbiguousDateInferenceDetectedEvent(DateTimeOffset At, string RunId, string NodeId,
    string Connection, string Entity, IReadOnlyList<string> Columns, string Format) : RunEvent(At, RunId);

public sealed record NodeTimingsPayload(long ProducerStallMs, long ConsumerStallMs);

/// <summary>BCL-only twin of <c>Pz.Engine.Resilience.OpStats</c> — Pz.Diagnostics
/// cannot reference Pz.Engine, so <c>RunEventPublisher.ToOpsPayload</c> maps one onto the
/// other, exactly like <see cref="NodeTimingsPayload"/> mirrors <c>NodeTimings</c>.</summary>
public sealed record OpStatsPayload(long Executed, long Retried, long ThrottleWaitMs);

/// <summary>BCL-only twin of <c>Pz.Engine.Execution.PartitionStats</c> — same
/// mapping discipline as <see cref="OpStatsPayload"/>. Counts only; never partition ids.</summary>
public sealed record PartitionStatsPayload(long Total, long Completed, long Reused, long Resumed);

/// <summary>BCL-only twin of <c>Pz.Engine.Execution.DeliveryStats</c> —
/// same mapping discipline as <see cref="PartitionStatsPayload"/>. The abort-semantics word
/// (discards_all/best_effort/none) and counts only; never URLs, paths, keys, or tokens.</summary>
public sealed record DeliveryPayload(string AbortSemantics, long RowsVisible, long ResumedRows);

/// <summary>BCL-only twin of <c>Pz.Engine.Execution.CdcStats</c> —
/// same mapping discipline as <see cref="DeliveryPayload"/>. Raw per-op counts of the change window
/// landed before the last-event-per-key collapse — never net counts. <see cref="Position"/> is the
/// opaque candidate token (LSN / CDC log position) the cdc partition emitted for this window, or
/// <c>null</c> when the connector emitted none.</summary>
public sealed record CdcPayload(long Inserts, long Updates, long Deletes, string? Position);

/// <summary>BCL-only twin of <c>Pz.Engine.State.SchemaDriftDiffer.Change</c> — same mapping
/// discipline as <see cref="CdcPayload"/>. <see cref="Kind"/> is the wire
/// word: "added" | "removed" | "retyped".</summary>
public sealed record DriftChangePayload(string Kind, string Column, string? From, string? To);

/// <summary>BCL-only twin of <c>Pz.Engine.State.SchemaColumn</c> — same mapping discipline as
/// <see cref="DriftChangePayload"/>.</summary>
public sealed record SchemaColumnPayload(string Name, string Type);
