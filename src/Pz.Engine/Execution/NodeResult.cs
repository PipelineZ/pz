using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Resilience;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

public enum NodeStatus { Success, Failed, Skipped }

/// <summary>How a node's recorded result came to be: absent/null =
/// executed normally this run. Reused = a retried SourceLoad whose staged table was copied from the
/// failed run's staging instead of re-extracting. CarriedForward = a SinkWrite that succeeded in the
/// prior run and was recorded into THIS run's results (never dispatched) because every SourceLoad
/// ancestor was reused byte-identically — the record that lets WatermarkAdvancement's
/// all-sinks-committed rule pass after a successful retry.</summary>
public enum NodeProvenance { Reused, CarriedForward }

/// <summary>Partition-scoped extraction stats: populated only when partition
/// mode ran, on the successful attempt (a failed attempt throws and builds no NodeResult).
/// Total = partitions planned (streaming: observed); Completed = partitions whose rows are in
/// the main staging table when the node finished; Reused = done ids pre-populated by pz retry's
/// cross-run partial copy; Resumed = checkpoint resumes engaged on the successful attempt.
/// Counts only — never partition ids.</summary>
public sealed record PartitionStats(long Total, long Completed, long Reused, long Resumed);

/// <summary>Per-op raw counts of the change window a cdc SourceLoad
/// landed into <c>&lt;staging&gt;__changes</c> before the last-event-per-key collapse — never net
/// counts. Attached to a successful cdc SourceLoad's result only; null for every non-cdc dataset.</summary>
public sealed record CdcStats(long Inserts, long Updates, long Deletes);

/// <summary>Honest-abort + delivery-resume observability for SinkWrite
/// nodes. Attached to a FAILED universal-tier sink result when the sink's declared
/// <see cref="Pz.Connectors.Abstractions.AbortSemantics"/> is not DiscardsAll ("delivery
/// stopped; up to RowsVisible rows already visible downstream"), and to a SUCCESSFUL one when
/// a checkpoint resume was accepted (ResumedRows &gt; 0). Null everywhere else — DiscardsAll
/// failures and scratch-delivery successes keep byte-identical artifacts.
/// <see cref="AbortSemantics"/> is the snake_case wire word (discards_all/best_effort/none);
/// counts only, never URLs/paths/keys.</summary>
public sealed record DeliveryStats(string AbortSemantics, long RowsVisible, long ResumedRows);

/// <summary><paramref name="WatermarkCandidate"/>: for a
/// successful SourceLoad on an incremental dataset, the new watermark <see cref="SourceLoadExecutor"/>
/// captured from the staging table it just landed — null for every non-incremental dataset, and null
/// for an incremental one whose extract was an empty delta (nothing to advance to). Engine-internal
/// only, never serialized into run_results.json — see <see cref="Pz.Engine.State.WatermarkAdvancement"/>,
/// which is the only reader.
///
/// <paramref name="SyncStateCandidate"/>: the sync analog of
/// <paramref name="WatermarkCandidate"/> for a successful SourceLoad on a `sync:` dataset — the opaque
/// delta-link/change token the partition emitted, or null when the dataset is not `sync:`-typed or
/// emitted none. Engine-internal only, read solely by <see cref="Pz.Engine.State.SyncStateAdvancement"/>.
///
/// <paramref name="Ops"/>: the gate's <see cref="OpStats"/> snapshot for
/// a gate-aware connector's universal-tier SUCCESS, or null when the connector isn't gate-aware, the
/// tier was native (native scan/copy never routes through a .NET <see cref="OperationGate"/>), or the
/// node failed. Unlike <paramref name="WatermarkCandidate"/>/<paramref name="SyncStateCandidate"/>, this
/// one IS serialized — into <c>run_results.json</c>'s <c>ops:</c> field and the NDJSON
/// <c>NodeCompletedEvent</c>.
///
/// <paramref name="Partitions"/>: see <see cref="PartitionStats"/>.
/// Null unless partition mode ran this node.
///
/// <paramref name="Delivery"/>: see <see cref="DeliveryStats"/>.
///
/// <paramref name="Observed"/>: the DESCRIBE'd staging
/// schema + read-hints hash <see cref="SchemaDriftGate"/> captured for a contract-less SourceLoad under
/// <c>on_source_drift: warn|fail</c> — null for `ignore` (the default; the gate never runs the DESCRIBE),
/// a contract dataset (<c>columns:</c> governs the read instead), or any non-SourceLoad node. Engine-owned
/// like <paramref name="Ops"/> — IS serialized, into <c>run_results.json</c>'s <c>observed_schema:</c>
/// field.</summary>
public sealed record NodeResult(NodeId Id, NodeKind Kind, string Name, NodeStatus Status,
    long RowsMoved, TimeSpan Duration, PzError? Error, NodeTimings? Timings = null,
    Watermark? WatermarkCandidate = null, NodeProvenance? Provenance = null,
    SyncState? SyncStateCandidate = null, OpStats? Ops = null, PartitionStats? Partitions = null,
    DeliveryStats? Delivery = null, CdcStats? Cdc = null, ObservedSchema? Observed = null)
{
    public static NodeResult Skipped(DagNode node) =>
        new(node.Id, node.Kind, node.Name, NodeStatus.Skipped, 0, TimeSpan.Zero, null);
}

/// <summary>One SourceLoad's DESCRIBE'd staging schema plus the
/// read-hints hash it was captured under (guards a config-change reseed — see
/// <see cref="Pz.Engine.State.SchemaBaselineStore"/>). Mirrors <see cref="Pz.Engine.State.SchemaBaseline"/>
/// minus the run id (which the writer/baseline store supply themselves).</summary>
public sealed record ObservedSchema(IReadOnlyList<Pz.Engine.State.SchemaColumn> Columns, string HintsHash);
