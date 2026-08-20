using Pz.Core.Dag;
using Pz.Diagnostics.Events;
using Pz.Diagnostics.Otel;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Events;

/// <summary>Bridges the engine's <see cref="IRunEvents"/> callback seam onto a <see cref="RunEventBus"/>,
/// mapping engine types (<see cref="DagNode"/>/<see cref="NodeResult"/>) down to the primitive-only
/// <see cref="RunEvent"/> records (Pz.Diagnostics stays BCL-only) and stamping each one via
/// an injected <see cref="TimeProvider"/> (default <see cref="TimeProvider.System"/> at composition
/// sites; tests inject a fixed clock for determinism). The node-level <see cref="IRunEvents"/> members
/// (<see cref="NodeStarted"/>, <see cref="NodeProgress"/>, <see cref="RetryScheduled"/>,
/// <see cref="NodeCompleted"/>) don't carry a runId argument of their own, so this publisher stamps
/// them with the runId it was constructed for; <see cref="RunStarted"/>/<see cref="RunCompleted"/>
/// already receive their own runId argument (in practice always equal to the constructor's) and use
/// that one directly.</summary>
public sealed class RunEventPublisher(RunEventBus bus, string runId, TimeProvider clock) : IRunEvents
{
    public void RunStarted(string startedRunId, string projectName, int nodeCount) =>
        bus.Publish(new RunStartedEvent(clock.GetUtcNow(), startedRunId, projectName, nodeCount));

    public void NodeStarted(DagNode node) =>
        bus.Publish(new NodeStartedEvent(clock.GetUtcNow(), runId, node.Id.Value, node.Kind.ToString(), node.Name));

    public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) =>
        bus.Publish(new NodeProgressEvent(clock.GetUtcNow(), runId, node.Id.Value, node.Name, rowsSoFar,
            bytesSoFar, batchesSoFar));

    public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) =>
        bus.Publish(new RetryScheduledEvent(clock.GetUtcNow(), runId, node.Id.Value, node.Name, attempt,
            maxAttempts, (long)delay.TotalMilliseconds, reason));

    public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown) =>
        bus.Publish(new BreakerStateChangedEvent(clock.GetUtcNow(), runId, instance, oldState, newState, trigger,
            (long)coolDown.TotalMilliseconds));

    /// <summary>Maps the engine's <see cref="SchemaDriftDiffer.Change"/>/
    /// <see cref="SchemaColumn"/> onto the BCL-only <see cref="DriftChangePayload"/>/<see cref="SchemaColumnPayload"/>
    /// (Pz.Diagnostics stays BCL-only), same discipline as <see cref="ToOpsPayload"/>.</summary>
    public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
        IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash) =>
        bus.Publish(new SourceDriftDetectedEvent(clock.GetUtcNow(), runId, node.Id.Value, connection, entity,
            policy, [.. changes.Select(c => new DriftChangePayload(c.Kind, c.Column, c.From, c.To))],
            [.. observed.Select(c => new SchemaColumnPayload(c.Name, c.Type))], hintsHash));

    /// <summary>Key column names and counts only — never row values,
    /// following <see cref="SourceDriftDetected"/>'s no-connection-config discipline.</summary>
    public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
        long duplicateGroups, long extraRows) =>
        bus.Publish(new MergeKeyDuplicatesDetectedEvent(clock.GetUtcNow(), runId, node.Id.Value, output,
            [.. keys], duplicateGroups, extraRows));

    /// <summary>Column names only — never row values, following
    /// <see cref="MergeKeyDuplicatesDetected"/>'s discipline.</summary>
    public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns) =>
        bus.Publish(new LossyIntegerInferenceDetectedEvent(clock.GetUtcNow(), runId, node.Id.Value,
            connection, entity, [.. columns]));

    /// <summary>Column names and the picked format only — never row values, following
    /// <see cref="LossyIntegerInferenceDetected"/>'s discipline.</summary>
    public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns, string format) =>
        bus.Publish(new AmbiguousDateInferenceDetectedEvent(clock.GetUtcNow(), runId, node.Id.Value,
            connection, entity, [.. columns], format));

    public void NodeCompleted(NodeResult result) =>
        bus.Publish(new NodeCompletedEvent(clock.GetUtcNow(), runId, result.Id.Value, result.Kind.ToString(),
            result.Name, NodeStatusName(result.Status), result.RowsMoved,
            (long)result.Duration.TotalMilliseconds, result.Error?.Code, result.Error?.Message,
            ToPayload(result.Timings), ProvenanceName(result.Provenance), ToOpsPayload(result.Ops),
            ToPartitionsPayload(result.Partitions), ToDeliveryPayload(result.Delivery),
            ToCdcPayload(result.Cdc, result.SyncStateCandidate)));

    private static NodeTimingsPayload? ToPayload(NodeTimings? timings) => timings is null
        ? null
        : new NodeTimingsPayload((long)timings.ProducerStall.TotalMilliseconds, (long)timings.ConsumerStall.TotalMilliseconds);

    /// <summary>Maps the engine's <see cref="Resilience.OpStats"/> onto the
    /// BCL-only <see cref="OpStatsPayload"/> (Pz.Diagnostics cannot reference Pz.Engine),
    /// exactly like <see cref="ToPayload"/> mirrors <see cref="NodeTimings"/>.</summary>
    private static OpStatsPayload? ToOpsPayload(Resilience.OpStats? ops) => ops is null
        ? null
        : new OpStatsPayload(ops.Executed, ops.Retried, ops.ThrottleWaitMs);

    /// <summary>Maps the engine's <see cref="PartitionStats"/> onto the BCL-only
    /// <see cref="PartitionStatsPayload"/> (Pz.Diagnostics cannot reference Pz.Engine),
    /// exactly like <see cref="ToOpsPayload"/> mirrors <see cref="Resilience.OpStats"/>.</summary>
    private static PartitionStatsPayload? ToPartitionsPayload(PartitionStats? stats) => stats is null
        ? null
        : new PartitionStatsPayload(stats.Total, stats.Completed, stats.Reused, stats.Resumed);

    /// <summary>Maps the engine's <see cref="DeliveryStats"/> onto the
    /// BCL-only <see cref="DeliveryPayload"/> — same discipline as <see cref="ToPartitionsPayload"/>.</summary>
    private static DeliveryPayload? ToDeliveryPayload(DeliveryStats? delivery) => delivery is null
        ? null
        : new DeliveryPayload(delivery.AbortSemantics, delivery.RowsVisible, delivery.ResumedRows);

    /// <summary>Maps the engine's <see cref="CdcStats"/> onto the
    /// BCL-only <see cref="CdcPayload"/> — same discipline as <see cref="ToDeliveryPayload"/>.
    /// <paramref name="syncState"/> is <see cref="NodeResult.SyncStateCandidate"/> (cdc reuses the
    /// sync-state seam for its candidate token) — its <see cref="SyncState.Token"/> becomes
    /// <c>position</c>, <c>null</c> when the connector emitted none this window.</summary>
    private static CdcPayload? ToCdcPayload(CdcStats? cdc, SyncState? syncState) => cdc is null
        ? null
        : new CdcPayload(cdc.Inserts, cdc.Updates, cdc.Deletes, syncState?.Token);

    public void RunCompleted(string completedRunId, RunStatus status, int succeeded, int failed, int skipped,
        TimeSpan duration)
    {
        var statusName = RunStatusName(status);
        PzMeters.RunsCompleted.Add(1, new KeyValuePair<string, object?>("pz.run.status", statusName));
        bus.Publish(new RunCompletedEvent(clock.GetUtcNow(), completedRunId, statusName, succeeded,
            failed, skipped, (long)duration.TotalMilliseconds));
    }

    /// <summary>Maps <see cref="NodeResult.Provenance"/> onto the exact wire values
    /// <c>RunResultsWriter</c> writes to <c>run_results.json</c>, so the NDJSON stream and the persisted
    /// artifact agree.</summary>
    private static string? ProvenanceName(NodeProvenance? provenance) => provenance switch
    {
        null => null,
        NodeProvenance.Reused => "reused",
        NodeProvenance.CarriedForward => "carried_forward",
        _ => throw new ArgumentOutOfRangeException(nameof(provenance), provenance, "unknown provenance"),
    };

    private static string NodeStatusName(NodeStatus status) => status switch
    {
        NodeStatus.Success => "success",
        NodeStatus.Failed => "failed",
        NodeStatus.Skipped => "skipped",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };

    private static string RunStatusName(RunStatus status) => status switch
    {
        RunStatus.Success => "success",
        RunStatus.CompletedWithFailures => "completed_with_failures",
        RunStatus.Fatal => "fatal",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown run status"),
    };
}
