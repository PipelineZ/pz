using Pz.Core.Dag;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Best-effort observation seam over one run: a run fires <see cref="RunStarted"/> once, then
/// per node
/// <see cref="NodeStarted"/> → <see cref="NodeProgress"/>* → [<see cref="RetryScheduled"/>*] →
/// <see cref="NodeCompleted"/>, then <see cref="RunCompleted"/> once. <see cref="RunOrchestrator"/> is
/// the sole publisher of <see cref="NodeStarted"/>/<see cref="NodeCompleted"/>/<see cref="RunStarted"/>/
/// <see cref="RunCompleted"/>; executors publish <see cref="NodeProgress"/> and
/// <see cref="RetryScheduled"/>. <see cref="BreakerStateChanged"/> stands outside
/// that per-node sequence entirely — <c>BreakerRegistry</c> publishes it whenever a source/sink
/// instance's circuit breaker transitions, independent of any one node's own lifecycle. None of these
/// calls may throw out into dispatcher control flow — every dispatch site uses the corresponding
/// <c>Safe*</c> wrapper below.</summary>
public interface IRunEvents
{
    void RunStarted(string runId, string projectName, int nodeCount);
    void NodeStarted(DagNode node);
    void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar);
    void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason);
    void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown);

    /// <summary>Fired by the SourceLoad drift gate under warn AND
    /// fail, before that node's own <see cref="NodeCompleted"/> — so an event consumer can implement
    /// "accept" purely from this event.</summary>
    void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
        IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash);

    /// <summary>Fired by <see cref="SinkWriteExecutor"/> when a merge
    /// output's staged input holds duplicate merge-key groups, before that node's own
    /// <see cref="NodeCompleted"/> — the in-batch collapse is connector-determined physical order, not
    /// cursor order, so the caller is warned that "which duplicate wins" is not what they may assume.</summary>
    void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
        long duplicateGroups, long extraRows);

    /// <summary>Fired by
    /// <see cref="IntegerInferenceLint"/> after a schema-inferred native scan stages a DOUBLE column
    /// whose values are all integers with at least one beyond 2^53 — auto-detect's shape for a
    /// &gt;int64 integer column, where digits may already have been lost. Before that node's own
    /// <see cref="NodeCompleted"/>; column names only, never row values.</summary>
    void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns);

    /// <summary>Fired by
    /// <see cref="AmbiguousDateLint"/> when a schema-inferred csv read's sniffed date/timestamp
    /// format is a day-first/month-first family and no staged value's day exceeds 12 — the sniffer's
    /// field-order pick was an unverified guess, so a month-first source is misread on every row.
    /// Before that node's own <see cref="NodeCompleted"/>; column names only, never row values.</summary>
    void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns, string format);

    void NodeCompleted(NodeResult result);
    void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration);
}

public sealed class NullRunEvents : IRunEvents
{
    public static readonly NullRunEvents Instance = new();
    public void RunStarted(string runId, string projectName, int nodeCount) { }
    public void NodeStarted(DagNode node) { }
    public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
    public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
    public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown) { }
    public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
        IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash) { }
    public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
        long duplicateGroups, long extraRows) { }
    public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns) { }
    public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns, string format) { }
    public void NodeCompleted(NodeResult result) { }
    public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
}

/// <summary>IRunEvents is a best-effort observation contract — a throwing
/// subscriber must never be able to break dispatcher control flow (e.g. skip the Skip-cascade that
/// follows a failed node's completion). Every dispatch site swallows and continues.</summary>
public static class RunEventsExtensions
{
    public static void SafeRunStarted(this IRunEvents events, string runId, string projectName, int nodeCount)
    {
        try { events.RunStarted(runId, projectName, nodeCount); } catch { /* best-effort observation only */ }
    }

    public static void SafeNodeStarted(this IRunEvents events, DagNode node)
    {
        try { events.NodeStarted(node); } catch { /* best-effort observation only */ }
    }

    public static void SafeNodeProgress(this IRunEvents events, DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar)
    {
        try { events.NodeProgress(node, rowsSoFar, bytesSoFar, batchesSoFar); } catch { /* best-effort observation only */ }
    }

    public static void SafeRetryScheduled(this IRunEvents events, DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason)
    {
        try { events.RetryScheduled(node, attempt, maxAttempts, delay, reason); } catch { /* best-effort observation only */ }
    }

    public static void SafeBreakerStateChanged(this IRunEvents events, string instance, string oldState, string newState, string trigger, TimeSpan coolDown)
    {
        try { events.BreakerStateChanged(instance, oldState, newState, trigger, coolDown); } catch { /* best-effort observation only */ }
    }

    public static void SafeSourceDriftDetected(this IRunEvents events, DagNode node, string connection,
        string entity, string policy, IReadOnlyList<SchemaDriftDiffer.Change> changes,
        IReadOnlyList<SchemaColumn> observed, string hintsHash)
    {
        try { events.SourceDriftDetected(node, connection, entity, policy, changes, observed, hintsHash); }
        catch { /* best-effort observation only */ }
    }

    public static void SafeMergeKeyDuplicatesDetected(this IRunEvents events, DagNode node, string output,
        IReadOnlyList<string> keys, long duplicateGroups, long extraRows)
    {
        try { events.MergeKeyDuplicatesDetected(node, output, keys, duplicateGroups, extraRows); }
        catch { /* best-effort observation only */ }
    }

    public static void SafeLossyIntegerInferenceDetected(this IRunEvents events, DagNode node,
        string connection, string entity, IReadOnlyList<string> columns)
    {
        try { events.LossyIntegerInferenceDetected(node, connection, entity, columns); }
        catch { /* best-effort observation only */ }
    }

    public static void SafeAmbiguousDateInferenceDetected(this IRunEvents events, DagNode node,
        string connection, string entity, IReadOnlyList<string> columns, string format)
    {
        try { events.AmbiguousDateInferenceDetected(node, connection, entity, columns, format); }
        catch { /* best-effort observation only */ }
    }

    public static void SafeNodeCompleted(this IRunEvents events, NodeResult result)
    {
        try { events.NodeCompleted(result); } catch { /* best-effort observation only */ }
    }

    public static void SafeRunCompleted(this IRunEvents events, string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration)
    {
        try { events.RunCompleted(runId, status, succeeded, failed, skipped, duration); } catch { /* best-effort observation only */ }
    }
}
