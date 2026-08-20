using Pz.Core.Dag;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Trivial fan-out over one or more <see cref="IRunEvents"/> targets:
/// lets a caller register e.g. <c>[SnapshotRunEvents, RunEventPublisher]</c> so the crash-safe
/// run_results.json snapshot path and the bus-backed renderer path both observe the same run without
/// either depending on the other. Every dispatch goes through the corresponding <c>Safe*</c> wrapper
/// per target, so one target throwing never prevents a later target in the list from observing the
/// same event.</summary>
public sealed class CompositeRunEvents(params IRunEvents[] targets) : IRunEvents
{
    public void RunStarted(string runId, string projectName, int nodeCount)
    {
        foreach (var target in targets)
        {
            target.SafeRunStarted(runId, projectName, nodeCount);
        }
    }

    public void NodeStarted(DagNode node)
    {
        foreach (var target in targets)
        {
            target.SafeNodeStarted(node);
        }
    }

    public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar)
    {
        foreach (var target in targets)
        {
            target.SafeNodeProgress(node, rowsSoFar, bytesSoFar, batchesSoFar);
        }
    }

    public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason)
    {
        foreach (var target in targets)
        {
            target.SafeRetryScheduled(node, attempt, maxAttempts, delay, reason);
        }
    }

    public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown)
    {
        foreach (var target in targets)
        {
            target.SafeBreakerStateChanged(instance, oldState, newState, trigger, coolDown);
        }
    }

    public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
        IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash)
    {
        foreach (var target in targets)
        {
            target.SafeSourceDriftDetected(node, connection, entity, policy, changes, observed, hintsHash);
        }
    }

    public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
        long duplicateGroups, long extraRows)
    {
        foreach (var target in targets)
        {
            target.SafeMergeKeyDuplicatesDetected(node, output, keys, duplicateGroups, extraRows);
        }
    }

    public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns)
    {
        foreach (var target in targets)
        {
            target.SafeLossyIntegerInferenceDetected(node, connection, entity, columns);
        }
    }

    public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns, string format)
    {
        foreach (var target in targets)
        {
            target.SafeAmbiguousDateInferenceDetected(node, connection, entity, columns, format);
        }
    }

    public void NodeCompleted(NodeResult result)
    {
        foreach (var target in targets)
        {
            target.SafeNodeCompleted(result);
        }
    }

    public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration)
    {
        foreach (var target in targets)
        {
            target.SafeRunCompleted(runId, status, succeeded, failed, skipped, duration);
        }
    }
}
