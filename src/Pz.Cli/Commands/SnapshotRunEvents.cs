using Pz.Core.Dag;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Cli.Commands;

/// <summary>The crash-safe incremental run-artifact snapshot path: a first-class
/// <see cref="IRunEvents"/> target registered directly on <see cref="CompositeRunEvents"/> alongside
/// <c>RunEventPublisher</c> — NEVER only a bus subscriber, so the persisted snapshot's integrity never
/// depends on the bus/renderer machinery being alive or healthy.
///
/// Console/NDJSON line printing belongs to the bus renderers
/// (<c>ConsoleRenderer</c>/<c>JsonRenderer</c>/<c>LiveTreeRenderer</c>) — this class touches
/// <see cref="Console"/> only for the write-failure warning, never for progress output.
///
/// Writes go through <see cref="IRunArtifactStore"/> (Local or SQL, whichever <c>StateBackendFactory</c>
/// resolved); <paramref name="runId"/>/<paramref name="startedAtIso"/> live here rather than in the
/// store because that interface takes them per call, so one store instance can serve every run (unlike
/// <see cref="RunResultsWriter"/>).
///
/// A snapshot write failure (e.g. disk full, or an unreachable SQL store) must
/// not silently disable crash-safety, but it also must not kill the run — <see cref="TryWriteSnapshot"/>
/// swallows the write failure and prints a one-time latched warning to stderr instead.
/// <c>RunCommand.ExecuteRun</c> also calls <see cref="TryWriteSnapshot"/> directly for the final
/// terminal-status snapshot, sharing the same latch so a persistently broken writer only warns once
/// across the whole run. Internal (rather than private-nested) and visible to Pz.Cli.Tests so the failure
/// path can be exercised directly without depending on the random per-run directory name.</summary>
internal sealed class SnapshotRunEvents(IRunArtifactStore artifacts, string runId, string startedAtIso) : IRunEvents
{
    private readonly List<NodeResult> _completed = [];
    private readonly Lock _gate = new();
    private bool _snapshotWarningPrinted;

    public void RunStarted(string runId, string projectName, int nodeCount) { }

    public void NodeStarted(DagNode node) { }

    public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }

    public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }

    // No-op — run_results.json's schema is untouched by breaker state; the event only ever reaches the
    // bus-backed renderer path via RunEventPublisher.
    public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown) { }

    // No-op — same reasoning as BreakerStateChanged above: run_results.json's schema is untouched by
    // drift detection; the event only ever reaches the bus-backed renderer path via RunEventPublisher.
    public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
        IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
        IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }

    // No-op — same reasoning as SourceDriftDetected above: run_results.json's schema is untouched by
    // the duplicate-key warning; the event only ever reaches the bus-backed renderer path via
    // RunEventPublisher.
    public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
        long duplicateGroups, long extraRows) { }

    // No-op — same reasoning as MergeKeyDuplicatesDetected above.
    public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns) { }

    // No-op — same reasoning as above.
    public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
        IReadOnlyList<string> columns, string format) { }

    public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }

    public void NodeCompleted(NodeResult result)
    {
        List<NodeResult> snapshot;
        lock (_gate)
        {
            _completed.Add(result);
            snapshot = [.. _completed];
        }

        TryWriteSnapshot(snapshot, "running");
    }

    /// <summary>Writes a run-artifact snapshot, swallowing any failure so the run itself is never
    /// affected. On failure, prints a warning to stderr exactly once for the lifetime of this instance
    /// (latched) — repeated failures (e.g. one per remaining node) do not spam stderr again.
    /// <paramref name="eventsDropped"/> is null for every call except the composition site's
    /// one deliberate post-dispose call — see <see cref="IRunArtifactStore.WriteSnapshot"/>.</summary>
    public void TryWriteSnapshot(IReadOnlyList<NodeResult> completed, string status, long? eventsDropped = null)
    {
        try
        {
            artifacts.WriteSnapshot(runId, startedAtIso, completed, status, eventsDropped);
        }
        catch (Exception ex)
        {
            bool shouldWarn;
            lock (_gate)
            {
                shouldWarn = !_snapshotWarningPrinted;
                _snapshotWarningPrinted = true;
            }

            if (shouldWarn)
            {
                Console.Error.WriteLine(
                    $"warning: could not write run_results.json ({ex.Message}) — resume/retry data may be stale");
            }
        }
    }
}
