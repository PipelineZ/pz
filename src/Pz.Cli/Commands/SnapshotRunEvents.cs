using Pz.Core.Dag;
using Pz.Diagnostics.Events;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;
using Pz.State.SqlServer;

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
/// path can be exercised directly without depending on the random per-run directory name.
///
/// Every snapshot rewrites the whole run, so writing one per completion is quadratic in node count and
/// would stall dispatch of the completed node's children. Completions therefore coalesce onto one
/// background writer that persists only the newest state, with two exceptions that write on the
/// calling thread before <see cref="NodeCompleted"/> returns: a successful SinkWrite (a committed sink
/// missing from the snapshot would be re-run by <c>pz retry</c>, duplicating append rows), and every
/// completion after <see cref="Flush"/>. <see cref="TryWriteSnapshot"/> flushes first, so no background
/// write can ever land after a direct one -- in particular after the terminal snapshot.</summary>
internal sealed class SnapshotRunEvents(IRunArtifactStore artifacts, string runId, string startedAtIso)
    : IRunEvents, IDisposable
{
    private readonly List<NodeResult> _completed = [];
    private readonly Lock _gate = new();
    private bool _snapshotWarningPrinted;

    // Serializes every store write, so a snapshot taken inside it is never overtaken by an older one.
    private readonly Lock _writeLock = new();

    // Guarded by _gate: _version counts completions; _closed is set by Flush; _worker starts lazily.
    private long _version;
    private bool _closed;
    private Thread? _worker;

    // Guarded by _writeLock: the _version the last per-node snapshot included.
    private long _writtenVersion;

    // Guarded by _wake (Monitor): set by a completion, consumed by the worker.
    private readonly object _wake = new();
    private bool _wakePending;

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

    // No-op — same reasoning as above: run_results.json's schema carries no per-event log lines; the
    // event only ever reaches the bus-backed renderer path via RunEventPublisher.
    public void ConnectorLog(string connection, string level, string message) { }

    public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }

    public void NodeCompleted(NodeResult result)
    {
        bool writeNow;
        lock (_gate)
        {
            _completed.Add(result);
            _version++;
            writeNow = _closed || result is { Kind: NodeKind.SinkWrite, Status: NodeStatus.Success };
            if (!writeNow && _worker is null)
            {
                // Background so a run that ends without Flush (an exception path) cannot keep the
                // process alive; Dispose flushes on every path that unwinds normally.
                _worker = new Thread(WorkerLoop) { IsBackground = true, Name = "pz-snapshot-writer" };
                _worker.Start();
            }
        }

        if (writeNow)
        {
            WriteLatest();
            return;
        }

        lock (_wake)
        {
            _wakePending = true;
            Monitor.Pulse(_wake);
        }
    }

    /// <summary>Waits for the background writer to persist every completion recorded so far and exit.
    /// From then on every write -- from <see cref="NodeCompleted"/> or <see cref="TryWriteSnapshot"/> --
    /// runs on the caller. Idempotent.</summary>
    internal void Flush()
    {
        Thread? worker;
        lock (_gate)
        {
            _closed = true;
            worker = _worker;
        }

        // Never join from the worker itself (a store calling back into this instance would deadlock).
        if (worker is null || worker == Thread.CurrentThread)
        {
            return;
        }

        lock (_wake)
        {
            _wakePending = true;
            Monitor.Pulse(_wake);
        }

        worker.Join();
    }

    public void Dispose() => Flush();

    private void WorkerLoop()
    {
        while (true)
        {
            lock (_wake)
            {
                while (!_wakePending)
                {
                    Monitor.Wait(_wake);
                }

                _wakePending = false;
            }

            // Read before writing: the write below then covers every completion that preceded the close,
            // and any later one is written by NodeCompleted itself.
            bool closed;
            lock (_gate)
            {
                closed = _closed;
            }

            WriteLatest();
            if (closed)
            {
                return;
            }
        }
    }

    // Snapshots _completed inside the write lock and skips the write when a newer-or-equal state has
    // already been persisted, so the store only ever sees snapshots in completion order.
    private void WriteLatest()
    {
        lock (_writeLock)
        {
            List<NodeResult> snapshot;
            long version;
            lock (_gate)
            {
                version = _version;
                if (version <= _writtenVersion)
                {
                    return;
                }

                snapshot = [.. _completed];
            }

            // A failed write must not count: a successful sink waiting on this lock would otherwise
            // see its version as persisted and return without recording its commit.
            if (WriteSnapshotCore(snapshot, "running", null, null))
            {
                _writtenVersion = version;
            }
        }
    }

    /// <summary>Writes a run-artifact snapshot, swallowing any failure so the run itself is never
    /// affected. On failure, prints a warning to stderr exactly once for the lifetime of this instance
    /// (latched) — repeated failures (e.g. one per remaining node) do not spam stderr again.
    /// <paramref name="eventsDropped"/> is null for every call except the composition site's
    /// one deliberate post-dispose call — see <see cref="IRunArtifactStore.WriteSnapshot"/>. Flushes
    /// first, so a background per-node write can never land after this one.</summary>
    public void TryWriteSnapshot(IReadOnlyList<NodeResult> completed, string status, long? eventsDropped = null,
        RunPhasesPayload? phases = null)
    {
        Flush();
        lock (_writeLock)
        {
            WriteSnapshotCore(completed, status, eventsDropped, phases);
        }
    }

    // Returns false when the store write failed (the failure is swallowed and warned about once).
    private bool WriteSnapshotCore(IReadOnlyList<NodeResult> completed, string status, long? eventsDropped,
        RunPhasesPayload? phases)
    {
        try
        {
            artifacts.WriteSnapshot(runId, startedAtIso, completed, status, eventsDropped, phases);
            return true;
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
                // Named by TYPE, not by StateBackends.Description: `state.artifacts: false` steers
                // artifacts to the local store even under `state: {backend: sqlserver}`, so the backend
                // description alone would misname which store actually failed.
                var target = artifacts switch
                {
                    LocalRunArtifactStore => "run_results.json",
                    SqlRunArtifactStore => "the SQL state store",
                    _ => "the configured run-artifact store",
                };
                Console.Error.WriteLine(
                    $"warning: could not write {target} ({ex.Message}) — resume/retry data may be stale");
            }

            return false;
        }
    }
}
