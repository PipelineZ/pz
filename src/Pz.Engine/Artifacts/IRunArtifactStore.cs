using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>The run-artifact seam. Covers everything three
/// consumers need from a run's persisted results — `pz retry`'s selection, `pz state show`'s history,
/// and retention's sweep.
///
/// <see cref="ReadAllNewestFirst"/> stays lazily enumerated: <see cref="ReadLatest"/> must keep costing
/// one run's worth of reading, never a full scan, which is what `pz retry` has always relied on.</summary>
public interface IRunArtifactStore
{
    /// <summary>Persists the run's results so far. Called after EVERY node completion with status
    /// "running", then once with the terminal status. Must be safe to call concurrently and must never
    /// leave a partially-observable snapshot — whichever call lands last is the one that survives.
    /// <paramref name="startedAtIso"/> is the run's actual start time: it is
    /// supplied by the caller on every call so the FIRST snapshot for a given <paramref name="runId"/>
    /// fixes it, exactly as <see cref="RunResultsWriter"/> requires at construction — no
    /// implementation may synthesize it from a clock at snapshot time.
    ///
    /// <paramref name="eventsDropped"/>: the run's persisted-event drop count, once known. Null
    /// on every call that does not know it yet -- every per-node snapshot, and even the terminal snapshot
    /// taken before the event sink has been drained/disposed. An implementation that tracks this column
    /// must leave a prior value alone when null rather than resetting it to zero -- the composition site
    /// makes exactly one call with a real count, after disposing the event sink, and it must win
    /// regardless of how many null-valued calls preceded it. <see cref="LocalRunArtifactStore"/> ignores
    /// the parameter entirely: the local backend has no persisted event stream to count drops for.</summary>
    void WriteSnapshot(string runId, string startedAtIso, IReadOnlyList<NodeResult> completed, string status,
        long? eventsDropped = null);

    PriorRun? ReadLatest();

    IEnumerable<PriorRun> ReadAllNewestFirst();

    IReadOnlyList<RunCandidate> ListCandidates();

    /// <summary>Idempotent: deleting an absent run is a no-op, never an error.</summary>
    void Delete(string runId);
}
