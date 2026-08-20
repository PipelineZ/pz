using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>The default <see cref="IRunArtifactStore"/> — `.pz/runs/&lt;id&gt;/run_results.json`. Every
/// method delegates to the existing writer/reader/scanner so the crash-safe write discipline, the
/// skip-the-unreadable scan, and the retention candidate list stay in one place each.
///
/// One <see cref="RunResultsWriter"/> is cached per run id: the writer owns a private publish lock that
/// serializes concurrent snapshots (RunResultsWriter.cs's class doc), and a fresh writer per call would
/// hand out a fresh lock and lose that guarantee. It is constructed with whichever
/// <c>startedAtIso</c> its run's FIRST <see cref="WriteSnapshot"/> call is given — later calls for the
/// same run id reuse it and ignore any different value they are (incorrectly) passed, matching
/// <see cref="RunResultsWriter"/>'s existing one-value-per-run contract.</summary>
public sealed class LocalRunArtifactStore(string projectDir) : IRunArtifactStore
{
    private readonly Dictionary<string, RunResultsWriter> _writers = new(StringComparer.Ordinal);
    private readonly Lock _writersLock = new();

    public void WriteSnapshot(string runId, string startedAtIso, IReadOnlyList<NodeResult> completed, string status,
        long? eventsDropped = null)
    {
        // eventsDropped: not applicable -- run_results.json has no events_dropped column (the local
        // backend has no persisted event stream at all).
        RunResultsWriter writer;
        lock (_writersLock)
        {
            if (!_writers.TryGetValue(runId, out writer!))
            {
                writer = new RunResultsWriter(new RunPaths(projectDir, runId), startedAtIso);
                _writers[runId] = writer;
            }
        }

        writer.WriteSnapshot(completed, status);
    }

    public PriorRun? ReadLatest() => RunResultsReader.ReadLatest(projectDir);

    public IEnumerable<PriorRun> ReadAllNewestFirst() => RunResultsReader.ReadAllNewestFirst(projectDir);

    public IReadOnlyList<RunCandidate> ListCandidates() => RunSweeper.Scan(projectDir);

    public void Delete(string runId)
    {
        var dir = new RunPaths(projectDir, runId).RunDir;
        if (Directory.Exists(dir))
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
