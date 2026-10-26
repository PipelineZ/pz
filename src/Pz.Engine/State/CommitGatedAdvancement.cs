using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.State;

/// <summary>The single commit-gated advancement walk behind <see cref="WatermarkAdvancement"/>
/// and <see cref="SyncStateAdvancement"/>.
///
/// Commit-gated advancement: the stored state advances iff
/// every SinkWrite node in the STRUCTURAL closure of that SourceLoad's <paramref name="dag"/> descendants
/// is BOTH present in <paramref name="nodeResults"/> AND succeeded. This closure is structural (the real
/// project DAG), not the run's effective/selected set: a sink a partial `--select` omitted is still a
/// structural descendant, just with no result at all this run -- so it is treated as un-committed and
/// BLOCKS advancement, ensuring the next full run re-extracts and feeds the omitted sink rather than
/// silently and permanently missing the delta it never received. A dataset with NO structural SinkWrite
/// descendant at all (drained by nothing) advances on source success alone -- forall over an empty set is
/// vacuously true, so this is not a separate case. A null candidate (empty delta, or a non-incremental
/// dataset) always leaves the previously stored state untouched, regardless of sink outcomes.
///
/// Called from <c>RunCommand.ExecuteRun</c> (shared by `pz run`/`pz retry`/`pz test`) at the end of a
/// run, immediately before the terminal run_results.json status write (see
/// <see cref="WatermarkAdvancement"/> for why that ordering is load-bearing) — state is not part of that
/// artifact's schema and must never block or be blocked by it.
///
/// Carry-forward soundness (advancement-time provenance gate): a carried-forward SinkWrite
/// (<see cref="NodeProvenance.CarriedForward"/>) was NOT executed this run — its recorded success only
/// vouches for the PRIOR slice the failed run already committed. The planner seeds it only when every
/// SourceLoad ancestor WILL be reused, but <see cref="Execution.SourceLoadExecutor"/> can fall back to
/// re-extraction at execution time (attach failure, missing table, row-count mismatch), producing a
/// normally-executed result (<see cref="NodeResult.Provenance"/> null, not <see cref="NodeProvenance.Reused"/>)
/// that may capture a NEWER, higher candidate than the carried sink ever committed. So: for a
/// candidate-bearing SourceLoad whose own provenance is NOT Reused, if ANY of its structural SinkWrite
/// descendants recorded a carried-forward result, advancement is skipped for that dataset — the carried
/// sink never received this run's (re-extracted) slice, so advancing would let it permanently miss the
/// delta. The check is per-source over the same descendant walk: a run with reused-A and fallen-back-B
/// blocks only B's dataset.
///
/// Each dataset's write is its own event. By the time this runs every sink has committed, so a store
/// failure on one dataset must cost that dataset its state and nothing else: the walk goes on, and every
/// dataset that did not advance comes back as an <see cref="AdvancementFailure"/> for the caller to
/// report. An unreachable store (PZ0518) is tried <see cref="MaxAttempts"/> times first — giving up on
/// it means re-extracting next run for the sake of one dropped connection. Nothing else is retried: a
/// lost race (PZ0520) means another run already advanced the dataset, and trying again cannot win it.
/// Cancellation is never a dataset failure; it propagates.</summary>
internal static class CommitGatedAdvancement
{
    private const int MaxAttempts = 3;

    private static readonly TimeSpan[] RetryWaits = [TimeSpan.FromMilliseconds(250), TimeSpan.FromSeconds(1)];

    public static IReadOnlyList<AdvancementFailure> Advance<T>(
        CompiledDag dag,
        IReadOnlyList<NodeResult> nodeResults,
        Func<NodeResult, T?> candidateOf,
        Func<SourceDatasetDef, string> keyOf,
        Action<string, T> apply,
        Action<TimeSpan>? wait = null) where T : class
    {
        wait ??= Thread.Sleep;
        var failures = new List<AdvancementFailure>();
        var dagById = dag.Nodes.ToDictionary(n => n.Id);
        var resultById = nodeResults.ToDictionary(r => r.Id);

        foreach (var result in nodeResults)
        {
            if (result.Kind != NodeKind.SourceLoad || result.Status != NodeStatus.Success ||
                candidateOf(result) is not { } candidate)
            {
                continue;
            }

            if (!dagById.TryGetValue(result.Id, out var node) || node.Definition is not SourceDatasetDef def)
            {
                continue;
            }

            var descendantSinks = dag.Descendants(result.Id)
                .Where(d => d.Kind == NodeKind.SinkWrite)
                .ToList();

            // Carry-forward soundness gate (see class doc): a carried-forward descendant sink only
            // vouches for the prior slice. If this SourceLoad did not actually reuse (it fell back to
            // re-extraction, so Provenance != Reused), the slice it just landed may differ from the one
            // that sink committed -- so its candidate must not advance the stored state.
            if (result.Provenance != NodeProvenance.Reused &&
                descendantSinks.Any(d =>
                    resultById.TryGetValue(d.Id, out var r) && r.Provenance == NodeProvenance.CarriedForward))
            {
                continue;
            }

            var allSinksCommitted = descendantSinks
                .All(d => resultById.TryGetValue(d.Id, out var r) && r.Status == NodeStatus.Success);

            if (!allSinksCommitted)
            {
                continue;
            }

            var key = keyOf(def);
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    apply(key, candidate);
                    break;
                }
                catch (PzConfigException ex) when (
                    ex.Error.Code == PzErrorCode.StateStoreUnavailable && attempt < MaxAttempts)
                {
                    wait(RetryWaits[attempt - 1]);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    failures.Add(new AdvancementFailure(key, MessageRedaction.Redact(ex)));
                    break;
                }
            }
        }

        return failures;
    }
}

/// <summary>One dataset whose state did not advance although its sinks committed: the next run
/// re-extracts it from the previous value. <see cref="Reason"/> is already redacted.</summary>
public sealed record AdvancementFailure(string Key, string Reason);
