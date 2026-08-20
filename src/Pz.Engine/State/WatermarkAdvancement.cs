using Pz.Core.Dag;
using Pz.Engine.Execution;

namespace Pz.Engine.State;

/// <summary>Applies one run's results to the watermark store under commit-gated advancement rules.
/// For every SourceLoad node that succeeded on an incremental dataset and produced a
/// candidate (<see cref="NodeResult.WatermarkCandidate"/> non-null), the stored watermark advances iff
/// every SinkWrite node in the STRUCTURAL closure of that SourceLoad's descendants is both present
/// in <paramref name="nodeResults"/> and succeeded. See <see cref="CommitGatedAdvancement"/> for
/// the full mechanism (structural closure rule, carried-forward provenance gate, vacuous-truth note).
///
/// Called from <c>RunCommand.ExecuteRun</c> (shared by `pz run`/`pz retry`/`pz test`) at the very end of
/// a run, after every node result is already durably recorded and immediately BEFORE the terminal
/// run_results.json status write. That ordering is load-bearing: a process death between the two must
/// not leave an artifact claiming "success" for a run whose watermarks never advanced, which `pz retry`
/// would then refuse as already-succeeded. Watermark state must never block or be blocked by the
/// run_results artifact either; the caller's try/catch supplies that (a persist failure is advisory,
/// never a status change).</summary>
public static class WatermarkAdvancement
{
    public static void Advance(CompiledDag dag, IReadOnlyList<NodeResult> nodeResults, WatermarkStore store) =>
        CommitGatedAdvancement.Advance(dag, nodeResults,
            static r => r.WatermarkCandidate,
            (def, candidate) => store.Set(WatermarkStore.Key(def.Source.Name, def.Dataset.Name), candidate));
}
