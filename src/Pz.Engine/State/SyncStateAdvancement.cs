using Pz.Core.Dag;
using Pz.Engine.Execution;

namespace Pz.Engine.State;

/// <summary>Applies one run's sync-state candidates to the sync-state store, under the
/// commit-gated + carried-forward-provenance rules of <see cref="CommitGatedAdvancement"/>.
/// For every SourceLoad that succeeded on a `sync:` dataset and produced a candidate
/// (<see cref="NodeResult.SyncStateCandidate"/> non-null), the stored token advances iff every
/// SinkWrite in the STRUCTURAL closure of that SourceLoad's descendants is present in
/// <paramref name="nodeResults"/> and succeeded. Called immediately alongside
/// WatermarkAdvancement.Advance from RunCommand, after every node result is durably recorded and just
/// before the terminal run_results.json status write — see <see cref="WatermarkAdvancement"/> for why
/// that ordering is load-bearing.</summary>
public static class SyncStateAdvancement
{
    public static void Advance(CompiledDag dag, IReadOnlyList<NodeResult> nodeResults, SyncStateStore store) =>
        CommitGatedAdvancement.Advance(dag, nodeResults,
            static r => r.SyncStateCandidate,
            (def, candidate) => store.Set(SyncStateStore.Key(def.Source.Name, def.Dataset.Name), candidate));
}
