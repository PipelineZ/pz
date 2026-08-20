namespace Pz.Connectors.Abstractions;

/// <summary>Optional capability an <see cref="IDatasetPartition"/> implements to hand the engine an
/// opaque sync-state token AFTER its <see cref="IDatasetPartition.ReadAsync"/> enumeration completes
/// successfully. The engine polls this once, post-enumeration, and (commit-gated) persists the token
/// for the next run's <see cref="DatasetSpec.PriorSyncState"/>. Return false (candidate null) when the
/// partition produced no new token this run — the stored token is then left unchanged. This is the
/// connector→engine state channel; ordered-cursor watermarks never needed one (the engine computes
/// them from landed rows).</summary>
public interface ISyncStatePartition
{
    bool TryGetSyncStateCandidate(out string? candidate);
}
