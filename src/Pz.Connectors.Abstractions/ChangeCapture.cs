using Apache.Arrow;

namespace Pz.Connectors.Abstractions;

/// <summary>Optional surface a cdc partition implements (alongside <see cref="ISyncStatePartition"/>):
/// after ReadAsync completes, reports the dataset's key columns (replica identity / primary key /
/// capture-instance index) so the engine can collapse the change window per key in DuckDB. Return
/// false when unknown (e.g. an empty window with no metadata) — the engine then skips collapse for
/// an empty window or fails the node for a non-empty one.</summary>
public interface IChangeCapturePartition
{
    bool TryGetChangeKeyColumns(out IReadOnlyList<string>? keyColumns);
}

/// <summary>Optional session surface for sinks declaring
/// <see cref="ConnectorCapabilities.ApplyDeletes"/>. After the upsert drain and before Commit, the
/// engine streams delete-key batches (columns = the output's merge keys, one row per deleted key)
/// through this call. Semantics come from <see cref="OutputSpec.OnDelete"/>: "delete" hard-deletes
/// matching keys; "soft" sets a nullable `_pz_deleted_at` timestamp column instead (and clears it
/// for keys re-upserted this session). Applied in the SAME transaction as the merge. Batch
/// ownership rules match <see cref="ISinkWriteSession.WriteBatchAsync"/> (engine-owned, copy what
/// you keep).</summary>
public interface IDeleteApplyingWriteSession : ISinkWriteSession
{
    ValueTask ApplyDeleteKeysAsync(RecordBatch keyBatch, CancellationToken ct);
}

/// <summary>Optional source surface backing `pz cdc status` / `pz cdc drop`: report and tear down
/// server-side change-capture state for one dataset. Never called by the run path.</summary>
public interface IChangeCaptureAdmin
{
    ValueTask<ChangeCaptureStatus> GetChangeCaptureStatusAsync(DatasetSpec spec, CancellationToken ct);
    ValueTask DropChangeCaptureStateAsync(DatasetSpec spec, CancellationToken ct);
}

/// <summary>One dataset's server-side cdc state. <see cref="Detail"/> lines are human-facing and
/// must contain no connection config. <see cref="RetainedBytes"/>: Postgres = WAL retained by the
/// slot; SQL Server = null. <see cref="Healthy"/> false = an unmet prerequisite or a retention gap;
/// Detail then carries the exact remediation statement(s).</summary>
public sealed record ChangeCaptureStatus(bool Healthy, string? PositionName, long? RetainedBytes,
    IReadOnlyList<string> Detail);
