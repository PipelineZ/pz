namespace Pz.Connectors.Abstractions;

/// <summary>Optional behaviors a connector declares so the planner knows what to expect.</summary>
[Flags]
public enum ConnectorCapabilities
{
    None = 0,
    /// <summary>Source honors <see cref="ReadHints.Columns"/>.</summary>
    ColumnPruning = 1,
    /// <summary>Source honors <see cref="ReadHints.PredicateSql"/>.</summary>
    PredicatePushdown = 2,
    /// <summary>Source may return more than one partition from PlanReadAsync.</summary>
    PartitionedRead = 4,
    /// <summary>Source can hand DuckDB a native scan via TryGetNativeScan.</summary>
    NativeScan = 8,
    /// <summary>Sink can hand DuckDB a native copy via TryGetNativeCopy.</summary>
    NativeCopy = 16,
    /// <summary>Sink supports merge/upsert write mode.</summary>
    Merge = 32,
    /// <summary>Sink commit is transactional (atomic swap or equivalent).</summary>
    Transactional = 64,
    /// <summary>Source applies <see cref="DatasetSpec.WatermarkUpperBound"/> (cursor &lt;= value) during
    /// extraction. Unlike WatermarkCursor (ignoring is always correct — merge dedups), a bounded window
    /// is load-bearing for the flaky-source contract, so the planner REFUSES a windowed dataset on a
    /// connector without this flag (PZ0313) rather than letting it silently extract everything.</summary>
    BoundedWindow = 128,
    /// <summary>Connector actually implements calendar-token path templating (see
    /// <c>Pz.Connectors.Abstractions.Paths.PathTemplate</c>): source-side templated-read pruning
    /// (narrowing a native scan to the incremental window's cover) and/or sink-side partitioned
    /// writes (fanning rows out to their `path` bucket via `partition_by`). DagCompiler's
    /// PZ0217/0218/0219/0221 validate the templating *syntax* connector-agnostically, but only a
    /// connector declaring this flag actually acts on the tokens -- the planner REFUSES a
    /// date-templated dataset path or a partitioned output on a connector without this flag
    /// (PZ0314) rather than letting it silently write a literal token folder.</summary>
    PathTemplating = 256,
    /// <summary>Source can yield partitions lazily via IStreamingSource.PlanReadStreamingAsync so the engine
    /// never materializes the full partition list (matters for millions of small files).</summary>
    StreamingPartitions = 512,
    /// <summary>Source applies DatasetSpec.WatermarkLowerInclusive (cursor >= value). Without this
    /// flag the engine will not hand the connector an inclusive bound at all -- it pushes no bound and
    /// lets the pipeline predicate make the cut, rather than silently narrowing it to a strict one.</summary>
    InclusiveWatermarkBound = 1024,
    /// <summary>Source emits an opaque, connector-owned sync-state token (delta link / change token)
    /// the engine stores verbatim and replays via <see cref="DatasetSpec.PriorSyncState"/> — for
    /// change-feed APIs with no ordered cursor. Paired with a dataset whose `sync:` block resolves to
    /// the `feed` read shape (declared `mode: auto` or an omitted `sync:` block; Pz.Core
    /// `SyncModeDef`/`NaturalReadShape`).</summary>
    SyncState = 2048,
    /// <summary>Connector routes its remote operations through the engine's IOperationGate
    /// (implements IOperationGateAware on its source and/or sink). Load-bearing for pacing:
    /// the planner REFUSES a rate_limit: on an instance whose connector lacks this flag (PZ0317)
    /// rather than silently not pacing.</summary>
    GatedOperations = 4096,
    /// <summary>Every partition the source plans implements <see cref="IIdentifiedPartition"/>
    /// with a stable, unique, non-empty id — the engine's partition-scoped retry mode (part
    /// tables + pz_meta ledger) engages only under this flag; without it the read path is
    /// byte-identical to the shared-channel single-ingest path.</summary>
    StablePartitionIds = 8192,
    /// <summary>Some partitions may implement <see cref="ICheckpointingPartition"/>. Requires
    /// <see cref="StablePartitionIds"/> — declaring this flag without it is a connector defect
    /// the planner refuses (PZ0319).</summary>
    CheckpointableReads = 16384,
    /// <summary>Sink supports <c>mode: replace</c> (full-overwrite finalization). The planner
    /// refuses a replace output on a connector without this flag (PZ0324) rather than letting
    /// the mode silently degrade or fail at run time.</summary>
    ReplaceWrites = 32768,
    /// <summary>Sink write sessions implement <see cref="ICheckpointingSinkSession"/>; the
    /// engine maintains a delivery ledger (pz_meta.sink_deliveries) and resumes retried
    /// attempts past the acknowledged prefix instead of re-delivering from zero.</summary>
    CheckpointableWrites = 65536,
    /// <summary>Source supports `sync: {mode: cdc}` datasets: it can land the change-row contract
    /// (`_pz_op`/`_pz_lsn`/`_pz_changed_at` + row columns) for a <see cref="DatasetSpec.ChangeCapture"/>
    /// spec and emits its log position through <see cref="ISyncStatePartition"/>. The planner REFUSES a
    /// cdc dataset on a connector without this flag (PZ0338).</summary>
    ChangeCapture = 131072,
    /// <summary>Sink write sessions can implement <see cref="IDeleteApplyingWriteSession"/> for
    /// cdc-fed merge outputs. The planner REFUSES `on_delete: delete|soft` on a sink without this
    /// flag (PZ0339); `on_delete: ignore` needs no capability.</summary>
    ApplyDeletes = 262144,
    /// <summary>Sink wants per-column maximum text lengths for the staged relation, delivered via
    /// <see cref="OutputSpec.MaxTextLengths"/> before BeginWriteAsync — so it can size text DDL
    /// instead of defaulting to an unbounded type. Purely an optimization hint: the engine computes
    /// nothing for sinks without this flag.</summary>
    TextLengthStats = 524288,
}
