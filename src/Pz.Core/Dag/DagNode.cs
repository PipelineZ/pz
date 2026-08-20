using Pz.Core.Model;

namespace Pz.Core.Dag;

public enum NodeKind { SourceLoad, Pipeline, Check, SinkWrite }

/// <summary>SQL-declared incremental: snapshot of all watermark() substitutions within a pipeline's
/// rendered SQL. Each entry records the sentinel that was emitted, the source/dataset it resolved to, and the
/// cursor column's DuckDB type for replacement at execution time. Substitution text itself (literals containing
/// the sentinel) is run-varying and never included in artifacts or NodeId hashes — it survives only in this
/// Definition list, consumed by PipelineExecutor at execution time to rewrite the sentinel out of the query
/// before execution.</summary>
public sealed record WatermarkSubstitution(string Sentinel, string SourceName, string Dataset, string? CursorType);

public sealed record DagNode(NodeId Id, NodeKind Kind, string Name, IReadOnlyList<NodeId> DependsOn,
    string? RenderedSql, object Definition)
{
    public IReadOnlyList<WatermarkSubstitution> WatermarkSubstitutions { get; init; } = [];
}

/// <summary>Definition for SourceLoad. <see cref="Hints"/> is what the dataset's single reading
/// pipeline lets pz ask the connector for; null when nothing is pushable, or
/// when no SQL parser is wired. It feeds the NodeId: a different projection is a different extraction,
/// and the staged table's shape depends on it, so retry must not reuse a staged table whose columns no
/// longer match.</summary>
public sealed record SourceDatasetDef(ConnectionDef Source, DatasetDef Dataset, ReadHintPlan? Hints = null);

/// <summary>Definition for SinkWrite. <see cref="Output"/>'s <c>Input</c> is always populated with the
/// resolved binding (the feeding pipeline's name, or "source.dataset" for a passthrough drain) even
/// when the output was bound inline by a pipeline's leading <c>INSERT INTO</c> rather than via
/// YAML — the raw YAML output declared no `input:` in that case, so DagCompiler synthesizes it
/// here for every downstream consumer (the
/// executor's staging-relation lookup, the canonical NodeId hash) that expects it populated.
/// <see cref="IsInlineBound"/> is the one place that distinction survives, for
/// <c>ManifestWriter</c>'s compiled-artifact binding header.
///
/// <see cref="CdcDeleteOrigin"/> is stamped by DagCompiler's cdc pairing-matrix pass iff
/// <c>Output.OnDelete</c> is <c>delete</c>/<c>soft</c> AND delete-key routing is valid (the SinkWrite's
/// upstream closure has exactly one SourceLoad and it is the cdc one); it is read at execution time to
/// route deletes. Set via `with` AFTER the node's NodeId is already computed from the plain canonical
/// string (sink/output/input/mode/schema/options — see DagCompiler stage 10), so it is derived
/// presentation state that never feeds the hash.</summary>
public sealed record SinkOutputDef(ConnectionDef Sink, OutputDef Output, bool IsInlineBound = false)
{
    public CdcOrigin? CdcDeleteOrigin { get; init; }
}

/// <summary>The single upstream <c>sync: {mode: cdc}</c> dataset a merge output's
/// <c>on_delete: delete</c>/<c>soft</c> delete keys are routed from. See
/// <see cref="SinkOutputDef.CdcDeleteOrigin"/>.</summary>
public sealed record CdcOrigin(string Source, string Dataset);

/// <summary>Definition for Check nodes: carries the owning pipeline's name alongside the check
/// itself so <c>CheckExecutor</c> can build <c>staging.&lt;pipeline&gt;</c> without parsing the node's
/// name. Purely a presentation-layer wrapper -- <c>DagCompiler</c>'s canonical-hash input for a check
/// node is kind/pipeline name/check type/columns/options only. <see cref="SampleValues"/> is the fully
/// resolved <c>perCheckOverride ?? projectDefault ?? true</c> opt-out flag -- a *runtime execution*
/// flag consumed only by <see cref="Pz.Engine.Checks.CheckExecutor"/>, deliberately excluded from that
/// canonical-hash input so toggling it never changes a NodeId or golden compile output.</summary>
public sealed record CheckNodeDef(string PipelineName, CheckDef Check, bool SampleValues = true);
