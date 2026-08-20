using Pz.Core.Dag;

namespace Pz.Engine.Planning;

/// <summary>One node's planned strategy. Serializable shape (plan.json) — the executor already holds
/// the DagNode, so only identity travels here. Reason is user-facing and template-only:
/// it must never contain SQL fragments, setup statements, or resolved absolute paths.</summary>
public sealed record PlannedNode(NodeId Id, NodeKind Kind, string Name, EdgeStrategy Strategy, int Partitions,
    string Reason, PushdownInfo? Pushdown = null);

/// <summary>What a SourceLoad will ask its connector for, so a pushed
/// read is distinguishable from an unpushed one without watching the network. Counts only — predicate
/// TEXT never reaches plan.json, per the same secret/PII hygiene rule that keeps SQL out of Reason
/// strings. <see cref="ColumnsPushed"/> is null when the whole row is read, which is not the same as
/// zero (zero columns cannot happen). Reflects capability gating but not the run-time schema
/// reconciliation in SourceLoadExecutor, which needs a connection the planner promises never to open;
/// a hint naming a column the source lacks is dropped there and this count would then read high.</summary>
public sealed record PushdownInfo(int? ColumnsPushed, bool PredicatePushed);
