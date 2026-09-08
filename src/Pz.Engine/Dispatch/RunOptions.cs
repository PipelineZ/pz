using System.Diagnostics;
using Pz.Core.Dag;
using Pz.Engine.Execution;

namespace Pz.Engine.Dispatch;

/// <summary><paramref name="ProjectName"/> is additive (default "") so existing callers/tests keep
/// compiling — it feeds <see cref="Pz.Engine.Execution.IRunEvents.RunStarted"/>'s project-name field
/// — presentation-only, and not part of run identity.
/// <paramref name="Seeded"/> is additive = pre-completed results
/// recorded verbatim into the run — <c>pz retry</c>'s carried-forward sink successes (success-status by
/// domain contract). They must be for nodes OUTSIDE the run's effective set: never dispatched, and fire
/// NodeCompleted once each, after RunStarted. A seeded result whose id IS in the effective set violates
/// this precondition and makes <see cref="RunOrchestrator.ExecuteAsync"/> throw
/// <see cref="ArgumentException"/> (checked before RunStarted ever fires) — the planner guarantees
/// disjointness, but the orchestrator is a public API and this project's "no silent failures" rule
/// applies to it too.
/// <paramref name="RunActivity"/> is additive = the <c>run</c> span the caller already opened around the
/// whole run (every phase, not just execution), which node spans then parent on; with none, the
/// orchestrator opens a <c>run</c> span around execution alone. Never a second <c>run</c> span either
/// way.</summary>
public sealed record RunOptions(int MaxConcurrency = 4, bool FailFast = false,
    IReadOnlySet<NodeId>? Selection = null, string ProjectName = "",
    IReadOnlyList<NodeResult>? Seeded = null, Activity? RunActivity = null);
