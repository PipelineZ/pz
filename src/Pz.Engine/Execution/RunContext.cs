using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Every optional member defaults to null (or its "off" value) so a directly-constructed
/// <see cref="RunContext"/> — a test, or a caller with no planning step — is a valid one.
///
/// <paramref name="Plan"/>: executors that consult it must fall back to
/// <see cref="ExecutionPlan.StrategyFor"/>'s default when it is null.
/// <paramref name="Batch"/>: the configured
/// <c>engine.batch_bytes</c> shaping, or null when unset/not constructed by a caller that plans batch
/// sizing — <see cref="EffectiveBatch"/> is what every batch-producing executor should actually use.
/// <paramref name="Watermarks"/>/<paramref name="FullRefresh"/>/<paramref name="Notice"/> default to
/// "no watermark store"/"not a full refresh"/"no notice sink": <see cref="SourceLoadExecutor"/>
/// reads the prior watermark for an incremental dataset via <see cref="Watermarks"/> unless
/// <see cref="FullRefresh"/> is set (full-refresh skips only the read side — capture and
/// advancement still run), and routes <see cref="Pz.Engine.State.WatermarkStore.Get"/>'s corrupt-file
/// notice through <see cref="Notice"/> for the CLI layer to render.
/// <paramref name="Time"/> mirrors <see cref="Batch"/>/<see cref="EffectiveBatch"/>
/// — <see cref="EffectiveTime"/> is what every time-consuming executor should actually use; tests inject
/// a fake clock so stall attribution is asserted deterministically instead of over load-sensitive wall
/// time.
/// <paramref name="Breakers"/> (see <see cref="BreakerRegistry"/>'s
/// own null-object design): absent means "no <c>engine.breaker:</c> configured for this run", and
/// <see cref="KindDispatchingExecutor"/>'s gate is a no-op whenever this is null.
/// <paramref name="Reuse"/> is non-null only on a <c>pz retry</c> run — SourceLoads present in the
/// manifest copy the failed run's staged table instead of extracting.
/// <paramref name="SyncState"/> mirrors <see cref="Watermarks"/> but for
/// `sync:` datasets — <see cref="SourceLoadExecutor"/> reads the prior opaque sync-state token for a
/// sync dataset via <see cref="SyncState"/> unless <see cref="FullRefresh"/> is set (same read-side gate
/// as watermarks), and <see cref="Pz.Engine.State.SyncStateAdvancement"/> is the only writer.
/// <paramref name="RateLimiters"/>: unlike <see cref="Breakers"/> —
/// null unless <c>engine.breaker:</c> is configured — production constructs this registry
/// UNCONDITIONALLY, since budget-hint pacing must work for a gated connector even when no
/// <c>rate_limit:</c> appears anywhere in the project; null here only ever means "a test built a
/// RunContext directly, without a registry" — <see cref="SourceLoadExecutor"/>/<see cref="SinkWriteExecutor"/>
/// still hand a gate-aware connector an <see cref="Resilience.OperationGate"/> in that case (op-level
/// retry alone, with no pacing behind it).
/// <paramref name="SchemaBaselines"/>/<paramref name="OnSourceDrift"/>:
/// <see cref="SchemaDriftGate"/> is the sole reader of both — null <see cref="SchemaBaselines"/>
/// (every direct-built test <see cref="RunContext"/>) and the default <see cref="DriftPolicy.Ignore"/>
/// both independently short-circuit the gate before it ever runs a DESCRIBE.</summary>
public sealed record RunContext(IDuckSession Duck, ConnectorRegistry Connectors, RunPaths Paths, IRunEvents Events,
    ExecutionPlan? Plan = null, BatchOptions? Batch = null, WatermarkStore? Watermarks = null,
    bool FullRefresh = false, Action<string>? Notice = null, TimeProvider? Time = null,
    BreakerRegistry? Breakers = null, ReuseManifest? Reuse = null, SyncStateStore? SyncState = null,
    RateLimiterRegistry? RateLimiters = null, SchemaBaselineStore? SchemaBaselines = null,
    DriftPolicy OnSourceDrift = DriftPolicy.Ignore)
{
    /// <summary>Side-band slot for the delivery stats of a sink attempt that
    /// FAILED — <see cref="SinkWriteExecutor"/> records here just before rethrowing (the thrown
    /// exception stays the retry vehicle), and <see cref="KindDispatchingExecutor"/> consumes
    /// (TryRemove) when materializing the terminal Failed NodeResult. Observability side-band
    /// only, never control flow. A get-only property with an initializer: `with`-clones share
    /// the same dictionary instance, which is exactly right — the slot is per-run state.</summary>
    public System.Collections.Concurrent.ConcurrentDictionary<Pz.Core.Dag.NodeId, DeliveryStats> DeliveryFailures { get; } = new();

    /// <summary>Which attempt at each node is currently executing, from 1.
    /// <see cref="KindDispatchingExecutor"/> owns the retry loop and stamps this before every call into
    /// the inner executor; <see cref="SinkWriteExecutor"/> reads it to fill
    /// <see cref="Pz.Connectors.Abstractions.OutputSpec.Attempt"/>, so a sink that can record a durable
    /// progress marker can tell a retry from a first attempt. Same shape and lifetime as
    /// <see cref="DeliveryFailures"/>: per-run state that `with`-clones share, and an absent entry means
    /// attempt 1 (a directly-built RunContext in a test, or an executor invoked without the retry
    /// loop).</summary>
    public System.Collections.Concurrent.ConcurrentDictionary<Pz.Core.Dag.NodeId, int> Attempts { get; } = new();

    /// <summary>Per-run memo of native setup statements (extension installs, secrets, session
    /// settings, attaches) already issued on <see cref="Duck"/> — see <see cref="NativeSetupLedger"/>.
    /// Same shape and lifetime as <see cref="DeliveryFailures"/>/<see cref="Attempts"/>: a get-only
    /// property with an initializer, so `with`-clones share the same ledger instance, which is exactly
    /// right — the memo is per-run state, not per-node. Built from <see cref="Duck"/> at construction
    /// (not lazily against whichever session an executor happens to pass), so the ledger can only ever
    /// be asked to run a statement against the session it is bound to. A `with`-clone that also
    /// replaces <see cref="Duck"/> would keep the ORIGINAL session's ledger — bound to a record field's
    /// value at construction time, a positional-record initializer does not re-run on `with` — but no
    /// production code clones a RunContext with a different Duck.</summary>
    internal NativeSetupLedger SetupLedger { get; } = new(Duck);

    /// <summary><see cref="Batch"/> when set, else <see cref="BatchOptions.Default"/> (32MB / 122,880
    /// rows) — every batch-producing site (universal source reads, egress) should read this, not
    /// <see cref="Batch"/> directly, so a RunContext built without an explicit Batch still gets the
    /// default shaping.</summary>
    public BatchOptions EffectiveBatch => Batch ?? BatchOptions.Default;

    /// <summary><see cref="Time"/> when set, else <see cref="TimeProvider.System"/> — mirrors
    /// <see cref="EffectiveBatch"/>.</summary>
    public TimeProvider EffectiveTime => Time ?? TimeProvider.System;
}
