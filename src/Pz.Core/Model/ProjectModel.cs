namespace Pz.Core.Model;

public sealed record PzProject(string Name, string Version, EngineConfig Engine,
    IReadOnlyDictionary<string, object?> Vars, IReadOnlyList<ConnectorRequirement> Connectors,
    IReadOnlyList<ConnectionDef> Connections, IReadOnlyList<PipelineDef> Pipelines,
    RetentionConfig? Retention = null, StateConfig? State = null,
    DriftPolicy OnSourceDrift = DriftPolicy.Ignore)
{
    /// <summary>Shadows the positional <c>State</c> parameter above with a non-nullable property: every
    /// hand-constructed <see cref="PzProject"/> (every test that builds one directly) keeps compiling by
    /// omitting the trailing argument, but nothing that reads <see cref="PzProject.State"/> ever sees
    /// null -- an omitted/null argument resolves to <see cref="StateConfig.Default"/> (local) right here,
    /// so callers never need a separate "OrDefault" accessor.</summary>
    public StateConfig State { get; init; } = State ?? StateConfig.Default;
}
/// <summary><see cref="CheckSamples"/> is the project-wide default for whether a failing check's error
/// message includes sample violating rows; defaults <c>true</c>. A per-check
/// <c>CheckDef.SampleValues</c>
/// override, when present, wins over this default in both directions -- resolved once at compile into
/// <c>CheckNodeDef.SampleValues</c>.</summary>
public sealed record EngineConfig(int Threads = 4, DuckOptionsConfig? DuckDb = null, bool ForceUniversal = false,
    int? BatchBytes = null, bool CheckSamples = true, BreakerConfig? Breaker = null);
public sealed record DuckOptionsConfig(string? MemoryLimit = null, int? Threads = null, string? TempDirectory = null);
/// <summary><c>engine.breaker</c> — config for the engine-owned
/// <c>Pz.Engine.Resilience.CircuitBreaker</c>. Absent -> the breaker is off entirely (no instance
/// trips, matching <c>retry:</c>/<c>max_concurrency:</c>'s "absent means off/unbounded" convention).
/// <see cref="FailureThreshold"/> consecutive transient failures trip Closed -&gt; Open; <see cref="CoolDown"/>
/// is the minimum wait before a single half-open probe is granted (a per-failure <c>retryAfterFloor</c> can
/// extend it further at open time, never shorten it).</summary>
public sealed record BreakerConfig(int FailureThreshold, TimeSpan CoolDown);
public sealed record ConnectorRequirement(string Package, string Version);
/// <summary>project.yml's top-level <c>retention:</c>. Null on
/// <see cref="PzProject.Retention"/> means retention is OFF -- the <c>keep_last: 10</c> default is
/// materialized by <see cref="Pz.Core.Loading.ProjectLoader"/> alone, never by this record's defaults, so
/// a hand-constructed <see cref="PzProject"/> (every test that builds one) never sweeps anything.
/// <see cref="KeepLast"/> is always >= 1 by construction: the loader rejects 0 (PZ0123).</summary>
public sealed record RetentionConfig(int KeepLast);

/// <summary>project.yml's top-level <c>on_source_drift:</c> -- what a run does when a SourceLoad's
/// landed schema no longer matches what a prior run of the same node landed (added/removed/retyped
/// columns). Absent -> <see cref="Ignore"/>, matching every other off-by-default gate in this file
/// (<c>engine.breaker</c>, <c>rate_limit</c>).</summary>
public enum DriftPolicy
{
    Ignore,
    Warn,
    Fail,
}
