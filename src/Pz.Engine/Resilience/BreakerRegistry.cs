using System.Collections.Concurrent;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Resilience;

/// <summary>Lazily creates and caches one <see cref="CircuitBreaker"/>
/// per source/sink INSTANCE (not per node, not per dataset/output -- mirrors <see cref="InstanceKey"/>'s
/// existing per-instance granularity used by <c>max_concurrency</c>). <see cref="For"/> is the null-object
/// entry point the executor gate consults: a Pipeline/Check node (no owning instance) or -- via
/// <see cref="RunContext.Breakers"/> being null entirely -- a run with no <c>engine.breaker:</c> configured
/// both fall through to "no gating", never touching a breaker at all.
///
/// <para><b>Thread safety / double-creation:</b> <see cref="For"/> uses
/// <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/> with a factory
/// closure rather than a lock. Under concurrent first-access for the same instance key (e.g. two datasets
/// of the same source instance both starting their first node at once, or a capped instance's queued nodes
/// releasing together), the factory delegate may run more than once -- but a fresh <see cref="CircuitBreaker"/>
/// has no side effects at construction (no timer, no I/O, no registration with anything external), and only
/// the ONE instance <c>GetOrAdd</c> actually stores is ever returned to any caller; every other
/// momentarily-constructed instance is simply discarded and garbage-collected, never observed by anything.
/// So the race is benign: state is never split across two breakers for the same key. A lock would remove
/// the redundant (free) construction but adds real contention to every lookup for a benefit that only
/// exists once per distinct instance key over the life of a run -- not a worthwhile trade.</para></summary>
public sealed class BreakerRegistry(
    BreakerConfig config, TimeProvider time,
    Action<string, string, string, string, TimeSpan>? onStateChanged = null)
{
    private readonly ConcurrentDictionary<string, CircuitBreaker> _breakers = new();

    /// <summary><c>null</c> when <paramref name="node"/> has no owning source/sink instance (Pipeline/Check
    /// nodes -- see <see cref="InstanceKey.For"/>); otherwise the instance's breaker, created on first
    /// access and reused for every subsequent node of that same instance.</summary>
    public CircuitBreaker? For(DagNode node)
    {
        var key = InstanceKey.For(node);
        if (key is null)
        {
            return null;
        }

        return _breakers.GetOrAdd(key, static (k, s) => new CircuitBreaker(s.Config, s.Time,
            s.OnStateChanged is null
                ? null
                : (oldState, newState, trigger, coolDown) => s.OnStateChanged(k, oldState, newState, trigger, coolDown)),
            (Config: config, Time: time, OnStateChanged: onStateChanged));
    }
}
