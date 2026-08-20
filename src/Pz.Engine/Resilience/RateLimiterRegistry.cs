using System.Collections.Concurrent;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Resilience;

/// <summary>Lazily materializes one InstancePacingState per source/sink
/// instance, shared across every node/partition/attempt of the run. Unlike BreakerRegistry
/// (null unless engine.breaker: is configured), this registry is ALWAYS constructed — budget
/// hints must work for gated connectors with no rate_limit: anywhere in the project; only the
/// entry's token bucket is conditional on the instance's RateLimitDef. Benign double-construction
/// race on GetOrAdd accepted, same as BreakerRegistry.</summary>
public sealed class RateLimiterRegistry(TimeProvider time)
{
    private readonly ConcurrentDictionary<string, InstancePacingState> _states = new();

    public InstancePacingState? For(DagNode node)
    {
        var key = InstanceKey.For(node);
        if (key is null)
        {
            return null;
        }

        return _states.GetOrAdd(key, static (_, s) => new InstancePacingState(s.RateLimit, s.Time),
            (RateLimit: RateLimitFor(node), Time: time));
    }

    private static RateLimitDef? RateLimitFor(DagNode node) => node.Definition switch
    {
        SourceDatasetDef def => def.Source.RateLimit,
        SinkOutputDef def => def.Sink.RateLimit,
        _ => null,
    };
}
