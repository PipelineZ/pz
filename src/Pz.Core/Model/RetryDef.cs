namespace Pz.Core.Model;

/// <summary>The optional `retry:` block — declared at instance level (sources/sinks file top level)
/// and/or dataset/output level. Every member
/// nullable — an absent key means "inherit that field from the next level up" (dataset/output ->
/// instance -> engine default); the cascade happens in Pz.Engine (RetryPolicyResolver), never here,
/// because Pz.Core cannot see RetryPolicy.Default (layering is strictly downward). Runtime execution
/// policy like CheckNodeDef.SampleValues: deliberately excluded from DagCompiler's canonical-hash
/// input, so declaring/changing it never changes a NodeId.</summary>
public sealed record RetryDef(int? MaxAttempts, TimeSpan? BaseDelay, TimeSpan? MaxDelay);

/// <summary>Instance-level request budget. Bucket capacity is
/// <see cref="EffectiveBurst"/>; refill rate is RequestsPerMinute/60 tokens per second.</summary>
public sealed record RateLimitDef(int RequestsPerMinute, int? Burst)
{
    public int EffectiveBurst => Burst ?? Math.Max(1, RequestsPerMinute / 60);
}
