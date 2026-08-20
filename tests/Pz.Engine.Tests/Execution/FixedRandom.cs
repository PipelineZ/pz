namespace Pz.Engine.Tests.Execution;

/// <summary>Deterministic jitter stub shared across the resilience test tree: <c>NextDouble()</c>
/// pinned at 0.5 maps to a jitter factor of exactly 1.0 in <see cref="Pz.Engine.Resilience.RetryPolicy.ComputeDelay"/>
/// (see its doc comment), so tests can assert exact backoff values instead of a jittered range.</summary>
internal sealed class FixedRandom(double value) : Random
{
    public override double NextDouble() => value;
}
