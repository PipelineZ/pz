using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

/// <summary>The suite-wide <c>GateFact()</c> hook can only skip everything, which costs a subclass the
/// facts it does satisfy. <c>ShouldRun(fact)</c> is the per-fact granularity — proven here by a subclass
/// that excludes exactly one fact and must still run the others.</summary>
public sealed class PerFactGateTests
{
    private sealed class ExcludesOneFact : SourceConnectorAcceptanceTests
    {
        protected override bool ShouldRun(string fact) => fact != nameof(Schema_matches_produced_batches);

        protected override ISourceConnector CreateSource() =>
            throw new InvalidOperationException("reached CreateSource(): this subclass exists only to observe gating");

        protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

        protected override DatasetSpec SmallDataset => new("mem", "small", new Dictionary<string, object?>());
    }

    [Fact]
    public async Task Excluded_fact_skips_and_the_others_still_run()
    {
        var sut = new ExcludesOneFact();

        await Assert.ThrowsAsync<SkipException>(() => sut.Schema_matches_produced_batches());

        // Not excluded, so gating lets it through and it reaches the subclass's own fixture — which
        // throws. The distinct exception type is the proof that ShouldRun gated one fact, not the suite.
        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.Read_is_deterministic_across_two_reads());
    }

    /// <summary>The pre-existing all-or-nothing hook must keep working untouched: a subclass that only
    /// overrides <c>GateFact()</c> is the shape every docker-backed connector already ships.</summary>
    private sealed class SkipsEverything : SourceConnectorAcceptanceTests
    {
        protected override void GateFact() => Skip.If(true, "gate always skips by design");

        protected override ISourceConnector CreateSource() =>
            throw new InvalidOperationException("GateFact() should have skipped first");

        protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

        protected override DatasetSpec SmallDataset => new("mem", "small", new Dictionary<string, object?>());
    }

    [Fact]
    public async Task GateFact_still_gates_every_fact()
    {
        var sut = new SkipsEverything();

        await Assert.ThrowsAsync<SkipException>(() => sut.Schema_matches_produced_batches());
        await Assert.ThrowsAsync<SkipException>(() => sut.Read_is_deterministic_across_two_reads());
    }
}
