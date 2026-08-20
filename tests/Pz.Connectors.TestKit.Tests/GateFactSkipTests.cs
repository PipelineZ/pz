using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

/// <summary>Proves the <c>GateFact()</c> hook actually gates, so docker-backed connector subclasses
/// (e.g. <c>PostgresSourceAcceptance</c>) SKIP rather than FAIL when docker is absent: a subclass whose
/// <see cref="AlwaysSkip"/> gate always skips must never reach
/// <c>CreateSource</c>/<c>ValidConfig</c>/<c>SmallDataset</c> (each throws if called), and the
/// SkipException it raises is the exact type
/// <see href="https://www.nuget.org/packages/Xunit.SkippableFact">Xunit.SkippableFact</see>'s
/// <c>[SkippableFact]</c> test-case wrapper intercepts to report a clean Skip instead of a
/// Failure.</summary>
public sealed class GateFactSkipTests
{
    private sealed class AlwaysSkip : SourceConnectorAcceptanceTests
    {
        protected override void GateFact() => Skip.If(true, "GateFactSkipTests: gate always skips by design");

        protected override ISourceConnector CreateSource() =>
            throw new InvalidOperationException("GateFact() should have skipped before CreateSource() was ever called");

        protected override ConnectorConfig ValidConfig =>
            throw new InvalidOperationException("GateFact() should have skipped before ValidConfig was ever read");

        protected override DatasetSpec SmallDataset =>
            throw new InvalidOperationException("GateFact() should have skipped before SmallDataset was ever read");
    }

    [Fact]
    public async Task Gate_skip_produces_skipped_not_failed()
    {
        var sut = new AlwaysSkip();

        // Every base fact calls GateFact() as its first statement, so invoking one directly
        // -- bypassing the xunit runner entirely -- is enough to prove the skip happens before any real
        // work: this throws Xunit.SkipException, not the InvalidOperationException that CreateSource(),
        // ValidConfig, or SmallDataset would throw if GateFact() ever let execution past it.
        await Assert.ThrowsAsync<SkipException>(() => sut.Validate_accepts_valid_config());
        await Assert.ThrowsAsync<SkipException>(() => sut.Schema_matches_produced_batches());
    }
}
