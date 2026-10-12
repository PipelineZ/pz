using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.TestKit.Reference;
using Xunit;

/// <summary>Self-tests for the opt-in stats facts: a connector reporting valid stats passes, one reporting a
/// malformed extra name fails naming the drop, and one without <c>ReportsStats</c> skips.</summary>
public sealed class StatsConformanceTests
{
    private sealed class StatsConnector(bool reports, Action<IConnectorStats> report) : ISourceConnector, ISinkConnector
    {
        private readonly InMemoryConnector _inner = new();

        public ConnectorInfo Info => _inner.Info;
        public ConnectorCapabilities Capabilities =>
            reports ? _inner.Capabilities | ConnectorCapabilities.ReportsStats : _inner.Capabilities;
        public string ConnectionConfigSchema => _inner.ConnectionConfigSchema;
        public string DatasetConfigSchema => _inner.DatasetConfigSchema;
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            _inner.ValidateAsync(config, ct);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            _inner.CheckConnectionAsync(config, ct);

        async ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            new Source(await ((ISourceConnector)_inner).OpenAsync(config, ct), report);

        async ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            new Sink(await ((ISinkConnector)_inner).OpenAsync(config, ct), report);
    }

    private sealed class Source(ISource inner, Action<IConnectorStats> report) : ISource
    {
        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            inner.GetSchemaAsync(spec, ct);

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan) => inner.TryGetNativeScan(spec, out scan);

        public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(
            DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            [.. (await inner.PlanReadAsync(spec, hints, ct)).Select(p => (IDatasetPartition)new Partition(p, report))];

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class Partition(IDatasetPartition inner, Action<IConnectorStats> report) : IDatasetPartition
    {
        public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct)
        {
            report(ConnectorStats.Current);
            return inner.ReadAsync(options, ct);
        }
    }

    private sealed class Sink(ISink inner, Action<IConnectorStats> report) : ISink
    {
        public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy) => inner.TryGetNativeCopy(spec, out copy);

        public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            new Session(await inner.BeginWriteAsync(spec, schema, ct), report);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private sealed class Session(ISinkWriteSession inner, Action<IConnectorStats> report) : ISinkWriteSession
    {
        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct) => inner.WriteBatchAsync(batch, ct);

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
        {
            report(ConnectorStats.Current);
            return inner.CommitAsync(ct);
        }

        public ValueTask AbortAsync(CancellationToken ct) => inner.AbortAsync(ct);

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    private static void Valid(IConnectorStats s)
    {
        s.AddRequests(2);
        s.AddWireBytes(100);
        s.AddExtra("fake.pages", 3);
    }

    private static void BadExtra(IConnectorStats s) => s.AddExtra("Bad", 1);

    private sealed class SourceFacts(bool reports, Action<IConnectorStats> report) : InMemorySourceAcceptance
    {
        protected override ISourceConnector CreateSource() => new StatsConnector(reports, report);
    }

    private sealed class SinkFacts(bool reports, Action<IConnectorStats> report) : InMemorySinkAcceptance
    {
        protected override ISinkConnector CreateSink() => new StatsConnector(reports, report);
    }

    [Fact]
    public async Task Source_valid_stats_pass() =>
        await new SourceFacts(true, Valid).Reported_stats_are_valid();

    [Fact]
    public async Task Sink_valid_stats_pass() =>
        await new SinkFacts(true, Valid).Reported_write_stats_are_valid();

    [Fact]
    public async Task Source_invalid_extra_name_fails_naming_the_drop()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => new SourceFacts(true, BadExtra).Reported_stats_are_valid());
        Assert.Contains("InvalidExtraName", ex.Message);
    }

    [Fact]
    public async Task Sink_invalid_extra_name_fails_naming_the_drop()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => new SinkFacts(true, BadExtra).Reported_write_stats_are_valid());
        Assert.Contains("InvalidExtraName", ex.Message);
    }

    [Fact]
    public async Task Non_reporting_connectors_skip()
    {
        await Assert.ThrowsAsync<SkipException>(() => new SourceFacts(false, BadExtra).Reported_stats_are_valid());
        await Assert.ThrowsAsync<SkipException>(() => new SinkFacts(false, BadExtra).Reported_write_stats_are_valid());
    }
}
