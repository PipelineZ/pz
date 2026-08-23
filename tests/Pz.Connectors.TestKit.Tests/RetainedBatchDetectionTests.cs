using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

/// <summary>The ownership protocol says the batch handed to <c>WriteBatchAsync</c> belongs to the engine
/// again the moment the call returns. A sink that keeps the instance rather than copying out of it is
/// the worst bug in this codebase, and the acceptance fact that exists to catch it must actually catch
/// it — a reference-inequality check alone never can, because a connector reading its rows back out of
/// the destination hands back fresh batches either way.
///
/// <para>So this drives the fact against a deliberately RETAINING sink and requires it to fail.</para></summary>
public sealed class RetainedBatchDetectionTests
{
    /// <summary>A sink that keeps every engine-owned batch instead of cloning it, and materializes its
    /// rows only at COMMIT time — by which point the engine has taken those buffers back. This is the
    /// shape an instance-identity check cannot see: what it hands back are fresh batches read out of its
    /// own destination, never the instances it was given, so only their CONTENT betrays the
    /// retention.</summary>
    private sealed class RetainingConnector : ISinkConnector
    {
        public readonly List<RecordBatch> Retained = [];

        public ConnectorInfo Info => new("retaining", "0.1.0", ProtocolVersion.Major);

        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;

        public string ConnectionConfigSchema =>
            """{ "type": "object", "properties": {}, "additionalProperties": false }""";

        public string DatasetConfigSchema =>
            """{ "type": "object", "properties": {}, "additionalProperties": false }""";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));

        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new RetainingSink(this));
    }

    private sealed class RetainingSink(RetainingConnector connector) : ISink
    {
        public bool TryGetNativeCopy(
            OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = null;
            return false;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            new(new RetainingSession(connector));

        public ValueTask DisposeAsync() => default;
    }

    private sealed class RetainingSession(RetainingConnector connector) : ISinkWriteSession
    {
        private readonly List<RecordBatch> _pending = [];

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            _pending.Add(batch); // the bug, on purpose: no Clone()
            return default;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
        {
            // Reading the retained instances only now — the mistake in full: whatever these buffers hold
            // at commit time is what gets persisted, and the engine stopped guaranteeing their contents
            // the moment each WriteBatchAsync returned.
            connector.Retained.AddRange(_pending.Select(b => b.Clone()));
            return new ValueTask<WriteResult>(
                new WriteResult(_pending.Sum(b => (long)b.Length), _pending.Count));
        }

        public ValueTask AbortAsync(CancellationToken ct) => default;

        public ValueTask DisposeAsync() => default;
    }

    private sealed class RetainingSinkAcceptance : SinkConnectorAcceptanceTests
    {
        protected override ISinkConnector CreateSink() => new RetainingConnector();

        protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

        protected override OutputSpec SmallOutput =>
            new("retaining", "out", "append", "fail_on_change", new Dictionary<string, object?>());

        protected override ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(
            ISinkConnector connector, OutputSpec spec) =>
            new(((RetainingConnector)connector).Retained);
    }

    [Fact]
    public async Task Retaining_sink_fails_the_ownership_fact()
    {
        var sut = new RetainingSinkAcceptance();

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => sut.Sink_does_not_retain_engine_owned_batch_instances());

        // The CONTENT assertion specifically: this connector never hands back an instance it was given,
        // so the reference-inequality half of the fact passes it — which is exactly how a retaining
        // connector used to sail through.
        Assert.IsType<Xunit.Sdk.EqualException>(ex);
    }
}
