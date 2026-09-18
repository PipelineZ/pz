using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.TestKit.Reference;

/// <summary><see cref="SourceConnectorAcceptanceTests.ValidConfigWithIntegerOptionAsDouble"/>'s own
/// fact, driven directly against a minimal connector that opts in -- proving the mechanism actually
/// threads a bare <see cref="double"/> connection option through a real <c>ValidateAsync</c> call and
/// <c>ConnectorConfig.GetInt</c> without the fact silently skipping. The full acceptance suite is not
/// run here (nothing else about this stub is interesting); only the one fact under test is invoked.</summary>
public sealed class NumericOptionAcceptanceTests
{
    /// <summary>Reads its one connection option ("max_connections") through
    /// <see cref="ConnectorConfig.GetInt"/> -- the exact contract the fact under test exists to prove --
    /// and otherwise delegates to <see cref="InMemoryConnector"/> for everything else the base
    /// suite's abstract members need.</summary>
    private sealed class NumericOptionConnector : ISourceConnector
    {
        private readonly InMemoryConnector _inner = new();

        public ConnectorInfo Info => _inner.Info;

        public ConnectorCapabilities Capabilities => _inner.Capabilities;

        public string ConnectionConfigSchema =>
            """{ "type": "object", "properties": { "max_connections": { "type": ["integer","number"] } }, "additionalProperties": false }""";

        public string DatasetConfigSchema => _inner.DatasetConfigSchema;

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        {
            // A non-integral or out-of-range value would throw PzConnectorException here -- that is
            // the behavior under test, not caught, so a regression fails this fact loudly.
            _ = config.GetInt("max_connections");
            return ((ISourceConnector)_inner).ValidateAsync(ConnectorConfig.Empty, ct);
        }

        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ((ISourceConnector)_inner).CheckConnectionAsync(ConnectorConfig.Empty, ct);

        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            ((ISourceConnector)_inner).OpenAsync(ConnectorConfig.Empty, ct);
    }

    private sealed class Fixture : SourceConnectorAcceptanceTests
    {
        protected override ISourceConnector CreateSource() => new NumericOptionConnector();

        protected override ConnectorConfig ValidConfig =>
            new(new Dictionary<string, object?> { ["max_connections"] = 5L });

        protected override ConnectorConfig? ValidConfigWithIntegerOptionAsDouble =>
            new(new Dictionary<string, object?> { ["max_connections"] = 5.0 });

        protected override DatasetSpec SmallDataset => new("mem", "numbers",
            new Dictionary<string, object?> { ["rows"] = 5L });
    }

    [Fact]
    public async Task Fact_passes_for_a_connector_that_opts_in()
    {
        var fixture = new Fixture();

        // No exception, no Skip -- ValidateAsync ran to completion and reported valid.
        await fixture.Connection_integer_option_delivered_as_a_double_is_accepted();
    }
}
