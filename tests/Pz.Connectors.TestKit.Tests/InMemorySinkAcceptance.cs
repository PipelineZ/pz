using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.TestKit.Reference;

public class InMemorySinkAcceptance : SinkConnectorAcceptanceTests
{
    protected override ISinkConnector CreateSink() => new InMemoryConnector();

    protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

    protected override OutputSpec SmallOutput => new("memsink", "out", "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput => new("memsink", "merge-out", "merge", "fail_on_change",
        new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override OutputSpec? ReplaceOutput => new("memsink", "replace-out", "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override OutputSpec? TypeMatrixOutput => new("memsink", "type-matrix-out", "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec) =>
        ReadByOutputAsync(connector, spec);

    // InMemorySink stores whatever RecordBatch it is handed, whatever the schema -- the same filter
    // ReadCommittedAsync uses answers the type-matrix fact too.
    protected override ValueTask<IReadOnlyList<RecordBatch>> ReadTypeMatrixCommittedAsync(ISinkConnector connector, OutputSpec spec) =>
        ReadByOutputAsync(connector, spec);

    private static ValueTask<IReadOnlyList<RecordBatch>> ReadByOutputAsync(ISinkConnector connector, OutputSpec spec)
    {
        var memConnector = (InMemoryConnector)connector;
        IReadOnlyList<RecordBatch> result = memConnector.Committed
            .Where(c => c.Spec.Output == spec.Output)
            .SelectMany(c => c.Batches)
            .ToList();
        return new ValueTask<IReadOnlyList<RecordBatch>>(result);
    }
}
