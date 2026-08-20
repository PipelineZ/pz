using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

[assembly: PzConnector("fakesource", typeof(FakeSourceConnector.SourceConnector))]

namespace FakeSourceConnector;

public sealed class SourceConnector : ISourceConnector
{
    public ConnectorInfo Info => new("fakesource", FakeTransitiveDep.Info.Marker, ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult<ISource>(new Source());

    private sealed class Source : ISource
    {
        private static Schema BuildSchema() => new Schema.Builder()
            .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
            .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
            .Build();

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
            => ValueTask.FromResult(new DatasetSchema(BuildSchema()));

        public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
        { scan = null; return false; }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(
            DatasetSpec spec, ReadHints hints, CancellationToken ct)
        {
            var rows = spec.Options.TryGetValue("rows", out var r) ? Convert.ToInt64(r) : 3L;
            return ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([new Partition(rows)]);
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Partition(long rows) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            BatchOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            var schema = new Schema.Builder()
                .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
                .Field(f => f.Name("name").DataType(StringType.Default).Nullable(true))
                .Build();
            var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes);
            for (long i = 1; i <= rows; i++)
            {
                ct.ThrowIfCancellationRequested();
                builder.AppendRow([i, $"row-{i}"]);
                if (builder.TryTakeBatch(out var batch)) yield return batch!;
            }
            if (builder.Flush() is { } last) yield return last;
            await Task.CompletedTask;
        }
    }
}
