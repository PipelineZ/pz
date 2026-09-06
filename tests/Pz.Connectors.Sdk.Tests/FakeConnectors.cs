using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>A source connector with one partition of <c>rows</c> int64 values, optionally
/// feed-shaped. Everything the honest-mapping and capture tests need, nothing more.</summary>
internal sealed class FakeSourceConnector(ConnectorCapabilities capabilities, bool feed) : ISourceConnector
{
    public ConnectorInfo Info => new("fake", "1.0.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ValidationResult([]));
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(true, null));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISource>(feed ? new FeedSource() : new PlainSource());
}

internal class PlainSource : ISource
{
    public static readonly Schema RowSchema = new Schema.Builder().Field(new Field("id", Int64Type.Default, false)).Build();
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        ValueTask.FromResult(new DatasetSchema(RowSchema));
    public bool TryGetNativeScan(
        DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }
    public virtual ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([new CountingPartition(3, spec.PriorSyncState)]);
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class FeedSource : PlainSource, INaturalReadShapeSource
{
    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;
    public override ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([new SyncPartition(3, spec.PriorSyncState)]);
}

/// <summary>Yields <c>rows</c> single-row batches. <see cref="Polls"/> counts sync-state polls.</summary>
internal class CountingPartition(int rows, string? prior) : IDatasetPartition
{
    public int Polls;
    protected int Rows => rows;
    protected string? Prior => prior;
    protected bool Drained;

    public async IAsyncEnumerable<RecordBatch> ReadAsync(
        BatchOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 0; i < rows; i++)
        {
            ct.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return new RecordBatch(PlainSource.RowSchema, [new Int64Array.Builder().Append(i).Build()], 1);
        }

        Drained = true;
    }
}

internal sealed class SyncPartition(int rows, string? prior) : CountingPartition(rows, prior), ISyncStatePartition
{
    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        Polls++;
        candidate = Drained ? $"{Prior ?? "0"}+{Rows}" : null;
        return candidate is not null;
    }
}
