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

/// <summary>Opens exactly one gate-aware source, and only when the test releases it: two RPCs can
/// therefore be parked past the "already open" fast path at the same time, which is the only shape in
/// which a second gate handover is observable.</summary>
internal sealed class GateCountingSourceConnector : ISourceConnector
{
    private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public GateAwareSource Source { get; } = new();

    public int Opens { get; private set; }

    /// <summary>Completes once <see cref="OpenAsync"/> has been entered, so a test can start a second
    /// RPC knowing the first holds the open gate and the source is still unset.</summary>
    public Task Entered => _entered.Task;

    public void ReleaseOpen() => _release.TrySetResult();

    public ConnectorInfo Info => new("gated", "1.0.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.GatedOperations;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ValidationResult([]));
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(true, null));

    public async ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        Opens++;
        _entered.TrySetResult();
        await _release.Task.WaitAsync(ct);
        return Source;
    }
}

internal sealed class GateAwareSource : PlainSource, IOperationGateAware
{
    public int GateHandovers { get; private set; }

    public void UseOperationGate(IOperationGate gate) => GateHandovers++;
}

/// <summary>The sink counterpart of <see cref="FakeSourceConnector"/>: one write session that records
/// every batch it is handed (a clone, honoring the ABI's "not the caller's buffer past the call"
/// rule) and reports it back on commit.</summary>
internal sealed class FakeSinkConnector : ISinkConnector
{
    public FakeSink Sink { get; } = new();

    /// <summary>How many times <see cref="OpenAsync"/> actually ran -- PcpConnectorService is
    /// supposed to open a sink at most once per process and hand every later BeginWrite the same
    /// instance, whatever op or output each call names.</summary>
    public int Opens { get; private set; }

    public ConnectorInfo Info => new("fake-sink", "1.0.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ValidationResult([]));
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(new ConnectionCheck(true, null));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        Opens++;
        return ValueTask.FromResult<ISink>(Sink);
    }
}

internal sealed class FakeSink : ISink
{
    public FakeWriteSession? LastSession { get; private set; }

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        LastSession = new FakeWriteSession(schema);
        return ValueTask.FromResult<ISinkWriteSession>(LastSession);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class FakeWriteSession(Schema schema) : ISinkWriteSession
{
    private readonly List<RecordBatch> _committed = [];

    public Schema Schema { get; } = schema;
    public bool Committed { get; private set; }
    public bool Aborted { get; private set; }
    public IReadOnlyList<RecordBatch> Batches => _committed;
    public long Rows => _committed.Sum(b => b.Length);

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        // The batch handed in is only valid until this call returns (pooled, off-heap buffers on the
        // real data plane): clone it, as any well-behaved sink must, instead of retaining the argument.
        _committed.Add(batch.Clone());
        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        Committed = true;
        return ValueTask.FromResult(new WriteResult(Rows, _committed.Count));
    }

    public ValueTask AbortAsync(CancellationToken ct)
    {
        Aborted = true;
        return ValueTask.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
