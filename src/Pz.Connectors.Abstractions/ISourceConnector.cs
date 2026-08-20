using System.Collections.Generic;
using Apache.Arrow;

namespace Pz.Connectors.Abstractions;

public interface ISourceConnector : IConnector
{
    /// <summary>Opens a configured source. The returned object owns any connection state and is
    /// disposed by the engine exactly once.</summary>
    ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct);
}

public interface ISource : IAsyncDisposable
{
    /// <summary>The schema batches will actually carry. Must match every produced batch exactly
    /// (field names, types, order) — the TestKit enforces this.</summary>
    ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct);

    /// <summary>Fast path: expose this dataset as a DuckDB-native scan. Return false to use the
    /// universal batch path. Must be cheap and offline.</summary>
    bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan);

    /// <summary>Plans 1..N independently readable partitions. Return a single partition unless the
    /// <see cref="ConnectorCapabilities.PartitionedRead"/> capability is declared. The union of all
    /// partitions' rows must equal one full read; partitions may be read concurrently.</summary>
    ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct);
}

public interface IDatasetPartition
{
    /// <summary>Streams the partition as Arrow batches. Ownership: each yielded batch belongs to the
    /// engine, which disposes it; the connector must not reuse or retain it after yielding.
    /// Cancellation: the enumeration must observe <paramref name="ct"/> between batches and stop
    /// promptly (the TestKit enforces a 5-second bound). Transient failures should surface as
    /// <see cref="PzConnectorException"/> with <c>IsTransient</c> set correctly.</summary>
    IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct);
}

public interface IStreamingSource
{
    /// <summary>Optional streaming variant of ISource: yields partitions lazily. The engine prefers this
    /// path when the source is IStreamingSource AND advertises Capabilities.StreamingPartitions; otherwise it
    /// falls back to ISource.PlanReadAsync. Additive-only — ISource is unchanged.</summary>
    IAsyncEnumerable<IDatasetPartition> PlanReadStreamingAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct);
}
