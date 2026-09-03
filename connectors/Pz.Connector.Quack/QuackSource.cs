using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Quack;

/// <summary>Native-only source over the connection's single attach of the remote server. The
/// plain incremental watermark is pushed down (the server evaluates it); the window ceiling is
/// MUST-apply. <see cref="PlanReadAsync"/> is the PZ0312 refusal stub.</summary>
internal sealed class QuackSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = QuackSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': quack is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset", isTransient: false);
        var fields = columns.Select(kv => QuackTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var alias = QuackSql.Alias(spec.Source);
        scan = new NativeScan(QuackSql.ScanFragment(alias, spec), QuackSql.SetupStatements(config, alias)) { Mechanism = "quack attach" };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': quack reads are native-scan only; they cannot run on the universal tier (remove engine.force_universal)",
            isTransient: false);

    public ValueTask DisposeAsync() => default;
}
