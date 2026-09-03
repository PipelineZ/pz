using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MotherDuck;

/// <summary>Native-only source over the connection's single attach of the MotherDuck database. The
/// plain incremental watermark is pushed down (the server evaluates it); the window ceiling is
/// MUST-apply. <see cref="PlanReadAsync"/> is the PZ0312 refusal stub.</summary>
internal sealed class MotherDuckSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = MotherDuckSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': motherduck is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset", isTransient: false);
        var fields = columns.Select(kv => MotherDuckTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var database = MotherDuckSql.Database(config);
        scan = new NativeScan(MotherDuckSql.ScanFragment(database, spec), MotherDuckSql.SetupStatements(config)) { Mechanism = "motherduck attach" };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': motherduck reads are native-scan only; they cannot run on the universal tier (remove engine.force_universal)",
            isTransient: false);

    public ValueTask DisposeAsync() => default;
}
