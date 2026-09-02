using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckDb;

/// <summary>Native-only source: <see cref="TryGetNativeScan"/> always succeeds with a fragment over
/// the connection's single read-write attach; <see cref="PlanReadAsync"/> is the PZ0312 refusal
/// stub. The plain (unwindowed) incremental watermark is pushed down (the database-source rule);
/// the windowed pair is MUST-apply.</summary>
internal sealed class DuckDbSource(ConnectorConfig config) : ISource
{
    /// <summary>With a declared `columns:` contract, the contract IS the schema (no driver, nothing
    /// to probe offline). Contract-less is a permanent refusal reached only by
    /// `pz validate --connect`'s drift precheck.</summary>
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = DuckDbSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': duckdb is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => DuckDbTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var alias = DuckDbSql.Alias(spec.Source);
        scan = new NativeScan(
            DuckDbSql.ScanFragment(alias, spec),
            DuckDbSql.SetupStatements(DuckDbSql.ResolvePath(config), alias))
        {
            Mechanism = "attach",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': duckdb reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
