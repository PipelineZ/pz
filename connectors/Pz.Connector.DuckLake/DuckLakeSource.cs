using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake;

/// <summary>Native-only source over the connection's single read-write attach. The plain
/// incremental watermark is pushed down; the window ceiling is MUST-apply; time travel pins the
/// snapshot. <see cref="PlanReadAsync"/> is the PZ0312 refusal stub.</summary>
internal sealed class DuckLakeSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = DuckLakeSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': ducklake is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => DuckLakeTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    /// <summary>The shared alias is read-write (writes must be able to create the catalog), so an
    /// unguarded `attach if not exists` against a missing file-backed catalog would silently create
    /// an empty catalog and "succeed" at reading zero rows -- indistinguishable from an empty table
    /// and a likely path typo. Refuse before returning the scan instead, for the two catalogs that
    /// are files (duckdb, sqlite); a server catalog (postgres/quack/motherduck) has no local file to
    /// check and is left to the attach itself. The resolved absolute path is never in the message
    /// (secret/path hygiene); the dataset name is enough to locate the fix.</summary>
    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var catalog = DuckLakeCatalog.Of(config);
        if (catalog is DuckLakeCatalog.DuckDb or DuckLakeCatalog.Sqlite &&
            !File.Exists(DuckLakeSql.ResolveLocal(config, "path")))
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': ducklake catalog file does not exist; reads cannot create it -- " +
                "create it with a write first, or fix the connection's 'path'", isTransient: false);
        }

        var alias = DuckLakeSql.Alias(spec.Source);
        scan = new NativeScan(DuckLakeSql.ScanFragment(alias, spec), DuckLakeSql.SetupStatements(config, alias))
        {
            Mechanism = "ducklake attach",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': ducklake reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
