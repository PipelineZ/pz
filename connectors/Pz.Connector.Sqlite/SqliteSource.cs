using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sqlite;

/// <summary>Native-only SQLite source: <see cref="TryGetNativeScan"/> always succeeds with a
/// self-contained <c>sqlite_scan('&lt;path&gt;', '&lt;table&gt;')</c> fragment — no attach, no alias,
/// no setup beyond the extension itself; <see cref="PlanReadAsync"/> is the PZ0312 refusal stub
/// (ParquetSource/MySqlSource precedent).
///
/// The PLAIN (unwindowed) incremental watermark IS pushed down (the database-source rule the MySQL
/// connector established): DatasetSpec's contract explicitly permits it, and DuckDB's sqlite scanner
/// pushes the filter into the file scan. The windowed pair (BoundedWindow) is MUST-apply as
/// everywhere else.</summary>
internal sealed class SqliteSource(ConnectorConfig config) : ISource
{
    /// <summary>With a declared `columns:` contract, the contract IS the schema (the
    /// MySQL/Azure precedent — no driver, nothing to probe offline). Contract-less is a clear
    /// permanent refusal reached only by `pz validate --connect`'s drift precheck; everything else
    /// (plain validate, run, the on_source_drift gate) never calls this.</summary>
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = SqliteSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': sqlite is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => SqliteTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        scan = new NativeScan(
            SqliteSql.ScanFragment(SqliteSql.ResolvePath(config), spec),
            SqliteSql.SetupStatements())
        {
            Mechanism = "sqlite_scan",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': sqlite reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
