using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>Native-only source: a catalog connection scans the attached catalog's table, a
/// <c>files</c> connection scans the table directory with <c>iceberg_scan</c>. The plain
/// incremental watermark is pushed down; the window ceiling is MUST-apply; time travel pins the
/// snapshot. <see cref="PlanReadAsync"/> is the PZ0312 refusal stub.</summary>
internal sealed class IcebergSource(ConnectorConfig config) : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = IcebergSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': iceberg is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => IcebergTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    /// <summary>A <c>files</c> read against a local root checks the table directory exists before
    /// returning the scan: <c>iceberg_scan</c>'s own error names the absolute path, and a missing
    /// directory is almost always an entity or <c>root</c> typo best reported at plan time. The
    /// resolved path is never in the message (path hygiene); the dataset name locates the fix.</summary>
    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var alias = IcebergSql.Alias(spec.Source);
        if (IcebergCatalog.Of(config) == IcebergCatalog.Files)
        {
            var root = IcebergSql.ResolveRoot(config);
            var path = IcebergSql.TablePath(root, spec.Dataset);
            if (!IcebergSql.IsUrl(root) && !Directory.Exists(path))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': no Iceberg table directory under the connection's 'root' -- " +
                    "check the entity name (namespace.table maps to root/namespace/table) or the 'root'", isTransient: false);
            }

            scan = new NativeScan(IcebergSql.FilesScanFragment(root, spec), IcebergSql.SetupStatements(config, alias))
            {
                Mechanism = "iceberg scan",
            };
            return true;
        }

        scan = new NativeScan(IcebergSql.ScanFragment(alias, spec), IcebergSql.SetupStatements(config, alias))
        {
            Mechanism = "iceberg attach",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': iceberg reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
