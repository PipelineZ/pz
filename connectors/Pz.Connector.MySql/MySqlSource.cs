using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MySql;

/// <summary>Native-only MySQL source: <see cref="TryGetNativeScan"/> always succeeds with a
/// <c>mysql_query('alias', '…')</c> fragment so projection and the watermark window execute inside
/// MySQL; <see cref="PlanReadAsync"/> is the PZ0312 refusal stub (ParquetSource precedent — the
/// bare-string code carries the same documented literal-duplication drift risk).
///
/// Watermark pushdown differs deliberately from the file connectors: the PLAIN (unwindowed)
/// incremental watermark IS pushed down here. Re-reading a whole file is merely wasteful; re-scanning
/// a whole production table defeats incremental EL — and DatasetSpec's contract explicitly permits
/// the pushdown ("connectors MAY apply cursor &gt; value"; ignoring it is the safe floor, not the
/// goal). The windowed pair (BoundedWindow) is MUST-apply as everywhere else.</summary>
internal sealed class MySqlSource(ConnectorConfig config) : ISource
{
    /// <summary>With a declared `columns:` contract, the contract IS the schema (the Azure
    /// json precedent — there is nothing to probe without a driver, and the contract is what the
    /// engine will hold the read to anyway). Contract-less is a clear permanent refusal reached only
    /// by `pz validate --connect`'s drift precheck; everything else (plain validate, run, the
    /// on_source_drift gate) never calls this.</summary>
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var columns = MySqlSql.ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': mysql is native-only and has no offline schema probe -- " +
            "declare a columns: contract to validate shape, or skip --connect for this dataset",
            isTransient: false);

        var fields = columns.Select(kv => MySqlTypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new ValueTask<DatasetSchema>(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var alias = MySqlSql.SourceAlias(spec.Source);
        scan = new NativeScan(
            MySqlSql.ScanFragment(alias, spec),
            MySqlSql.SetupStatements(config, alias, readOnly: true))
        {
            Mechanism = "mysql_query",
        };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': mysql reads are native-scan only; they cannot run on the " +
            "universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
