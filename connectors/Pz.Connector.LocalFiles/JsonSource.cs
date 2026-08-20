using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles;

/// <summary>JSON (newline-delimited) source: native-only via DuckDB's <c>read_json</c>, mirroring
/// azure's json read shape (<c>AzureSource</c>) and the <see cref="ParquetSource"/> native-only
/// precedent — there is no managed NDJSON read tier in v0, so <see cref="PlanReadAsync"/> always throws.
/// The two-state contract model matches <see cref="CsvSource.TryGetNativeScan"/>: a declared
/// `columns:` contract — partial or full — renders a strict <c>columns = {…}</c> map that prunes the
/// read to exactly its declared columns; no contract at all goes through DuckDB's own
/// <c>auto_detect</c>. <c>read_json</c> has no <c>types=</c> parameter, so there is no third state to
/// guard against (the same fact azure's json scan leans on).</summary>
internal sealed class JsonSource(string baseDir) : ISource
{
    /// <summary>No header row to peek (unlike csv) and no footer to read (unlike parquet) — the
    /// declared `columns:` contract IS the schema (azure json precedent), so beyond the existence
    /// check no file bytes are read here at all.</summary>
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var path = ResolvePath(spec);
        if (!File.Exists(path))
        {
            throw new PzConnectorException($"json file not found: '{path}'", isTransient: false);
        }

        var columns = GetColumnsContract(spec);
        var fields = columns.Select(kv => TypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var absPath = ResolvePath(spec);
        var declared = ExtractColumns(spec);
        string fragment;
        if (declared is { Count: > 0 })
        {
            var duckColumns = string.Join(", ", declared.Select(c =>
                $"'{EscapeSqlLiteral(c.Key)}': '{TypeNameMap.ToDuckDbName(c.Value, c.Key)}'"));
            fragment = $"read_json('{EscapeSqlLiteral(absPath)}', columns = {{{duckColumns}}}, format = 'newline_delimited')";
        }
        else
        {
            fragment = $"read_json('{EscapeSqlLiteral(absPath)}', auto_detect = true, format = 'newline_delimited')";
        }

        scan = new NativeScan(LocalFilesWindowSql.Wrap(fragment, spec), SetupStatements: [])
        {
            Mechanism = "read_json",
            SchemaInferred = declared is not { Count: > 0 },
        };
        return true;
    }

    /// <summary>The universal path: like <see cref="ParquetSource.PlanReadAsync"/>, there is no managed
    /// NDJSON reader in v0, so this always throws — reached whenever the planner has no native strategy
    /// for this edge, including under <c>engine.force_universal</c>. Same PZ0312 bare-string convention
    /// (and the same literal-duplication drift risk) documented on the parquet counterpart.</summary>
    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': localfiles json source is native-scan only; it cannot run " +
            "on the universal tier (remove engine.force_universal)", isTransient: false);

    public ValueTask DisposeAsync() => default;

    /// <summary>The entity names the file, and <c>path:</c> overrides that when the layout does not
    /// match the name. A source needs the extension its format implies; the sink writes a directory, so
    /// it does not. An absolute <c>path:</c> ignores the connection's location entirely.</summary>
    private string ResolvePath(DatasetSpec spec)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : $"{spec.Dataset}.json";

        return Path.IsPathRooted(relative) ? relative : Path.Combine(baseDir, relative);
    }

    /// <summary>Same lookup pair as <see cref="CsvSource"/>'s: <see cref="ExtractColumns"/> tells
    /// no-contract-at-all from declared (native scan needs no contract), while this throwing variant
    /// serves <see cref="GetSchemaAsync"/>, which has nothing to infer a schema from without one.</summary>
    private static IReadOnlyDictionary<string, string> GetColumnsContract(DatasetSpec spec) =>
        ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': localfiles json requires a declared columns: contract for schema " +
            "fetch — NDJSON has no header row or footer to infer one from",
            isTransient: false);

    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) && value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
