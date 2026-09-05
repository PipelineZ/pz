using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.LocalFiles;

/// <summary>Native-only source for the formats DuckDB reads and nothing managed does here: json
/// (<c>read_json</c>), xlsx (<c>read_xlsx</c>, excel extension) and avro (<c>read_avro</c>, avro
/// extension) -- mirroring azure's json read shape (<c>AzureSource</c>) and the
/// <see cref="ParquetSource"/> native-only precedent: there is no managed reader for any of the three
/// in v0, so <see cref="PlanReadAsync"/> always throws. The two-state contract model matches
/// <see cref="CsvSource.TryGetNativeScan"/>: a declared `columns:` contract -- partial or full --
/// prunes/casts the read to exactly its declared columns (a strict <c>columns = {…}</c> map for json,
/// a projecting cast for xlsx/avro, which take no <c>columns=</c> parameter of their own); no contract
/// at all goes through DuckDB's own typing (json's <c>auto_detect</c>, xlsx/avro's native inference).
/// The contract IS the schema: <see cref="GetSchemaAsync"/> answers from it or refuses -- none of the
/// three formats gives schema fetch a header row or footer to read without one.</summary>
internal sealed class NativeOnlySource(string baseDir) : ISource
{
    /// <summary>No file bytes are read here at all beyond the existence check -- the declared
    /// `columns:` contract IS the schema (the azure json precedent, generalised to xlsx/avro).</summary>
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", context);
        var path = ResolvePath(spec, format);
        if (!File.Exists(path))
        {
            throw new PzConnectorException($"{format.Name} file not found: '{path}'", isTransient: false);
        }

        var columns = GetColumnsContract(spec, format);
        var fields = columns.Select(kv => TypeNameMap.ToArrowField(kv.Key, kv.Value)).ToArray();
        return new(new DatasetSchema(new Schema(fields, null)));
    }

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        var context = $"dataset '{spec.Dataset}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", context);
        var absPath = ResolvePath(spec, format);
        var declared = ExtractColumns(spec);
        var urlArg = $"'{EscapeSqlLiteral(absPath)}'";
        var request = new FormatReadRequest(urlArg, 1, declared, TypeNameMap.ToDuckDbName);
        var fragment = FileFormatCatalog.ReadFragment(format, spec.Options, request, context);
        scan = new NativeScan(LocalFilesWindowSql.Wrap(fragment, spec), FileFormatCatalog.SetupStatements(format))
        {
            Mechanism = FileFormatCatalog.ReadMechanism(format),
            SchemaInferred = FileFormatCatalog.SchemaInferred(format, declared),
        };
        return true;
    }

    /// <summary>The universal path: like <see cref="ParquetSource.PlanReadAsync"/>, there is no managed
    /// reader for json/xlsx/avro in v0, so this always throws -- reached whenever the planner has no
    /// native strategy for this edge, including under <c>engine.force_universal</c>. Same PZ0312
    /// bare-string convention (and the same literal-duplication drift risk) documented on the parquet
    /// counterpart.</summary>
    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", $"dataset '{spec.Dataset}'");
        throw new PzConnectorException(
            $"PZ0312: dataset '{spec.Dataset}': localfiles {format.Name} source is native-scan only; it cannot run " +
            "on the universal tier (remove engine.force_universal)", isTransient: false);
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>The entity names the file, and <c>path:</c> overrides that when the layout does not
    /// match the name. A source needs the extension its format implies; the sink writes a directory, so
    /// it does not. An absolute <c>path:</c> ignores the connection's location entirely.</summary>
    private string ResolvePath(DatasetSpec spec, FileFormat format)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : $"{spec.Dataset}.{format.Extension}";

        return Path.IsPathRooted(relative) ? relative : Path.Combine(baseDir, relative);
    }

    /// <summary>Same lookup pair as <see cref="CsvSource"/>'s: <see cref="ExtractColumns"/> tells
    /// no-contract-at-all from declared (native scan needs no contract), while this throwing variant
    /// serves <see cref="GetSchemaAsync"/>, which has nothing to infer a schema from without one.</summary>
    private static IReadOnlyDictionary<string, string> GetColumnsContract(DatasetSpec spec, FileFormat format) =>
        ExtractColumns(spec) ?? throw new PzConnectorException(
            $"dataset '{spec.Dataset}': localfiles {format.Name} requires a declared columns: contract for " +
            $"schema fetch -- {SchemaFetchReason(format.Name)}",
            isTransient: false);

    /// <summary>Why none of the three formats gives schema fetch anything to infer a schema from
    /// without a declared contract -- json's wording is unchanged from before this source generalised
    /// beyond json (<c>JsonFormatTests</c> pins it).</summary>
    private static string SchemaFetchReason(string formatName) => formatName switch
    {
        "json" => "NDJSON has no header row or footer to infer one from",
        "xlsx" => "a workbook's header row names columns but not their types",
        "avro" => "avro's embedded schema is not read here -- schema fetch never opens the file bytes",
        _ => "there is no header row or footer to infer one from",
    };

    private static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) && value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
