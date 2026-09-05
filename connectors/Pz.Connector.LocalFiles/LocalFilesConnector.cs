using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

[assembly: PzConnector("localfiles", typeof(Pz.Connector.LocalFiles.LocalFilesConnector))]

namespace Pz.Connector.LocalFiles;

/// <summary>Local filesystem connector: CSV source via Sylvan.Data.Csv, parquet/CSV sink via
/// Parquet.Net + a minimal RFC-4180 writer, json (NDJSON) in both directions via the shared toolkit
/// codec, and xlsx/avro native-only through DuckDB's excel/avro extensions (<see cref="NativeOnlySource"/>,
/// which also carries json's native-only <c>read_json</c> read). Registered under the logical name
/// "localfiles". Relative dataset/output <c>path</c>s resolve against the connection option
/// <c>base_dir</c> — <c>RunCommand</c> injects the project directory there (see
/// <see cref="Pz.Cli.BuiltinConnectors"/>), keeping this connector itself unaware of the project
/// layout. Also declares <see cref="ConnectorCapabilities.BoundedWindow"/> --
/// <see cref="CsvSource.TryGetNativeScan"/>/<see cref="ParquetSource.TryGetNativeScan"/> push
/// <see cref="DatasetSpec.WatermarkUpperBound"/> down into the native-scan fragment via
/// <see cref="LocalFilesWindowSql"/>; the universal (non-native) read path does not, so a windowed
/// LocalFiles dataset requires the native tier (see <see cref="CsvPartition.ReadAsync"/>'s doc comment).</summary>
public sealed class LocalFilesConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("localfiles", "0.1.0", ProtocolVersion.Major);

    /// <summary><see cref="ConnectorCapabilities.PartitionedRead"/> is declared because
    /// <see cref="CsvSource.PlanReadAsync"/> may return one partition per byte range of a large csv
    /// file (see <see cref="CsvSplitPlan"/>). Declaring it is what makes that honest to the planner; it
    /// costs nothing here, since the only rule keyed on the flag refuses a sync-state (`feed`) dataset,
    /// and this connector has no feed shape to declare (it implements no
    /// <c>INaturalReadShapeSource</c>, so every dataset resolves to `full`).</summary>
    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.BoundedWindow |
        ConnectorCapabilities.PartitionedRead;

    // base_dir is injected internally by RunCommand and never user-written; `root` is the only
    // user-facing connection option.
    public string ConnectionConfigSchema =>
        """{ "type": "object", "properties": { "root": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "path": { "type": "string" }, """ + FileFormatCatalog.SchemaProperties +
        """, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new LocalFilesSource(ResolveBaseDir(config)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new LocalFilesSink(ResolveBaseDir(config)));

    /// <summary><c>base_dir</c> is where the PROJECT is (the CLI injects it; never user-written), and
    /// <c>root</c> is where the author says this connection's data lives — relative to the project, or
    /// absolute. Path.Combine gives the absolute case for free.</summary>
    private static string ResolveBaseDir(ConnectorConfig config)
    {
        var baseDir = config.GetString("base_dir") ?? Directory.GetCurrentDirectory();
        return config.GetString("root") is { Length: > 0 } root ? Path.Combine(baseDir, root) : baseDir;
    }
}

/// <summary>Format dispatcher: <see cref="ISourceConnector.OpenAsync"/> is connector-
/// (not dataset-) scoped, so it cannot pick <see cref="CsvSource"/> vs <see cref="ParquetSource"/> vs
/// <see cref="NativeOnlySource"/> up front -- every <see cref="ISource"/> method receives its own
/// <see cref="DatasetSpec"/>, which is where <c>format:</c> actually lives. This holds one instance of
/// each and forwards per call: <c>format: parquet</c> routes to <see cref="ParquetSource"/>;
/// <c>format: csv</c>/<c>tsv</c> or absent (the default) routes to <see cref="CsvSource"/>; everything
/// else -- json, xlsx, avro -- routes to <see cref="NativeOnlySource"/>. <see
/// cref="FileFormatCatalog.Resolve"/> owns the "unsupported format" error for anything else.</summary>
internal sealed class LocalFilesSource(string baseDir) : ISource
{
    private readonly CsvSource _csv = new(baseDir);
    private readonly ParquetSource _parquet = new(baseDir);
    private readonly NativeOnlySource _native = new(baseDir);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        Resolve(spec).GetSchemaAsync(spec, ct);

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan) =>
        Resolve(spec).TryGetNativeScan(spec, out scan);

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        Resolve(spec).PlanReadAsync(spec, hints, ct);

    public ValueTask DisposeAsync() => default;

    private ISource Resolve(DatasetSpec spec)
    {
        var format = FileFormatCatalog.Resolve(spec.Options, "csv", "localfiles", $"dataset '{spec.Dataset}'");
        return format.Name switch
        {
            "parquet" => _parquet,
            "csv" or "tsv" => _csv,
            _ => _native,
        };
    }
}
