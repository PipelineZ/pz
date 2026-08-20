using Pz.Connectors.Abstractions;

[assembly: PzConnector("s3", typeof(Pz.Connector.S3.S3Connector))]

namespace Pz.Connector.S3;

/// <summary>S3 object-store source + sink connector, native-only in both directions: DuckDB's httpfs
/// extension is the entire data plane, and the connector is deliberately SDK-free. Writes are a DuckDB
/// COPY with a scoped CREATE SECRET; reads are
/// <c>read_parquet</c>/<c>read_csv</c>/<c>read_json</c> native scans with the azure two-state contract
/// model, windowed-dataset wrapping, and the date-template watermark cover — see <see cref="S3Source"/>.
/// The SDK-free control-plane cost follows the MySQL/sqlite precedent: `pz validate --connect`'s
/// schema fetch answers only from a declared `columns:` contract. Registered under the logical name
/// "s3"; GCS is reachable via the `endpoint` override (https://pipelinez.dev/how-to/gcs/).</summary>
public sealed class S3Connector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    private static readonly string[] ValidUrlStyles = ["vhost", "path"];

    public ConnectorInfo Info => new("s3", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.PathTemplating;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["access_key","secret_key"], "properties": { "root": { "type": "string" }, "access_key": { "type": "string" }, "secret_key": { "type": "string" }, "region": { "type": "string" }, "endpoint": { "type": "string" }, "url_style": { "enum": ["vhost","path"] }, "use_ssl": { "type": "boolean" } }, "additionalProperties": false }""";

    // Strict source-dataset schema (the azureblob parity shape): unknown/typo'd options fail
    // `pz validate` with PZ0301 instead of being silently
    // ignored. Mirrors what S3Source.ResolveLocation actually reads: bucket + path both optional
    // (the connection `root:` and the `<entity>.<format>` default fill the gaps at probe time),
    // format (parquet default), the generic columns: contract. files_per_partition is deliberately
    // ACCEPTED (int-or-string) so the plan-time PZ0312 refusal on this native-only source keeps
    // owning that case with its targeted message. Sink OUTPUT options stay plan/probe-validated by
    // S3Sink — tier 3 never evaluates output options.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "bucket": { "type": "string" }, "path": { "type": "string" }, "format": { "enum": ["csv","parquet","json"] }, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } }, "files_per_partition": { "type": ["integer","string"] } }, "additionalProperties": false }""";

    /// <summary>Offline cross-field validation: both credential fields must be present,
    /// and <c>url_style</c> — when given — must be one of DuckDB's two accepted values. Never touches
    /// the network; deep connectivity is <see cref="CheckConnectionAsync"/>'s job (deferred, see there).</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();

        if (string.IsNullOrEmpty(config.GetString("access_key")))
        {
            errors.Add("s3 connection requires 'access_key'");
        }

        if (string.IsNullOrEmpty(config.GetString("secret_key")))
        {
            errors.Add("s3 connection requires 'secret_key'");
        }

        var urlStyle = config.GetString("url_style");
        if (urlStyle is not null && !ValidUrlStyles.Contains(urlStyle))
        {
            errors.Add($"s3 connection 'url_style' must be one of 'vhost', 'path' (got '{urlStyle}')");
        }

        return new ValueTask<ValidationResult>(
            errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    /// <summary>No deep probe in v0: the native COPY path either succeeds or fails at run
    /// time, and that failure is already reported (PZ0311) with the same clarity a dedicated connectivity
    /// check would add. Always reports Ok — a real online probe arrives with `pz validate --connect` in a
    /// later version.</summary>
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true,
            "s3 connectivity is verified at run time (native COPY); deep probe arrives with pz validate --connect in a later version"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new S3Source(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new S3Sink(config));
}
