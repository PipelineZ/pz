using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

[assembly: PzConnector("gcs", typeof(Pz.Connector.Gcs.GcsConnector))]

namespace Pz.Connector.Gcs;

/// <summary>Google Cloud Storage source + sink connector. The auth method selects the data plane
/// (see <see cref="GcsAuth"/>): <c>hmac</c> interop keys drive DuckDB's httpfs <c>gs://</c> tier —
/// native scan reads and native COPY writes, the s3 shapes — while <c>service_account</c>/<c>adc</c>
/// are OAuth-only, which DuckDB cannot speak, so they carry SDK-backed universal write sessions and
/// no reads at all. Reads are NATIVE-ONLY (<see cref="INativeOnlySource"/>): a source on a non-hmac
/// connection is refused at open with the fix in the message, which is what keeps "no read path"
/// from surfacing as a runtime mystery.</summary>
public sealed class GcsConnector : ISourceConnector, ISinkConnector, INativeOnlySource
{
    private static readonly string[] ValidUrlStyles = ["vhost", "path"];

    public ConnectorInfo Info => new("gcs", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.PathTemplating |
        ConnectorCapabilities.GatedOperations;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["auth"], "properties": { "auth": { "enum": ["hmac","service_account","adc"] }, "key_id": { "type": "string" }, "secret": { "type": "string" }, "key_file": { "type": "string" }, "key_json": { "type": "string" }, "root": { "type": "string" }, "endpoint": { "type": "string" }, "url_style": { "enum": ["vhost","path"] }, "use_ssl": { "type": "boolean" } }, "additionalProperties": false }""";

    // Strict source-dataset schema (the s3/azureblob parity shape): unknown/typo'd options fail
    // `pz validate` with PZ0301 instead of being silently ignored. Mirrors what
    // GcsSource.ResolveLocation actually reads: bucket + path both optional (the connection `root:`
    // and the `<entity>.<format>` default fill the gaps at probe time), format (parquet default),
    // the generic columns: contract. files_per_partition is deliberately ACCEPTED (int-or-string) so
    // the plan-time PZ0312 refusal on this native-only source keeps owning that case with its
    // targeted message. Sink OUTPUT options stay plan/probe-validated by GcsSink — tier 3 never
    // evaluates output options.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "bucket": { "type": "string" }, "path": { "type": "string" }, """ + FileFormatCatalog.SchemaProperties +
        """, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } }, "files_per_partition": { "type": ["integer","string"] } }, "additionalProperties": false }""";

    /// <summary>Offline cross-field validation: the auth matrix's required fields (aggregate), plus
    /// <c>url_style</c> — when given — must be one of DuckDB's two accepted values. Never touches
    /// the network.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>(GcsAuth.Validate(config).Errors);

        var urlStyle = config.GetString("url_style");
        if (urlStyle is not null && !ValidUrlStyles.Contains(urlStyle))
        {
            errors.Add($"gcs connection 'url_style' must be one of 'vhost', 'path' (got '{urlStyle}')");
        }

        return new ValueTask<ValidationResult>(
            errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    /// <summary>No deep probe in v0 (the s3 precedent): the native COPY / SDK upload either succeeds
    /// or fails at run time, and that failure is already reported (PZ0311) with the same clarity a
    /// dedicated connectivity check would add. Always reports Ok — a real online probe arrives with
    /// `pz validate --connect` in a later version.</summary>
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true,
            "gcs connectivity is verified at run time; deep probe arrives with pz validate --connect in a later version"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        GcsAuth.IsHmac(config)
            ? new(new GcsSource(config))
            : throw new PzConnectorException(
                "gcs sources require 'auth: hmac' -- DuckDB's native gs:// scan authenticates with HMAC " +
                "interop keys only; create HMAC keys for the service account (Cloud Storage -> Settings -> " +
                "Interoperability) or keep this connection write-only", isTransient: false);

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new GcsSink(config));
}
