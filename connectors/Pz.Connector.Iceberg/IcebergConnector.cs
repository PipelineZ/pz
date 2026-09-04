using Pz.Connectors.Abstractions;

[assembly: PzConnector("iceberg", typeof(Pz.Connector.Iceberg.IcebergConnector))]

namespace Pz.Connector.Iceberg;

/// <summary>Apache Iceberg source + sink connector — native-path-only. DuckDB's own `iceberg`
/// extension is the ENTIRE data plane: the engine's session attaches the catalog once per
/// connection and every read/write is a plain statement against that alias. The connection names
/// the catalog (an Iceberg REST catalog, AWS Glue, Amazon S3 Tables, or no catalog at all — a
/// `files` root of table directories, read-only) and its credentials; optional storage credentials
/// (S3-compatible keys, or an Azure auth method under `storage: azure`) cover the data files when
/// the catalog does not vend them. Zero drivers: `pz validate
/// --connect` verifies a REST endpoint by TCP reachability only and a local root by its directory,
/// and its schema fetch works only for datasets with a declared `columns:` contract. Registered
/// under the logical name "iceberg".</summary>
public sealed class IcebergConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    public ConnectorInfo Info => new("iceberg", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.Transactional |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """
        { "type": "object", "properties": {
          "catalog": { "enum": ["rest", "glue", "s3_tables", "files"] },
          "endpoint": { "type": "string" }, "warehouse": { "type": "string" },
          "token": { "type": "string" },
          "client_id": { "type": "string" }, "client_secret": { "type": "string" },
          "oauth2_server_uri": { "type": "string" }, "oauth2_scope": { "type": "string" },
          "nested_namespaces": { "type": "boolean" },
          "root": { "type": "string" },
          "storage_key_id": { "type": "string" }, "storage_secret_key": { "type": "string" },
          "storage_region": { "type": "string" }, "storage_endpoint": { "type": "string" },
          "storage_url_style": { "enum": ["vhost", "path"] }, "storage_use_ssl": { "type": "boolean" },
          "storage": { "enum": ["s3", "azure"] },
          "storage_auth": { "enum": ["connection_string", "account_key", "service_principal", "credential_chain"] },
          "storage_connection_string": { "type": "string" }, "storage_account_name": { "type": "string" },
          "storage_account_key": { "type": "string" }, "storage_tenant_id": { "type": "string" },
          "storage_client_id": { "type": "string" }, "storage_client_secret": { "type": "string" },
          "storage_chain": { "type": "string" }
        }, "additionalProperties": false }
        """;

    public string DatasetConfigSchema =>
        """
        { "type": "object", "properties": {
          "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } },
          "version": { "type": "integer", "minimum": 0 },
          "timestamp": { "type": "string" },
          "metadata_version": { "type": "string" }
        }, "additionalProperties": false }
        """;

    /// <summary>The root a RELATIVE <c>root</c> is normalized against when no <c>base_dir</c> is
    /// present. Config validation runs on the connection as the user wrote it, before the host
    /// injects the anchor, and a relative value is project-relative by definition -- so the
    /// containment question ("does this land under the project's .pz/?") is answerable without
    /// knowing where the project is, as long as both sides of the comparison share one stand-in
    /// root. A value that climbs out with <c>../</c> resolves above this root, matches nothing, and
    /// is none of this rule's business.</summary>
    private static readonly string StandInProjectRoot =
        Path.GetFullPath(Path.DirectorySeparatorChar + "pz-project");

    /// <summary>A local <c>root</c> may not live under the project's <c>.pz/</c>, the engine's own
    /// staging/state area. A relative value is checked against <see cref="StandInProjectRoot"/> so
    /// the rule fires on the pre-injection config tier-3 validation actually sees; an ABSOLUTE value
    /// is only comparable when the host did inject <c>base_dir</c>. An object-store root (a URL)
    /// cannot land under a local <c>.pz/</c> by construction.</summary>
    private static string? PzDirError(ConnectorConfig config)
    {
        if (config.GetString("root") is not { Length: > 0 } value || IcebergSql.IsUrl(value))
        {
            return null;
        }

        var baseDir = config.GetString("base_dir") is { Length: > 0 } injected ? injected : null;
        if (Path.IsPathRooted(value) && baseDir is null)
        {
            return null;
        }

        var root = baseDir ?? StandInProjectRoot;
        var resolved = Path.GetFullPath(Path.IsPathRooted(value) ? value : Path.Combine(root, value));
        var pzDir = Path.GetFullPath(Path.Combine(root, ".pz")) + Path.DirectorySeparatorChar;
        return resolved.StartsWith(pzDir, StringComparison.Ordinal)
            ? "iceberg connection 'root' resolves inside the project's .pz/ directory, which is pz's own " +
              "staging and state area -- point it outside .pz/"
            : null;
    }

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>(IcebergCatalog.Validate(config));
        if (IcebergCatalog.Of(config) == IcebergCatalog.Files && PzDirError(config) is { } rootError)
        {
            errors.Add(rootError);
        }

        return new(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = IcebergCatalog.Validate(config);
        if (errors.Count > 0)
        {
            return new(new ConnectionCheck(false, "permanent: " + errors[0]));
        }

        switch (IcebergCatalog.Of(config))
        {
            case IcebergCatalog.Rest:
                var endpoint = new Uri(config.GetString("endpoint")!, UriKind.Absolute);
                return IcebergProbe.TcpAsync(endpoint.Host, endpoint.Port, "iceberg rest catalog", ct);
            case IcebergCatalog.Files:
                var root = config.GetString("root")!;
                return IcebergSql.IsUrl(root)
                    ? new(new ConnectionCheck(true, "not checked: an object-store root has no offline probe; the first run reads it"))
                    : IcebergProbe.CheckDirectory(IcebergSql.ResolveLocal(config, "root"));
            default:
                return new(new ConnectionCheck(true, "not checked: an AWS catalog has no offline probe; the first run authenticates"));
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new IcebergSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new IcebergSink(config));
}
