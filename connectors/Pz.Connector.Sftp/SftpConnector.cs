using Pz.Connectors.Abstractions;

[assembly: PzConnector("sftp", typeof(Pz.Connector.Sftp.SftpConnector))]

namespace Pz.Connector.Sftp;

/// <summary>SFTP source + sink connector, universal-Arrow-tier only in both directions: DuckDB has
/// no SFTP filesystem, so SSH.NET streams remote csv/parquet/json files through managed format
/// readers (source) and the shared toolkit codecs (sink). One partition per matched remote file;
/// windowed datasets are honored row-level (see SftpWindowFilter) — which is what lets a connector
/// with no native tier declare BoundedWindow. Registered under the logical name "sftp".</summary>
public sealed class SftpConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("sftp", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.PathTemplating |
        ConnectorCapabilities.GatedOperations;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["host","username"], "properties": { "host": { "type": "string" }, "port": { "type": "integer" }, "username": { "type": "string" }, "password": { "type": "string" }, "private_key_path": { "type": "string" }, "private_key_passphrase": { "type": "string" }, "host_key_fingerprint": { "type": "string" }, "root": { "type": "string" } }, "additionalProperties": false }""";

    // Strict source-dataset schema (the azureblob/s3 parity shape): unknown/typo'd options fail
    // `pz validate` with PZ0301 instead of being silently ignored. files_per_partition is genuinely
    // honored here (universal-tier reads), unlike the native-only file connectors.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "path": { "type": "string" }, "format": { "enum": ["csv","parquet","json"] }, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } }, "files_per_partition": { "type": ["integer","string"] } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        if (string.IsNullOrEmpty(config.GetString("host")))
        {
            errors.Add("sftp connection requires 'host'");
        }

        if (string.IsNullOrEmpty(config.GetString("username")))
        {
            errors.Add("sftp connection requires 'username'");
        }

        var hasPassword = !string.IsNullOrEmpty(config.GetString("password"));
        var hasKey = !string.IsNullOrEmpty(config.GetString("private_key_path"));
        if (!hasPassword && !hasKey)
        {
            errors.Add("sftp connection requires 'password' or 'private_key_path'");
        }
        else if (hasPassword && hasKey)
        {
            errors.Add("sftp connection must declare exactly one of 'password' and 'private_key_path', not both");
        }

        if (!hasKey && !string.IsNullOrEmpty(config.GetString("private_key_passphrase")))
        {
            errors.Add("sftp connection 'private_key_passphrase' requires 'private_key_path'");
        }

        if (config.GetInt("port") is { } port and (< 1 or > 65535))
        {
            errors.Add($"sftp connection 'port' must be 1-65535 (got {port})");
        }

        if (config.GetString("host_key_fingerprint") is { Length: > 0 } fp &&
            !SftpConnectionSettings.IsValidFingerprint(fp))
        {
            errors.Add("sftp connection 'host_key_fingerprint' must be a SHA-256 fingerprint " +
                "('SHA256:<base64>' or the bare base64 body)");
        }

        return new ValueTask<ValidationResult>(
            errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    // Real probe lands in Task 10 alongside registration (needs SftpClientFactory from Task 2).
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SftpSource(SftpConnectionSettings.Parse(config), s => SftpClientFactory.Open(s)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();  // Task 8
}
