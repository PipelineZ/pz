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

    /// <summary>Real probe: connects, authenticates, then stats the root (or login directory) --
    /// proving connect + auth + basic access in one round trip. A stat, not a listing: `ListFiles`
    /// treats a missing directory as "no entries" (the no-match error belongs to the dataset-aware
    /// caller, not here), so listing a wrong `root:` would silently report the connection healthy --
    /// this must instead see the missing root and fail. <see cref="SftpClientFactory.BuildAuth"/>'s
    /// failures (neither auth method declared, or a key file that fails to load) are config-shape
    /// errors discovered before any network attempt; mirroring AzureConnector's ConnectivityValidator
    /// convention, they are deliberately called outside the try below and THROW rather than fold into
    /// a false ConnectionCheck. Everything the try can throw -- connect, auth, and the stat itself --
    /// is a genuine connectivity outcome, folded into the message with the transient/permanent tag
    /// (the Azure/Postgres convention; ConnectionCheck carries no separate transience field).</summary>
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var settings = SftpConnectionSettings.Parse(config);
        var auth = SftpClientFactory.BuildAuth(settings);
        try
        {
            using var fs = SftpClientFactory.Connect(settings, auth);
            return new ValueTask<ConnectionCheck>(ProbeRoot(fs, settings));
        }
        catch (PzConnectorException ex)
        {
            return new ValueTask<ConnectionCheck>(new ConnectionCheck(false,
                $"{(ex.IsTransient ? "transient" : "permanent")}: {ex.Message}"));
        }
    }

    /// <summary>The stat half of <see cref="CheckConnectionAsync"/>'s probe, split out so it can be
    /// exercised directly against a fake <see cref="ISftpFileSystem"/> -- the surrounding connect/auth
    /// round trip needs a live server, but the root-exists decision does not.</summary>
    internal static ConnectionCheck ProbeRoot(ISftpFileSystem fs, SftpConnectionSettings settings)
    {
        var root = settings.Root ?? ".";
        bool exists;
        try
        {
            exists = fs.DirectoryExists(root);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            // Same classify-any-raw-exception convention SftpGate/SftpSource use for a
            // mid-operation SSH.NET failure.
            var mapped = SftpErrors.Map(ex, $"sftp host '{settings.Host}': stat failed");
            return new ConnectionCheck(false, $"{(mapped.IsTransient ? "transient" : "permanent")}: {mapped.Message}");
        }

        return exists
            ? new ConnectionCheck(true)
            : new ConnectionCheck(false,
                $"permanent: sftp host '{settings.Host}': root '{root}' does not exist or is not a directory");
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SftpSource(SftpConnectionSettings.Parse(config), s => SftpClientFactory.Open(s)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SftpSink(SftpConnectionSettings.Parse(config), s => SftpClientFactory.Open(s)));
}
