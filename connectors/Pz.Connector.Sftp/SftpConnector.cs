using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

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
        """{ "type": "object", "required": ["host","username"], "properties": { "host": { "type": "string" }, "port": { "type": "integer" }, "username": { "type": "string" }, "password": { "type": "string" }, "private_key_path": { "type": "string" }, "private_key_passphrase": { "type": "string" }, "host_key_fingerprint": { "type": "string" }, "root": { "type": "string" }, "connect_timeout_seconds": { "type": "integer", "minimum": 1, "maximum": 3600 } }, "additionalProperties": false }""";

    // Strict source-dataset schema (the azureblob/s3 parity shape): unknown/typo'd options fail
    // `pz validate` with PZ0301 instead of being silently ignored. files_per_partition is genuinely
    // honored here (universal-tier reads), unlike the native-only file connectors. `format` comes
    // from the shared catalog (FileFormatCatalog.SchemaProperties), same as every other file-place
    // connector's schema.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "path": { "type": "string" }, """ + FileFormatCatalog.SchemaProperties +
        """, "columns": { "type": "object", "minProperties": 1, "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } }, "files_per_partition": { "type": ["integer","string"] } }, "additionalProperties": false }""";

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

        var declaredFingerprint = config.GetString("host_key_fingerprint");
        if (declaredFingerprint is { Length: > 0 } fp && !SftpConnectionSettings.IsValidFingerprint(fp))
        {
            errors.Add("sftp connection 'host_key_fingerprint' must be a SHA-256 fingerprint " +
                "('SHA256:<base64>' or the bare base64 body)");
        }

        if (errors.Count > 0)
        {
            return new ValueTask<ValidationResult>(ValidationResult.Failed([.. errors]));
        }

        // No pin at all (not merely an invalid one, already caught above): every host key is accepted
        // silently, with no MITM protection and no signal -- unless something says so. This never fails
        // validation (a first connect to an unknown host is a legitimate, common case); it only makes
        // the choice visible.
        List<string>? warnings = string.IsNullOrEmpty(declaredFingerprint)
            ? ["sftp connection accepts any SSH host key because 'host_key_fingerprint' is not set -- " +
                "no protection against a man-in-the-middle; run 'pz validate --connect' to see the " +
                "fingerprint the server presents, then pin it"]
            : null;

        return new ValueTask<ValidationResult>(new ValidationResult([], warnings));
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
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var settings = SftpConnectionSettings.Parse(config);
        var auth = SftpClientFactory.BuildAuth(settings);
        string? presentedFingerprint = null;
        try
        {
            using var fs = await SftpClientFactory.ConnectAsync(settings, auth, ct, fp => presentedFingerprint = fp)
                .ConfigureAwait(false);
            return ProbeRoot(fs, settings, presentedFingerprint);
        }
        catch (PzConnectorException ex)
        {
            return new ConnectionCheck(false, $"{(ex.IsTransient ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    /// <summary>The stat half of <see cref="CheckConnectionAsync"/>'s probe, split out so it can be
    /// exercised directly against a fake <see cref="ISftpFileSystem"/> -- the surrounding connect/auth
    /// round trip needs a live server, but the root-exists decision does not. <paramref
    /// name="presentedFingerprint"/> is the SHA-256 fingerprint the server presented during THIS
    /// connect (learned by <see cref="SftpClientFactory.ConnectAsync"/>'s host-key callback,
    /// regardless of whether a pin is declared); with no pin declared, a successful check surfaces it
    /// in the same <c>SHA256:&lt;base64&gt;</c> form <c>host_key_fingerprint</c> accepts, so pinning
    /// is copy-paste straight from `pz validate --connect` output.</summary>
    internal static ConnectionCheck ProbeRoot(
        ISftpFileSystem fs, SftpConnectionSettings settings, string? presentedFingerprint = null)
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

        if (!exists)
        {
            return new ConnectionCheck(false,
                $"permanent: sftp host '{settings.Host}': root '{root}' does not exist or is not a directory");
        }

        return settings.HostKeyFingerprint is null && presentedFingerprint is not null
            ? new ConnectionCheck(true,
                $"host_key_fingerprint is not pinned; the server presented SHA256:{presentedFingerprint} -- " +
                "pin it by setting host_key_fingerprint to this exact value")
            : new ConnectionCheck(true);
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SftpSource(SftpConnectionSettings.Parse(config), s => SftpClientFactory.Open(s)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SftpSink(SftpConnectionSettings.Parse(config), s => SftpClientFactory.Open(s)));
}
