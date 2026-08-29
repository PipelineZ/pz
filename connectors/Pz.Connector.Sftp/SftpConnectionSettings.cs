using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sftp;

/// <summary>Parsed connection options. <see cref="Parse"/> re-checks only the presence of
/// host/username (throwing the same clear permanent error for a directly-constructed config);
/// auth-method exclusivity, port range, and fingerprint shape are ValidateAsync's job and are
/// not re-verified here.</summary>
internal sealed record SftpConnectionSettings(
    string Host, int Port, string Username, string? Password,
    string? PrivateKeyPath, string? PrivateKeyPassphrase, string? HostKeyFingerprint, string? Root)
{
    public static SftpConnectionSettings Parse(ConnectorConfig config) => new(
        Require(config, "host"),
        (int)(config.GetInt("port") ?? 22),
        Require(config, "username"),
        config.GetString("password"),
        config.GetString("private_key_path"),
        config.GetString("private_key_passphrase"),
        NormalizeFingerprint(config.GetString("host_key_fingerprint")),
        config.GetString("root"));

    /// <summary>Canonical comparison form: the OpenSSH "SHA256:" prefix and base64 '=' padding are
    /// both presentation, so both are stripped before storing/comparing.</summary>
    internal static string? NormalizeFingerprint(string? raw)
    {
        if (raw is not { Length: > 0 })
        {
            return null;
        }

        var body = raw.StartsWith("SHA256:", StringComparison.OrdinalIgnoreCase) ? raw[7..] : raw;
        return body.TrimEnd('=');
    }

    /// <summary>A SHA-256 base64 body is 43 chars unpadded, from the base64 alphabet.</summary>
    internal static bool IsValidFingerprint(string raw)
    {
        var body = NormalizeFingerprint(raw)!;
        return body.Length == 43 && body.All(c => char.IsAsciiLetterOrDigit(c) || c is '+' or '/');
    }

    private static string Require(ConnectorConfig config, string key) =>
        config.GetString(key) is { Length: > 0 } s
            ? s
            : throw new PzConnectorException($"sftp connection requires '{key}'", isTransient: false);
}
