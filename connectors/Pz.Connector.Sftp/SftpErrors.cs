using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;
using Renci.SshNet.Common;

namespace Pz.Connector.Sftp;

/// <summary>Transience classification and third-party error-text redaction. Network shapes
/// (connection drops, timeouts, IO) retry; auth, permission, and missing-path errors do not.
/// Redaction runs over every wrapped third-party message because PzConnectorException messages
/// land verbatim in run_results.json and the NDJSON event stream.</summary>
internal static partial class SftpErrors
{
    public static PzConnectorException Map(Exception ex, string context) =>
        new($"{context}: {Redact(ex.Message)}", IsTransient(ex), innerException: ex);

    public static bool IsTransient(Exception ex) => ex switch
    {
        SshAuthenticationException or SftpPermissionDeniedException or SftpPathNotFoundException => false,
        SshConnectionException or SshOperationTimeoutException or ProxyException => true,
        System.Net.Sockets.SocketException or IOException or TimeoutException => true,
        _ when ex.InnerException is not null => IsTransient(ex.InnerException),
        _ => false,
    };

    // name=value credential pairs (value up to the next delimiter) and the XML credential elements
    // real object-store rejections use. Key names only — never patterns over values, which would
    // false-positive on hostnames and paths.
    [GeneratedRegex(@"(?i)\b(password|passphrase|secret|secret_key|access_key|signature|token|key_id|awsaccesskeyid)\s*=\s*[^;,\s""'<&]+")]
    private static partial Regex NameValueSecret();

    [GeneratedRegex(@"(?is)<(AWSAccessKeyId|StringToSign|Signature|SecretAccessKey)>.*?</\1>")]
    private static partial Regex XmlSecretElement();

    public static string Redact(string text) =>
        NameValueSecret().Replace(
            XmlSecretElement().Replace(text, "<$1>***</$1>"),
            m => $"{m.Value[..m.Value.IndexOf('=')]}=***");
}
