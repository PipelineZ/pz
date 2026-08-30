using System.Security.Cryptography;
using Pz.Connectors.Abstractions;
using Renci.SshNet;

namespace Pz.Connector.Sftp;

/// <summary>Builds a connected, authenticated SftpClient from parsed settings. Host-key policy:
/// a declared fingerprint pin is verified (SHA-256 over the presented key blob, OpenSSH base64
/// form); with no pin, any host key is accepted. The mismatch message carries both fingerprints —
/// fingerprints are public values, never key material.</summary>
internal static class SftpClientFactory
{
    public static ISftpFileSystem Open(SftpConnectionSettings settings) => Connect(settings, BuildAuth(settings));

    /// <summary>The connect-and-authenticate half of <see cref="Open"/>, split out so
    /// <c>CheckConnectionAsync</c> can call <see cref="BuildAuth"/> on its own first -- config-shape
    /// failures (neither auth method, or an unreadable/wrong-passphrase key file) surface before any
    /// network attempt and must propagate uncaught, while everything this method can throw is a
    /// genuine connect/auth outcome. <paramref name="auth"/> is disposed on every path out of this
    /// method that does not hand it to a live <see cref="SftpFileSystem"/> -- the caller no longer
    /// owns it once this is called.</summary>
    internal static ISftpFileSystem Connect(SftpConnectionSettings settings, SftpAuth auth)
    {
        var info = new ConnectionInfo(settings.Host, settings.Port, settings.Username, auth.Method);
        var client = new SftpClient(info);

        string? mismatch = null;
        client.HostKeyReceived += (_, e) =>
        {
            if (settings.HostKeyFingerprint is null)
            {
                e.CanTrust = true;
                return;
            }

            var presented = Convert.ToBase64String(SHA256.HashData(e.HostKey)).TrimEnd('=');
            if (presented == settings.HostKeyFingerprint)
            {
                e.CanTrust = true;
            }
            else
            {
                mismatch = presented;
                e.CanTrust = false;
            }
        };

        try
        {
            client.Connect();
        }
        catch (Exception ex)
        {
            client.Dispose();
            auth.Dispose();
            if (mismatch is not null)
            {
                throw new PzConnectorException(
                    $"sftp host '{settings.Host}': host key fingerprint mismatch — expected " +
                    $"SHA256:{settings.HostKeyFingerprint}, server presented SHA256:{mismatch}; update " +
                    "'host_key_fingerprint' only after verifying the server's key out of band",
                    isTransient: false, innerException: ex);
            }

            throw SftpErrors.Map(ex, $"sftp host '{settings.Host}': connect failed");
        }

        return new SftpFileSystem(client, auth);
    }

    /// <summary>Password auth needs nothing beyond the <see cref="PasswordAuthenticationMethod"/>
    /// itself. Key auth additionally constructs a <see cref="PrivateKeyFile"/>, which SSH.NET's
    /// <see cref="PrivateKeyAuthenticationMethod"/> holds a reference to but -- verified against the
    /// SSH.NET 2026.0.0 source -- never disposes on its own <c>Dispose()</c>; the key file (and the
    /// decrypted key material it holds) would leak once per connection without <see cref="SftpAuth"/>
    /// bundling it in explicitly.</summary>
    internal static SftpAuth BuildAuth(SftpConnectionSettings s)
    {
        if (s.Password is not null)
        {
            return new SftpAuth(new PasswordAuthenticationMethod(s.Username, s.Password), key: null);
        }

        // ValidateAsync rejects a config with neither auth method, but a directly-constructed
        // settings record can skip that gate — this is the only backstop before PrivateKeyPath
        // gets dereferenced below, and it must fire before the try/catch turns a null-path failure
        // into a misleading "cannot load private key 'null'" message.
        if (s.PrivateKeyPath is null)
        {
            throw new PzConnectorException(
                "sftp connection requires 'password' or 'private_key_path'", isTransient: false);
        }

        try
        {
            var key = s.PrivateKeyPassphrase is null
                ? new PrivateKeyFile(s.PrivateKeyPath)
                : new PrivateKeyFile(s.PrivateKeyPath, s.PrivateKeyPassphrase);
            return new SftpAuth(new PrivateKeyAuthenticationMethod(s.Username, key), key);
        }
        catch (Exception ex)
        {
            // Config-shape error (unreadable or undecryptable key file), not connectivity. The
            // message names the path, never anything from the file's contents.
            throw new PzConnectorException(
                $"sftp connection: cannot load private key '{s.PrivateKeyPath}' " +
                "(unreadable file or wrong passphrase)", isTransient: false, innerException: ex);
        }
    }
}

/// <summary>Bundles a built <see cref="AuthenticationMethod"/> with the <see cref="PrivateKeyFile"/>
/// it wraps for key auth (null for password auth) so both get disposed together. Needed because
/// <see cref="PrivateKeyAuthenticationMethod"/> does not dispose the key sources it was constructed
/// with -- disposing only the auth method would leak the key file's decrypted material. Ownership
/// passes to whichever of <see cref="SftpClientFactory.Connect"/>'s outcomes ends up responsible for
/// it: the catch block on a failed connect, or the resulting <see cref="SftpFileSystem"/> on
/// success.</summary>
internal sealed class SftpAuth(AuthenticationMethod method, IDisposable? key) : IDisposable
{
    public AuthenticationMethod Method { get; } = method;

    public void Dispose()
    {
        Method.Dispose();
        key?.Dispose();
    }
}
