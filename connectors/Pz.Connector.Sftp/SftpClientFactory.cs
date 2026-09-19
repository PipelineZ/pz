using System.Security.Cryptography;
using Pz.Connectors.Abstractions;
using Renci.SshNet;
using Renci.SshNet.Common;

namespace Pz.Connector.Sftp;

/// <summary>Builds a connected, authenticated SftpClient from parsed settings. Host-key policy:
/// a declared fingerprint pin is verified (SHA-256 over the presented key blob, OpenSSH base64
/// form); with no pin, any host key is accepted. The mismatch message carries both fingerprints —
/// fingerprints are public values, never key material.</summary>
internal static class SftpClientFactory
{
    /// <summary>Synchronous convenience wrapper over <see cref="ConnectAsync"/> for call sites (test
    /// fixture seeding, xunit constructors) that cannot await. Never used by the connector's own
    /// probe -- <c>SftpConnector.CheckConnectionAsync</c> calls <see cref="ConnectAsync"/> directly so
    /// it can honor cancellation and observe <c>connect_timeout_seconds</c> promptly.</summary>
    public static ISftpFileSystem Open(SftpConnectionSettings settings) =>
        ConnectAsync(settings, BuildAuth(settings), CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>The connect-and-authenticate half of <see cref="Open"/>, split out so
    /// <c>CheckConnectionAsync</c> can call <see cref="BuildAuth"/> on its own first -- config-shape
    /// failures (neither auth method, or an unreadable/wrong-passphrase key file) surface before any
    /// network attempt and must propagate uncaught, while everything this method can throw is a
    /// genuine connect/auth outcome. <paramref name="auth"/> is disposed on every path out of this
    /// method that does not hand it to a live <see cref="SftpFileSystem"/> -- the caller no longer
    /// owns it once this is called.
    ///
    /// Cancelling <paramref name="ct"/> propagates as a plain <see cref="OperationCanceledException"/>,
    /// never wrapped into a <see cref="PzConnectorException"/> -- SSH.NET's own
    /// <c>BaseClient.ConnectAsync</c> links <paramref name="ct"/> with a timeout derived from
    /// <see cref="SftpConnectionSettings.ConnectTimeoutSeconds"/> (absent: SSH.NET's own 30s default,
    /// <see cref="ConnectionInfo"/>'s own unchanged behavior) and only the TIMEOUT half turns into
    /// <see cref="SshOperationTimeoutException"/>, which <see cref="SftpErrors"/> classifies as
    /// transient like any other connectivity outcome.</summary>
    internal static async Task<ISftpFileSystem> ConnectAsync(
        SftpConnectionSettings settings, SftpAuth auth, CancellationToken ct, Action<string>? onHostKey = null)
    {
        var client = new SftpClient(BuildConnectionInfo(settings, auth.Method));

        string? mismatch = null;
        client.HostKeyReceived += (_, e) =>
        {
            var presented = Convert.ToBase64String(SHA256.HashData(e.HostKey)).TrimEnd('=');
            onHostKey?.Invoke(presented);
            if (settings.HostKeyFingerprint is null)
            {
                e.CanTrust = true;
                return;
            }

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
            await client.ConnectAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The caller's ct, cancelled: BaseClient.ConnectAsync itself only turns its OWN internal
            // connect-timeout window into SshOperationTimeoutException (a genuine connectivity outcome,
            // handled by the classified-exception path below) -- an OperationCanceledException reaching
            // here can only be the caller's own token, and must propagate unwrapped.
            client.Dispose();
            auth.Dispose();
            throw;
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

    /// <summary>Pure, network-free: <paramref name="settings"/>.<see
    /// cref="SftpConnectionSettings.ConnectTimeoutSeconds"/> absent leaves SSH.NET's own
    /// <see cref="ConnectionInfo"/> default (30s) untouched -- unchanged behavior for a connection that
    /// declares no override.</summary>
    internal static ConnectionInfo BuildConnectionInfo(SftpConnectionSettings settings, AuthenticationMethod method)
    {
        var info = new ConnectionInfo(settings.Host, settings.Port, settings.Username, method);
        if (settings.ConnectTimeoutSeconds is { } timeoutSeconds)
        {
            info.Timeout = TimeSpan.FromSeconds(timeoutSeconds);
        }

        return info;
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
/// passes to whichever of <see cref="SftpClientFactory.ConnectAsync"/>'s outcomes ends up responsible
/// for it: the catch block on a failed connect, or the resulting <see cref="SftpFileSystem"/> on
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
