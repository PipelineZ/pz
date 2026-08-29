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
    public static ISftpFileSystem Open(SftpConnectionSettings settings)
    {
        var auth = BuildAuth(settings);
        var info = new ConnectionInfo(settings.Host, settings.Port, settings.Username, auth);
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

        return new SftpFileSystem(client);
    }

    private static AuthenticationMethod BuildAuth(SftpConnectionSettings s)
    {
        if (s.Password is not null)
        {
            return new PasswordAuthenticationMethod(s.Username, s.Password);
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
            return new PrivateKeyAuthenticationMethod(s.Username, key);
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
