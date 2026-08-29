using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;
using Renci.SshNet;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Two behaviors the source/sink acceptance suites never exercise, because both need a raw SSH
/// connection outside the connector under test: (1) key-only authentication actually round-trips through
/// the real <see cref="SftpConnector"/> (every acceptance-suite fact authenticates with a password);
/// (2) <c>host_key_fingerprint</c> pinning accepts a match and rejects a mismatch, permanently, with a
/// message naming both fingerprints and no password.</summary>
[Collection("sftp")]
public sealed class SftpKeyAuthAndHostKeyPinTests(SftpContainerFixture fixture)
{
    /// <summary>Seeds a file as <see cref="SftpContainerFixture.KeyUser"/> (that user has no password —
    /// the ONLY way onto their chroot is the key itself), then reads it back through the real
    /// <see cref="SftpConnector"/> configured with <c>private_key_path</c> — the round trip proves the
    /// connector's own key-auth wiring, not just SSH.NET's.</summary>
    [SkippableFact]
    public async Task Key_auth_round_trip_lists_and_reads_a_seeded_file()
    {
        DockerFacts.SkipUnlessDocker();

        var keySettings = new SftpConnectionSettings(
            fixture.Host, fixture.Port, SftpContainerFixture.KeyUser, Password: null,
            fixture.PrivateKeyPath, PrivateKeyPassphrase: null, HostKeyFingerprint: null, Root: null);
        using (var seedFs = SftpClientFactory.Open(keySettings))
        {
            seedFs.CreateDirectories("upload");
            using var stream = seedFs.OpenWrite("upload/keyauth.csv");
            var bytes = Encoding.UTF8.GetBytes("id,name\n1,alice\n2,bob\n");
            stream.Write(bytes, 0, bytes.Length);
        }

        var connector = new SftpConnector();
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = fixture.Host,
            ["port"] = fixture.Port,
            ["username"] = SftpContainerFixture.KeyUser,
            ["private_key_path"] = fixture.PrivateKeyPath,
            ["root"] = "upload",
        });

        await using var source = await ((ISourceConnector)connector).OpenAsync(config, CancellationToken.None);
        var spec = new DatasetSpec("sftp", "keyauth", new Dictionary<string, object?>
        {
            ["path"] = "keyauth.csv",
            ["format"] = "csv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var rows = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                rows += batch.Length;
                batch.Dispose();
            }
        }

        Assert.Equal(2, rows);
    }

    [SkippableFact]
    public async Task Host_key_pin_learns_matches_then_rejects_a_wrong_pin_permanently_without_the_password()
    {
        DockerFacts.SkipUnlessDocker();

        var realFingerprint = LearnHostKeyFingerprint();
        var connector = new SftpConnector();

        var matched = await connector.CheckConnectionAsync(ConfigPinnedTo(realFingerprint), CancellationToken.None);
        Assert.True(matched.Ok, matched.Message);

        // Same shape as a real SHA-256 fingerprint (43 base64-alphabet chars) but guaranteed to differ
        // from the real one -- the shape a copy-pasted-then-mistyped pin would take.
        var wrongFingerprint = FlipFirstChar(realFingerprint);
        var mismatched = await connector.CheckConnectionAsync(ConfigPinnedTo(wrongFingerprint), CancellationToken.None);

        Assert.False(mismatched.Ok);
        Assert.StartsWith("permanent:", mismatched.Message);
        Assert.Contains(wrongFingerprint, mismatched.Message);
        Assert.Contains(realFingerprint, mismatched.Message);
        Assert.DoesNotContain(SftpContainerFixture.Password, mismatched.Message);
    }

    private ConnectorConfig ConfigPinnedTo(string fingerprint) => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["username"] = SftpContainerFixture.PasswordUser,
        ["password"] = SftpContainerFixture.Password,
        ["root"] = "upload",
        ["host_key_fingerprint"] = fingerprint,
    });

    /// <summary>A raw, unpinned connect (accepts any host key) that exists only to learn the fingerprint
    /// the container's OWN host key hashes to — the same SHA-256-over-the-key-blob computation
    /// <see cref="SftpClientFactory.Connect"/> uses internally, reproduced here because that computed
    /// value is otherwise never handed back to a caller who didn't pin (and mismatch) against it.</summary>
    private string LearnHostKeyFingerprint()
    {
        var auth = new PasswordAuthenticationMethod(SftpContainerFixture.PasswordUser, SftpContainerFixture.Password);
        var info = new ConnectionInfo(fixture.Host, fixture.Port, SftpContainerFixture.PasswordUser, auth);
        using var client = new SftpClient(info);

        string? fingerprint = null;
        client.HostKeyReceived += (_, e) =>
        {
            fingerprint = Convert.ToBase64String(SHA256.HashData(e.HostKey)).TrimEnd('=');
            e.CanTrust = true;
        };

        client.Connect();
        client.Disconnect();

        return fingerprint ?? throw new InvalidOperationException("sftp container never presented a host key");
    }

    private static string FlipFirstChar(string fingerprint)
    {
        const string Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        var next = Alphabet[(Alphabet.IndexOf(fingerprint[0], StringComparison.Ordinal) + 1) % Alphabet.Length];
        return next + fingerprint[1..];
    }
}
