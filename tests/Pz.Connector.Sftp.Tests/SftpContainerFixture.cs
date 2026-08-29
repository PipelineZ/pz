using System.Security.Cryptography;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Pz.TestSupport;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Shared atmoz/sftp container: a password user and a key-only user, each with a
/// writable 'upload' directory. The RSA keypair is generated per fixture run — no key material is
/// ever committed to the repo — with the private key written PEM to a temp file (SSH.NET's
/// PrivateKeyFile input) and the public key mounted in OpenSSH line format.</summary>
public sealed class SftpContainerFixture : IAsyncLifetime
{
    private const string ImageName = "atmoz/sftp:alpine";
    public const string PasswordUser = "pzpass";
    public const string Password = "pz-test-password";
    public const string KeyUser = "pzkey";

    private IContainer? _container;

    public SftpContainerFixture()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();
    }

    public string Host { get; private set; } = "";
    public int Port { get; private set; }
    public string PrivateKeyPath { get; private set; } = "";

    public async Task InitializeAsync()
    {
        using var rsa = RSA.Create(2048);
        PrivateKeyPath = Path.Combine(Path.GetTempPath(), $"pz-sftp-test-{Guid.NewGuid():N}.pem");
        await File.WriteAllTextAsync(PrivateKeyPath, rsa.ExportRSAPrivateKeyPem());
        var publicLine = OpenSshPublicKey(rsa);

        // ContainerBuilder's parameterless constructor is obsolete in Testcontainers 4.13 (image is
        // supplied via the constructor now, not a later WithImage call).
        _container = new ContainerBuilder(ImageName)
            .WithCommand($"{PasswordUser}:{Password}:::upload", $"{KeyUser}::::upload")
            .WithResourceMapping(Encoding.ASCII.GetBytes(publicLine + "\n"),
                $"/home/{KeyUser}/.ssh/keys/id_rsa.pub")
            .WithPortBinding(22, assignRandomHostPort: true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server listening on"))
            .Build();
        await _container.StartAsync();
        Host = _container.Hostname;
        Port = _container.GetMappedPublicPort(22);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }

        File.Delete(PrivateKeyPath);
    }

    /// <summary>ssh-rsa wire format: length-prefixed "ssh-rsa", mpint e, mpint n (leading 0x00
    /// when the high bit is set), base64'd.</summary>
    private static string OpenSshPublicKey(RSA rsa)
    {
        var p = rsa.ExportParameters(false);
        using var ms = new MemoryStream();
        void WriteBlock(byte[] data)
        {
            var withSign = data[0] >= 0x80 ? new byte[] { 0 }.Concat(data).ToArray() : data;
            var len = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(withSign.Length);
            ms.Write(BitConverter.GetBytes(len));
            ms.Write(withSign);
        }

        WriteBlock(Encoding.ASCII.GetBytes("ssh-rsa"));
        WriteBlock(p.Exponent!);
        WriteBlock(p.Modulus!);
        return $"ssh-rsa {Convert.ToBase64String(ms.ToArray())}";
    }
}

[CollectionDefinition("sftp")]
public sealed class SftpCollection : ICollectionFixture<SftpContainerFixture>;
