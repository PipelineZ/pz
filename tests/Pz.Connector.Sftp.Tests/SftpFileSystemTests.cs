using System.Text;
using Pz.TestSupport;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Real-server proof that <see cref="SftpFileSystem.ListFiles"/> yields paths relative to
/// whatever form the caller passed, never the server's <c>realpath</c>-canonicalized form. A chrooted
/// OpenSSH server (atmoz/sftp) canonicalizes a relative listing directory like "upload/x" to "/upload/x",
/// so <c>ISftpFile.FullName</c> comes back server-absolute -- which used to never match the caller's
/// own relative glob pattern.</summary>
[Collection("sftp")]
public sealed class SftpFileSystemTests(SftpContainerFixture fixture)
{
    private SftpConnectionSettings Settings() => new(
        fixture.Host, fixture.Port, SftpContainerFixture.PasswordUser, SftpContainerFixture.Password,
        PrivateKeyPath: null, PrivateKeyPassphrase: null, HostKeyFingerprint: null, Root: null);

    private static void Write(ISftpFileSystem fs, string path)
    {
        using var stream = fs.OpenWrite(path);
        var bytes = Encoding.UTF8.GetBytes("id\n1\n");
        stream.Write(bytes, 0, bytes.Length);
    }

    [SkippableFact]
    public void ListFiles_yields_paths_relative_to_the_requested_directory_not_the_servers_realpath()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"upload/realpath-{Guid.NewGuid():N}";
        using var fs = SftpClientFactory.Open(Settings());
        fs.CreateDirectories(prefix);
        Write(fs, $"{prefix}/orders-0.csv");

        var matches = fs.ListFiles(prefix, recursive: false).ToArray();

        Assert.Equal([$"{prefix}/orders-0.csv"], matches);
    }

    [SkippableFact]
    public void Recursive_listing_stays_relative_through_a_nested_directory()
    {
        DockerFacts.SkipUnlessDocker();

        var prefix = $"upload/realpath-{Guid.NewGuid():N}";
        using var fs = SftpClientFactory.Open(Settings());
        fs.CreateDirectories($"{prefix}/nested");
        Write(fs, $"{prefix}/top.csv");
        Write(fs, $"{prefix}/nested/deep.csv");

        var matches = fs.ListFiles(prefix, recursive: true).OrderBy(m => m, StringComparer.Ordinal).ToArray();

        Assert.Equal([$"{prefix}/nested/deep.csv", $"{prefix}/top.csv"], matches);
    }

    [SkippableFact]
    public void Recursive_listing_from_the_login_directory_has_no_dot_slash_prefix()
    {
        DockerFacts.SkipUnlessDocker();

        var name = $"realpath-{Guid.NewGuid():N}.csv";
        using var fs = SftpClientFactory.Open(Settings());
        Write(fs, $"upload/{name}");

        // "." is the login-relative root; "upload" is the only writable subdirectory under it. This
        // proves both halves of the join rule together: the recursion into "upload" enqueues "upload"
        // itself (not "./upload"), and the file underneath lands as "upload/<name>" (not
        // "./upload/<name>", and not the server's realpath-canonicalized absolute form).
        var matches = fs.ListFiles(".", recursive: true).ToArray();

        Assert.Contains($"upload/{name}", matches);
        Assert.DoesNotContain(matches, m => m.StartsWith("./", StringComparison.Ordinal));
    }
}
