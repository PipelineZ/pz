using Renci.SshNet.Common;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Self-test for the fake used by every later protocol-level suite — a bug here would be
/// invisible in whatever test happens to exercise it first.</summary>
public class FakeSftpFileSystemTests
{
    [Fact]
    public void ListFiles_non_recursive_excludes_nested_files()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/a.csv", []);
        fake.Seed("/data/sub/b.csv", []);

        var files = fake.ListFiles("/data", recursive: false).ToList();

        Assert.Equal(["/data/a.csv"], files);
    }

    [Fact]
    public void ListFiles_recursive_includes_nested_files()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/data/a.csv", []);
        fake.Seed("/data/sub/b.csv", []);

        var files = fake.ListFiles("/data", recursive: true).ToList();

        Assert.Equal(["/data/a.csv", "/data/sub/b.csv"], files);
    }

    [Fact]
    public void ListFiles_on_a_missing_directory_yields_nothing() =>
        Assert.Empty(new FakeSftpFileSystem().ListFiles("/nope", recursive: true));

    [Fact]
    public void OpenWrite_content_is_visible_through_OpenRead_after_dispose()
    {
        var fake = new FakeSftpFileSystem();

        using (var write = fake.OpenWrite("/out/x.csv"))
        {
            var bytes = "hello"u8.ToArray();
            write.Write(bytes, 0, bytes.Length);
        }

        using var read = fake.OpenRead("/out/x.csv");
        using var reader = new StreamReader(read);
        Assert.Equal("hello", reader.ReadToEnd());
    }

    [Fact]
    public void OpenRead_missing_file_throws_SftpPathNotFoundException() =>
        Assert.Throws<SftpPathNotFoundException>(() => new FakeSftpFileSystem().OpenRead("/missing"));

    [Fact]
    public void Rename_moves_content_to_the_new_path()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/a", "data"u8.ToArray());

        fake.Rename("/a", "/b");

        Assert.False(fake.FileExists("/a"));
        Assert.True(fake.FileExists("/b"));
    }

    [Fact]
    public void Rename_onto_an_existing_target_throws_and_leaves_the_source_in_place()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/a", "data"u8.ToArray());
        fake.Seed("/b", "other"u8.ToArray());

        Assert.Throws<SftpPermissionDeniedException>(() => fake.Rename("/a", "/b"));
        Assert.True(fake.FileExists("/a"));
    }

    [Fact]
    public void Rename_of_a_missing_source_throws_SftpPathNotFoundException() =>
        Assert.Throws<SftpPathNotFoundException>(() => new FakeSftpFileSystem().Rename("/a", "/b"));

    [Fact]
    public void Delete_removes_the_file()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/a", []);

        fake.Delete("/a");

        Assert.False(fake.FileExists("/a"));
    }

    [Fact]
    public void Delete_of_a_missing_path_throws_SftpPathNotFoundException() =>
        Assert.Throws<SftpPathNotFoundException>(() => new FakeSftpFileSystem().Delete("/nope"));

    [Fact]
    public void Operations_log_records_calls_in_order()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("/a", []);

        fake.Rename("/a", "/b");
        fake.Delete("/b");

        Assert.Equal(["rename:/a->/b", "delete:/b"], fake.Operations);
    }

    [Fact]
    public void FailOn_injects_a_fault_for_the_matching_operation()
    {
        var fake = new FakeSftpFileSystem();
        fake.FailOn = op => op == "delete:/a" ? new SshConnectionException("dropped") : null;
        fake.Seed("/a", []);

        Assert.Throws<SshConnectionException>(() => fake.Delete("/a"));
        // The guard runs before the operation body, so the fault fires without mutating state.
        Assert.True(fake.FileExists("/a"));
    }

    [Fact]
    public void CreateDirectories_is_logged_and_idempotent()
    {
        var fake = new FakeSftpFileSystem();

        fake.CreateDirectories("/a/b/c");
        fake.CreateDirectories("/a/b/c");

        Assert.Equal(["mkdir:/a/b/c", "mkdir:/a/b/c"], fake.Operations);
    }
}
