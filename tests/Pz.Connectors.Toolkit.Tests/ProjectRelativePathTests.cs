using Pz.Connectors.Toolkit;

namespace Pz.Connectors.Toolkit.Tests;

/// <summary>The shared resolver behind #103: a connector's own local-file-path connection option
/// (SFTP's <c>private_key_path</c>, GCS's <c>key_file</c>) must anchor against the project directory
/// the same way <c>localfiles</c>' <c>root</c> and <c>sqlite</c>'s <c>path</c> already do, while an
/// absolute path, a <c>~</c>-prefixed home-directory shorthand, or a URL-shaped value passes through
/// exactly as written.</summary>
public sealed class ProjectRelativePathTests
{
    [Fact]
    public void Relative_value_joins_base_dir()
    {
        var resolved = ProjectRelativePath.Resolve("keys/id_rsa", "/projects/demo");

        Assert.Equal(Path.GetFullPath("/projects/demo/keys/id_rsa"), resolved);
    }

    [Fact]
    public void Absolute_value_passes_through_untouched()
    {
        var resolved = ProjectRelativePath.Resolve("/etc/secrets/id_rsa", "/projects/demo");

        Assert.Equal("/etc/secrets/id_rsa", resolved);
    }

    [Fact]
    public void Tilde_value_passes_through_untouched()
    {
        var resolved = ProjectRelativePath.Resolve("~/.ssh/id_rsa", "/projects/demo");

        Assert.Equal("~/.ssh/id_rsa", resolved);
    }

    [Theory]
    [InlineData("s3://bucket/key.json")]
    [InlineData("https://example.test/key.json")]
    public void Url_shaped_value_passes_through_untouched(string url)
    {
        var resolved = ProjectRelativePath.Resolve(url, "/projects/demo");

        Assert.Equal(url, resolved);
    }

    [Fact]
    public void Null_base_dir_falls_back_to_the_process_working_directory()
    {
        var resolved = ProjectRelativePath.Resolve("keys/id_rsa", baseDir: null);

        Assert.Equal(Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "keys/id_rsa")), resolved);
    }

    [Fact]
    public void Null_value_passes_through()
    {
        Assert.Null(ProjectRelativePath.Resolve(null, "/projects/demo"));
    }

    [Fact]
    public void Empty_value_passes_through()
    {
        Assert.Equal("", ProjectRelativePath.Resolve("", "/projects/demo"));
    }
}
