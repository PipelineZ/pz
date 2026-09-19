using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpConnectionSettingsTests
{
    /// <summary>A relative `private_key_path` must resolve against the project directory
    /// (`base_dir`, injected by the CLI -- see `ProjectDirectoryAnchor`), not wherever `pz` was
    /// invoked from. No process-CWD manipulation: `base_dir` is passed explicitly, and differs from
    /// this test process's actual working directory, so a wrong resolution (falling back to CWD)
    /// would produce a visibly different path.</summary>
    [Fact]
    public void Parse_resolves_a_relative_private_key_path_against_base_dir()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "sftp.example",
            ["username"] = "alice",
            ["private_key_path"] = "keys/id_rsa",
            ["base_dir"] = "/projects/demo",
        });
        Assert.NotEqual("/projects/demo", Directory.GetCurrentDirectory());

        var settings = SftpConnectionSettings.Parse(config);

        Assert.Equal(Path.GetFullPath("/projects/demo/keys/id_rsa"), settings.PrivateKeyPath);
    }

    [Fact]
    public void Parse_leaves_an_absolute_private_key_path_untouched()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "sftp.example",
            ["username"] = "alice",
            ["private_key_path"] = "/keys/id_rsa",
            ["base_dir"] = "/projects/demo",
        });

        var settings = SftpConnectionSettings.Parse(config);

        Assert.Equal("/keys/id_rsa", settings.PrivateKeyPath);
    }

    [Fact]
    public void Parse_leaves_a_tilde_private_key_path_untouched()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "sftp.example",
            ["username"] = "alice",
            ["private_key_path"] = "~/.ssh/id_rsa",
            ["base_dir"] = "/projects/demo",
        });

        var settings = SftpConnectionSettings.Parse(config);

        Assert.Equal("~/.ssh/id_rsa", settings.PrivateKeyPath);
    }

    // The compiler-synthesized record ToString() would print every property, Password included --
    // this pins the override that keeps credential material out of anything that implicitly calls
    // ToString() on a settings instance (log lines, exception messages, interpolated strings).
    [Fact]
    public void ToString_names_the_host_but_never_the_password()
    {
        var settings = new SftpConnectionSettings(
            "sftp.example", 22, "alice", "super-secret-password", null, null, null, Root: "/data");

        var text = settings.ToString();

        Assert.Contains("sftp.example", text, StringComparison.Ordinal);
        Assert.DoesNotContain("super-secret-password", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ToString_never_prints_the_private_key_passphrase()
    {
        var settings = new SftpConnectionSettings(
            "sftp.example", 22, "alice", null, "/keys/id_rsa", "super-secret-passphrase", null, Root: null);

        var text = settings.ToString();

        Assert.DoesNotContain("super-secret-passphrase", text, StringComparison.Ordinal);
    }
}
