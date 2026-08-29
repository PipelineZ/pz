using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpConnectionSettingsTests
{
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
