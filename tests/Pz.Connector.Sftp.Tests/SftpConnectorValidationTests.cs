using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpConnectorValidationTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] kv) =>
        new(kv.ToDictionary(p => p.Key, p => p.Value));

    private static ValidationResult Validate(ConnectorConfig config) =>
        new SftpConnector().ValidateAsync(config, CancellationToken.None).AsTask().Result;

    [Fact]
    public void Password_config_is_valid() =>
        Assert.True(Validate(Config(("host", "sftp.example"), ("username", "u"), ("password", "p"))).IsValid);

    [Fact]
    public void Key_config_is_valid() =>
        Assert.True(Validate(Config(("host", "h"), ("username", "u"), ("private_key_path", "/k"))).IsValid);

    [Fact]
    public void Missing_auth_and_missing_host_aggregate()
    {
        var result = Validate(Config(("username", "u")));
        Assert.Equal(2, result.Errors.Count);   // host missing + no auth method
    }

    [Fact]
    public void Both_password_and_key_is_an_error() =>
        Assert.False(Validate(Config(("host", "h"), ("username", "u"),
            ("password", "p"), ("private_key_path", "/k"))).IsValid);

    [Fact]
    public void Passphrase_without_key_is_an_error() =>
        Assert.False(Validate(Config(("host", "h"), ("username", "u"), ("password", "p"),
            ("private_key_passphrase", "x"))).IsValid);

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void Port_out_of_range_is_an_error(int port) =>
        Assert.False(Validate(Config(("host", "h"), ("username", "u"), ("password", "p"),
            ("port", port))).IsValid);

    [Theory]
    [InlineData("SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU")]
    [InlineData("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU")]
    public void Fingerprint_forms_accepted(string fp) =>
        Assert.True(Validate(Config(("host", "h"), ("username", "u"), ("password", "p"),
            ("host_key_fingerprint", fp))).IsValid);

    [Fact]
    public void Malformed_fingerprint_is_an_error() =>
        Assert.False(Validate(Config(("host", "h"), ("username", "u"), ("password", "p"),
            ("host_key_fingerprint", "md5:aa:bb"))).IsValid);

    // CheckConnectionAsync: a key file that fails to load is a config-shape error discovered before
    // any network attempt (SftpClientFactory.BuildAuth), not a connectivity outcome -- it must throw
    // rather than fold into a false ConnectionCheck(false, ...). No live server is involved.
    [Fact]
    public async Task CheckConnectionAsync_throws_for_an_unreadable_key_file()
    {
        var config = Config(("host", "sftp.example"), ("username", "u"),
            ("private_key_path", "/nonexistent/path/to/key"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await new SftpConnector().CheckConnectionAsync(config, CancellationToken.None));

        Assert.Contains("cannot load private key", ex.Message, StringComparison.Ordinal);
    }

    // Same config-shape-before-network-attempt guarantee for the neither-password-nor-key case, which
    // ValidateAsync rejects but a directly-constructed config can still reach CheckConnectionAsync with.
    [Fact]
    public async Task CheckConnectionAsync_throws_when_neither_auth_method_is_declared()
    {
        var config = Config(("host", "sftp.example"), ("username", "u"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await new SftpConnector().CheckConnectionAsync(config, CancellationToken.None));

        Assert.Contains("requires 'password' or 'private_key_path'", ex.Message, StringComparison.Ordinal);
    }
}
