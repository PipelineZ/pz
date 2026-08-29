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
}
