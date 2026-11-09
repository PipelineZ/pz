using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpAllowedHostsTests
{
    private static HttpConnectionConfig Connection(string baseUrl, params string[] allowHosts)
    {
        var errors = new List<string>();
        var config = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = baseUrl,
            ["allow_hosts"] = allowHosts.ToList(),
        }), errors);
        Assert.Empty(errors);
        return config!;
    }

    [Fact]
    public void An_allow_listed_host_is_reachable_over_https()
    {
        var connection = Connection("https://api.example.com", "files.example.com");
        Assert.True(connection.IsAllowedTarget(new Uri("https://files.example.com/blob/1")));
    }

    [Fact]
    public void An_allow_listed_host_is_refused_over_cleartext_http_when_the_base_is_https()
    {
        // The credentials would leave over an unencrypted connection the endpoint chose.
        var connection = Connection("https://api.example.com", "files.example.com");
        Assert.False(connection.IsAllowedTarget(new Uri("http://files.example.com/blob/1")));
    }

    [Fact]
    public void An_allow_listed_host_may_use_http_when_the_base_itself_is_http()
    {
        var connection = Connection("http://localhost:8080", "files.local");
        Assert.True(connection.IsAllowedTarget(new Uri("http://files.local/blob/1")));
    }

    [Fact]
    public void The_base_host_over_a_different_scheme_is_refused()
    {
        var connection = Connection("https://api.example.com");
        Assert.False(connection.IsAllowedTarget(new Uri("http://api.example.com/items")));
    }

    [Theory]
    [InlineData("bearer")]
    [InlineData("basic")]
    [InlineData("api_key")]
    public async Task Validate_warns_when_credentials_go_to_a_cleartext_base_url(string authType)
    {
        var auth = new Dictionary<string, object?> { ["type"] = authType };
        switch (authType)
        {
            case "bearer": auth["token"] = "t"; break;
            case "basic": auth["user"] = "u"; auth["password"] = "p"; break;
            default: auth["key"] = "k"; auth["header"] = "X-Api-Key"; break;
        }

        var result = await new HttpConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "http://api.example.com",
            ["auth"] = auth,
        }), CancellationToken.None);

        Assert.True(result.IsValid);
        var warning = Assert.Single(result.Warnings);
        Assert.Contains("http://", warning);
        Assert.Contains("https", warning);
    }

    [Fact]
    public async Task Validate_warns_when_an_authorization_header_goes_to_a_cleartext_base_url()
    {
        var result = await new HttpConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "http://api.example.com",
            ["headers"] = new Dictionary<string, object?> { ["authorization"] = "Token abc" },
        }), CancellationToken.None);

        Assert.Single(result.Warnings);
    }

    [Theory]
    [InlineData("https://api.example.com")] // encrypted
    [InlineData("http://localhost:8080")] // loopback: never leaves the machine
    [InlineData("http://127.0.0.1:8080")]
    [InlineData("http://[::1]:8080")]
    public async Task Validate_does_not_warn_for_https_or_loopback(string baseUrl)
    {
        var result = await new HttpConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = baseUrl,
            ["auth"] = new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "t" },
        }), CancellationToken.None);

        Assert.Empty(result.Warnings);
    }

    [Fact]
    public async Task Validate_does_not_warn_for_cleartext_without_credentials()
    {
        var result = await new HttpConnector().ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = "http://api.example.com",
        }), CancellationToken.None);

        Assert.Empty(result.Warnings);
    }
}
