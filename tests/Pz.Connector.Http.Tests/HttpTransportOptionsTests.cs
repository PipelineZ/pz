using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpTransportOptionsTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] pairs)
        => pairs.ToDictionary(p => p.Key, p => p.Value);

    private static (HttpTransportOptions Options, List<string> Errors) Parse(
        Dictionary<string, object?> values, string? baseDir = null)
    {
        var errors = new List<string>();
        var options = HttpTransportOptions.Parse(values, baseDir, errors);
        return (options, errors);
    }

    private static Dictionary<string, object?> Proxy(string url, params (string Key, object? Value)[] extra)
    {
        var proxy = Map(("url", url));
        foreach (var (k, v) in extra)
        {
            proxy[k] = v;
        }

        return Map(("proxy", proxy));
    }

    [Fact]
    public void Absent_keys_are_default()
    {
        var (options, errors) = Parse([]);
        Assert.Empty(errors);
        Assert.True(options.IsDefault);
        Assert.Equal(ProxyMode.Default, options.Mode);
    }

    [Fact]
    public void Proxy_none_and_explicit()
    {
        var (none, noneErrors) = Parse(Map(("proxy", "none")));
        Assert.Empty(noneErrors);
        Assert.Equal(ProxyMode.None, none.Mode);
        Assert.False(none.IsDefault);

        var (explicitProxy, errors) = Parse(Proxy("http://proxy.corp:3128"));
        Assert.Empty(errors);
        Assert.Equal(ProxyMode.Explicit, explicitProxy.Mode);
        Assert.Equal("proxy.corp", explicitProxy.ProxyUrl!.Host);
        Assert.Equal(3128, explicitProxy.ProxyUrl.Port);

        var (_, other) = Parse(Map(("proxy", "None")));
        Assert.Single(other);
        Assert.Contains("proxy", other[0], StringComparison.Ordinal);
    }

    [Fact]
    public void Proxy_url_rules()
    {
        Assert.Contains(Parse(Proxy("ftp://p.corp")).Errors, e => e.Contains("proxy.url", StringComparison.Ordinal));

        var (_, userinfo) = Parse(Proxy("http://alice:hunter2@p.corp:3128"));
        var error = Assert.Single(userinfo);
        Assert.Contains("userinfo", error, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", error, StringComparison.Ordinal);
        Assert.DoesNotContain("alice", error, StringComparison.Ordinal);

        Assert.Single(Parse(Proxy("http://p.corp/pac/proxy")).Errors);
        Assert.Single(Parse(Proxy("http://p.corp/?a=b")).Errors);
        Assert.Single(Parse(Proxy("http://p.corp/#frag")).Errors);
        Assert.Single(Parse(Proxy("not a url")).Errors);

        Assert.Empty(Parse(Proxy("http://p.corp/")).Errors);
        Assert.Empty(Parse(Proxy("http://p.corp")).Errors);

        // A TLS connection to the proxy itself is not supported: the target's TLS still runs end to end.
        foreach (var https in new[] { "https://p.corp/", "https://p.corp:8443" })
        {
            Assert.Equal(["proxy.url must be an http:// URL (the TLS connection to the target still runs end to end through the proxy)"],
                Parse(Proxy(https)).Errors);
        }

        var (_, missing) = Parse(Map(("proxy", Map(("bypass", new List<object?> { "a.example" })))));
        Assert.Contains(missing, e => e.Contains("proxy.url", StringComparison.Ordinal));
    }

    [Fact]
    public void Proxy_user_password_both_or_neither()
    {
        var (both, bothErrors) = Parse(Proxy("http://p.corp", ("user", "alice"), ("password", "hunter2")));
        Assert.Empty(bothErrors);
        Assert.Equal("alice", both.ProxyUser);
        Assert.Equal("hunter2", both.ProxyPassword);

        foreach (var half in new[] { ("user", "alice"), ("password", "hunter2") })
        {
            var (_, errors) = Parse(Proxy("http://p.corp", half));
            var error = Assert.Single(errors);
            Assert.Contains("proxy.user", error, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Bypass_patterns()
    {
        Assert.True(HttpTransportOptions.BypassMatches("*.corp.example", "a.corp.example"));
        Assert.True(HttpTransportOptions.BypassMatches("*.corp.example", "x.y.CORP.example"));
        Assert.False(HttpTransportOptions.BypassMatches("*.corp.example", "corp.example"));
        Assert.False(HttpTransportOptions.BypassMatches("*.corp.example", "evilcorp.example"));
        Assert.True(HttpTransportOptions.BypassMatches("Api.Example.com", "api.example.COM"));
        Assert.False(HttpTransportOptions.BypassMatches("api.example.com", "x.api.example.com"));
        Assert.False(HttpTransportOptions.BypassMatches("api.example.com", "api.example.com.evil"));

        var (ok, errors) = Parse(Proxy("http://p.corp",
            ("bypass", new List<object?> { "localhost", "*.corp.example", "10.0.0.1" })));
        Assert.Empty(errors);
        Assert.Equal(["localhost", "*.corp.example", "10.0.0.1"], ok.Bypass);

        foreach (var bad in new[] { "", "*", "*.", "a*.example", "http://x.example", "x.example/path", "a b", "*.*.x", "h:80" })
        {
            var (_, badErrors) = Parse(Proxy("http://p.corp", ("bypass", new List<object?> { bad })));
            Assert.Single(badErrors);
            Assert.Contains("proxy.bypass", badErrors[0], StringComparison.Ordinal);
        }

        Assert.Single(Parse(Proxy("http://p.corp", ("bypass", "x.example"))).Errors);
    }

    [Fact]
    public void Tls_paths_resolve_relative_and_home()
    {
        var baseDir = Path.Combine(Path.GetTempPath(), "pz-proj");
        var tls = Map(("tls", Map(
            ("ca_file", "certs/ca.pem"),
            ("client_certificate", Map(("path", "~/id/client.pfx"), ("password", "pw"))))));
        var (options, errors) = Parse(tls, baseDir);
        Assert.Empty(errors);
        Assert.Equal(Path.Combine(baseDir, "certs", "ca.pem"), options.CaFile);
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        Assert.Equal(Path.Combine(home, "id", "client.pfx"), options.CertPath);
        Assert.Equal("pw", options.CertPassword);
        Assert.Null(options.KeyPath);
        Assert.False(options.IsDefault);

        var pem = Map(("tls", Map(("client_certificate", Map(("path", "c.crt"), ("key_path", "k/c.key"))))));
        var (pemOptions, pemErrors) = Parse(pem, baseDir);
        Assert.Empty(pemErrors);
        Assert.Equal(Path.Combine(baseDir, "c.crt"), pemOptions.CertPath);
        Assert.Equal(Path.Combine(baseDir, "k", "c.key"), pemOptions.KeyPath);
    }

    [Fact]
    public void Key_path_only_with_pem()
    {
        var (_, pfx) = Parse(Map(("tls", Map(("client_certificate", Map(("path", "c.pfx"), ("key_path", "k.key")))))), "/p");
        Assert.Contains("key_path", Assert.Single(pfx), StringComparison.Ordinal);

        var (_, pem) = Parse(Map(("tls", Map(("client_certificate", Map(("path", "c.pem")))))), "/p");
        Assert.Contains("key_path", Assert.Single(pem), StringComparison.Ordinal);

        var (_, ext) = Parse(Map(("tls", Map(("client_certificate", Map(("path", "c.txt")))))), "/p");
        var message = Assert.Single(ext);
        foreach (var accepted in new[] { ".pfx", ".p12", ".pem", ".crt", ".cer" })
        {
            Assert.Contains(accepted, message, StringComparison.Ordinal);
        }

        foreach (var name in new[] { "c.p12", "c.PFX" })
        {
            Assert.Empty(Parse(Map(("tls", Map(("client_certificate", Map(("path", name)))))), "/p").Errors);
        }

        Assert.Empty(Parse(Map(("tls", Map(("client_certificate", Map(("path", "c.cer"), ("key_path", "k")))))), "/p").Errors);
    }

    [Fact]
    public void Password_without_certificate_is_error()
    {
        var (_, errors) = Parse(Map(("tls", Map(("client_certificate", Map(("password", "pw")))))));
        Assert.Contains(errors, e => e.Contains("tls.client_certificate.path", StringComparison.Ordinal));
        Assert.DoesNotContain(errors, e => e.Contains("pw", StringComparison.Ordinal) && !e.Contains("password", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Credential_chain_refuses_proxy_or_tls()
    {
        const string message = "auth.method credential_chain cannot use proxy or tls: its developer-tool credentials run outside pz";
        var chain = Map(("type", "azure_ad"), ("method", "credential_chain"), ("scope", "api://x/.default"));
        var managed = Map(("type", "azure_ad"), ("method", "managed_identity"), ("scope", "api://x/.default"));
        foreach (var transport in new[]
        {
            ("proxy", (object?)"none"),
            ("tls", Map(("ca_file", "ca.pem"))),
            ("tls", Map(("client_certificate", Map(("path", "client.pfx"))))),
        })
        {
            var refused = await new HttpConnector().ValidateAsync(new ConnectorConfig(
                Map(("base_url", "https://api.example.com"), ("auth", chain), transport)), CancellationToken.None);
            Assert.Equal([message], refused.Errors);

            var allowed = await new HttpConnector().ValidateAsync(new ConnectorConfig(
                Map(("base_url", "https://api.example.com"), ("auth", managed), transport)), CancellationToken.None);
            Assert.True(allowed.IsValid, string.Join("; ", allowed.Errors));
        }

        // An empty 'tls: {}' configures nothing, so it is not refused (the same rule that leaves the handler untouched).
        foreach (var plainConfig in new[]
        {
            Map(("base_url", "https://api.example.com"), ("auth", chain)),
            Map(("base_url", "https://api.example.com"), ("auth", chain), ("tls", Map())),
        })
        {
            var plain = await new HttpConnector().ValidateAsync(new ConnectorConfig(plainConfig), CancellationToken.None);
            Assert.True(plain.IsValid, string.Join("; ", plain.Errors));
        }
    }

    [Fact]
    public async Task Errors_are_aggregate_and_secret_free()
    {
        var values = Map(
            ("base_url", "not a url"),
            ("proxy", Map(("url", "ftp://alice:hunter2@p.corp"), ("password", "hunter2"), ("bypass", new List<object?> { "bad host" }))),
            ("tls", Map(("client_certificate", Map(("path", "c.pfx"), ("key_path", "k"), ("password", "s3cretpw"))))));
        var result = await new HttpConnector().ValidateAsync(new ConnectorConfig(values), CancellationToken.None);
        Assert.False(result.IsValid);
        Assert.True(result.Errors.Count >= 5, string.Join("; ", result.Errors));
        Assert.Contains(result.Errors, e => e.Contains("base_url", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("proxy.url", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("proxy.bypass", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("key_path", StringComparison.Ordinal));
        foreach (var secret in new[] { "hunter2", "alice", "s3cretpw" })
        {
            Assert.DoesNotContain(result.Errors, e => e.Contains(secret, StringComparison.Ordinal));
        }

        var (options, _) = Parse(Proxy("http://p.corp", ("user", "alice"), ("password", "hunter2")));
        Assert.DoesNotContain("hunter2", options.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Schema_has_proxy_and_tls()
    {
        var text = new HttpConnector().ConnectionConfigSchema;
        _ = JsonSchema.FromText(text);
        var props = JsonDocument.Parse(text).RootElement.GetProperty("properties");

        var proxy = props.GetProperty("proxy").GetProperty("oneOf");
        Assert.Equal(2, proxy.GetArrayLength());
        Assert.Equal("none", proxy[0].GetProperty("const").GetString());
        var proxyMap = proxy[1];
        Assert.Equal("object", proxyMap.GetProperty("type").GetString());
        Assert.False(proxyMap.GetProperty("additionalProperties").GetBoolean());
        Assert.True(proxyMap.GetProperty("properties").GetProperty("password").GetProperty("writeOnly").GetBoolean());

        var tls = props.GetProperty("tls");
        Assert.False(tls.GetProperty("additionalProperties").GetBoolean());
        var cert = tls.GetProperty("properties").GetProperty("client_certificate");
        Assert.False(cert.GetProperty("additionalProperties").GetBoolean());
        Assert.True(cert.GetProperty("properties").GetProperty("password").GetProperty("writeOnly").GetBoolean());

        var schema = JsonSchema.FromText(text);
        bool Valid(string json) => schema.Evaluate(JsonDocument.Parse(json).RootElement).IsValid;
        Assert.True(Valid("""{"base_url":"https://x","proxy":"none"}"""));
        Assert.True(Valid("""{"base_url":"https://x","proxy":{"url":"http://p","bypass":["a"]},"tls":{"ca_file":"a.pem"}}"""));
        Assert.False(Valid("""{"base_url":"https://x","proxy":{"url":"http://p","extra":1}}"""));
        Assert.False(Valid("""{"base_url":"https://x","tls":{"verify":false}}"""));
    }
}
