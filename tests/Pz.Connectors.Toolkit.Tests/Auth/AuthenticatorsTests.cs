using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connectors.Toolkit.Tests.Auth;

public class AuthenticatorsTests
{
    private static Dictionary<string, object?> Auth(params (string, object?)[] pairs)
        => pairs.ToDictionary(p => p.Item1, p => p.Item2);

    [Fact]
    public void Absent_block_is_anonymous()
    {
        Assert.True(Authenticators.TryCreate(null, out var auth, out _));
        Assert.Null(auth);
    }

    [Fact]
    public void Bearer_sets_authorization_header()
    {
        Assert.True(Authenticators.TryCreate(Auth(("type", "bearer"), ("token", "T")), out var auth, out _));
        var request = new HttpRequestMessage(HttpMethod.Get, "https://x/");
        auth!.Apply(request);
        Assert.Equal("Bearer T", request.Headers.Authorization!.ToString());
        Assert.Empty(auth.SecretQueryParams);
    }

    [Fact]
    public void Basic_encodes_user_password()
    {
        Assert.True(Authenticators.TryCreate(Auth(("type", "basic"), ("user", "u"), ("password", "p")),
            out var auth, out _));
        var request = new HttpRequestMessage(HttpMethod.Get, "https://x/");
        auth!.Apply(request);
        Assert.Equal("Basic " + Convert.ToBase64String("u:p"u8.ToArray()),
            request.Headers.Authorization!.ToString());
    }

    [Fact]
    public void Api_key_header_and_query_variants()
    {
        Assert.True(Authenticators.TryCreate(Auth(("type", "api_key"), ("key", "K"), ("header", "X-Api-Key")),
            out var header, out _));
        var request = new HttpRequestMessage(HttpMethod.Get, "https://x/");
        header!.Apply(request);
        Assert.Equal("K", request.Headers.GetValues("X-Api-Key").Single());

        Assert.True(Authenticators.TryCreate(Auth(("type", "api_key"), ("key", "K"), ("param", "api_key")),
            out var query, out _));
        var request2 = new HttpRequestMessage(HttpMethod.Get, "https://x/items?a=1");
        query!.Apply(request2);
        Assert.Contains("api_key=K", request2.RequestUri!.Query);
        Assert.Equal(["api_key"], query.SecretQueryParams.ToArray());
    }

    [Theory]
    [InlineData("bearer")]                   // missing token
    [InlineData("basic")]                    // missing user/password
    [InlineData("api_key")]                  // missing key and header/param
    [InlineData("oauth2")]                   // unsupported in v1
    public void Incomplete_or_unknown_blocks_error_without_echoing_secrets(string type)
    {
        Assert.False(Authenticators.TryCreate(Auth(("type", type)), out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public void Api_key_with_neither_header_nor_param_requires_exactly_one()
    {
        Assert.False(Authenticators.TryCreate(Auth(("type", "api_key"), ("key", "K")),
            out var auth, out var error));
        Assert.Null(auth);
        Assert.Contains("exactly one", error);
    }

    [Fact]
    public void Api_key_with_both_header_and_param_requires_exactly_one()
    {
        Assert.False(Authenticators.TryCreate(
            Auth(("type", "api_key"), ("key", "K"), ("header", "X-Api-Key"), ("param", "api_key")),
            out var auth, out var error));
        Assert.Null(auth);
        Assert.Contains("exactly one", error);
    }
}
