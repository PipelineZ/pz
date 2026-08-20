using System.Net.Http.Headers;
using System.Text;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>Builds the v1 static-auth trio from a connection `auth:` block. OAuth2 flows are a
/// deliberate non-goal in v1; interactive flows are permanently out of scope for a batch
/// engine.</summary>
public static class Authenticators
{
    public static bool TryCreate(IReadOnlyDictionary<string, object?>? auth,
        out IRequestAuthenticator? authenticator, out string? error)
    {
        authenticator = null;
        error = null;
        if (auth is null || auth.Count == 0)
        {
            return true;
        }

        var type = auth.TryGetValue("type", out var t) ? t?.ToString() : null;
        string? Get(string key) => auth.TryGetValue(key, out var v) ? v?.ToString() : null;

        switch (type)
        {
            case "bearer" when Get("token") is { Length: > 0 } token:
                authenticator = new HeaderAuthenticator(r =>
                    r.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token));
                return true;
            case "bearer":
                error = "auth type 'bearer' requires 'token'";
                return false;
            case "basic" when Get("user") is { } user && Get("password") is { } password:
                authenticator = new HeaderAuthenticator(r => r.Headers.Authorization =
                    new AuthenticationHeaderValue("Basic",
                        Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"))));
                return true;
            case "basic":
                error = "auth type 'basic' requires 'user' and 'password'";
                return false;
            case "api_key" when Get("key") is { Length: > 0 } key:
                var header = Get("header");
                var param = Get("param");
                if ((header is null) == (param is null))
                {
                    error = "auth type 'api_key' requires exactly one of 'header' or 'param'";
                    return false;
                }

                authenticator = header is not null
                    ? new HeaderAuthenticator(r => r.Headers.TryAddWithoutValidation(header, key))
                    : new QueryAuthenticator(param!, key);
                return true;
            case "api_key":
                error = "auth type 'api_key' requires 'key'";
                return false;
            default:
                error = $"unknown auth type '{type}' (accepted: api_key, bearer, basic)";
                return false;
        }
    }

    private sealed class HeaderAuthenticator(Action<HttpRequestMessage> apply) : IRequestAuthenticator
    {
        public void Apply(HttpRequestMessage request) => apply(request);
        public IReadOnlyCollection<string> SecretQueryParams => [];
    }

    private sealed class QueryAuthenticator(string param, string key) : IRequestAuthenticator
    {
        public void Apply(HttpRequestMessage request)
            => request.RequestUri = QueryString.With(request.RequestUri!, param, key);

        public IReadOnlyCollection<string> SecretQueryParams { get; } = [param];
    }
}
