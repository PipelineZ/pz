using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http;

/// <summary>Turns a connection's <c>auth:</c> map into either a ready static authenticator or a
/// factory for a token-fetching one. Parsing never builds a credential or an HttpClient: the factory
/// runs only when the connection is bound for use.</summary>
internal static class HttpAuth
{
    private const string AzureAd = "azure_ad";
    private const string OAuth2ClientCredentials = "oauth2_client_credentials";

    internal sealed record Parsed(
        IRequestAuthenticator? Static,
        Func<TimeProvider, IRequestAuthenticator>? Factory,
        string? CertificatePath);

    internal static Parsed Parse(IReadOnlyDictionary<string, object?>? auth, string? baseDir, TimeSpan timeout, List<string> errors)
    {
        var none = new Parsed(null, null, null);
        if (auth is null || auth.Count == 0)
        {
            return none;
        }

        var type = auth.TryGetValue("type", out var t) ? t?.ToString() : null;
        switch (type)
        {
            case AzureAd:
                return AzureAdOptions.TryParse(auth, baseDir, errors) is { } azure
                    ? new Parsed(null, time => azure.CreateAuthenticator(time), azure.CertificatePath)
                    : none;
            case OAuth2ClientCredentials:
                return OAuth2ClientCredentialsOptions.TryParse(auth, errors) is { } oauth
                    ? new Parsed(null, time => new OAuth2ClientCredentialsAuthenticator(oauth, timeout, time), null)
                    : none;
            case "api_key" or "bearer" or "basic":
                if (!Authenticators.TryCreate(auth, out var authenticator, out var error))
                {
                    errors.Add(error!);
                    return none;
                }

                return new Parsed(authenticator, null, null);
            default:
                errors.Add($"unknown auth type '{type}' (accepted: api_key, bearer, basic, {AzureAd}, {OAuth2ClientCredentials})");
                return none;
        }
    }
}
