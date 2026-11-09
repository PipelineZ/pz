using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http;

/// <summary>Turns a connection's <c>auth:</c> map into either a ready static authenticator or a
/// factory for a token-fetching one. Parsing never builds a credential or an HttpClient: the factory
/// runs only when the connection is bound for use.</summary>
internal static class HttpAuth
{
    private const string AzureAd = "azure_ad";
    private const string OAuth2ClientCredentials = "oauth2_client_credentials";
    private const string OAuth2RefreshToken = "oauth2_refresh_token";
    private const string Session = "session";

    /// <summary>Reports a final response to the authenticator and turns a rejected token into the
    /// exception the engine acts on: a token that was accepted before is discarded and the retry signs
    /// in again (transient); one that never worked is a configuration problem (permanent).</summary>
    internal static void ThrowIfTokenRejected(IRequestAuthenticator? authenticator, HttpRequestMessage request,
        int status, string label)
    {
        switch (authenticator?.OnResponse(request, status))
        {
            case AuthResponseOutcome.TokenDiscarded:
                throw new PzConnectorException(
                    $"{label}: the API rejected the access token (HTTP {status}); signing in again on retry",
                    isTransient: true);
            case AuthResponseOutcome.FreshTokenRejected when authenticator is SessionAuthenticator:
                throw new PzConnectorException(
                    $"{label}: the API rejected a fresh session token (HTTP {status}); check that auth.token_pointer " +
                    "picks the token from the login response and that auth.inject sends it the way this API expects",
                    isTransient: false);
            case AuthResponseOutcome.FreshTokenRejected:
                throw new PzConnectorException(
                    $"{label}: the API rejected a freshly issued token (HTTP {status}); check that 'scope' names " +
                    "this API and that the app has been granted access to it", isTransient: false);
        }
    }

    /// <summary>Gives the sign-in a place for its notices; only a refresh-token sign-in has any.</summary>
    internal static void UseNotice(IRequestAuthenticator? authenticator, Action<string> notice)
    {
        if (authenticator is OAuth2RefreshTokenAuthenticator refresh)
        {
            refresh.Notice = notice;
        }
    }

    internal sealed record Parsed(
        IRequestAuthenticator? Static,
        Func<TimeProvider, Func<Uri, HttpMessageHandler>?, IRequestAuthenticator>? Factory,
        string? CertificatePath,
        IReadOnlyList<string>? Warnings = null,
        Uri? LoginUrl = null,
        SessionInject? Inject = null);

    /// <param name="baseUrl">The connection's base URL (null when it is invalid): a session login path resolves against it.</param>
    /// <param name="retryAfter">The connection's <c>retry_after</c> header, used by a session login that is throttled.</param>
    internal static Parsed Parse(IReadOnlyDictionary<string, object?>? auth, string? baseDir, TimeSpan timeout, List<string> errors,
        bool hasTransport = false, Uri? baseUrl = null, HttpRetryAfterHeader? retryAfter = null)
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
                return AzureAdOptions.TryParse(auth, baseDir, errors, hasTransport) is { } azure
                    ? new Parsed(null, (time, handlers) => azure.CreateAuthenticator(timeout, time, handlers), azure.CertificatePath, azure.Warnings())
                    : none;
            case OAuth2ClientCredentials:
                return OAuth2ClientCredentialsOptions.TryParse(auth, errors) is { } oauth
                    ? new Parsed(null, (time, handlers) => new OAuth2ClientCredentialsAuthenticator(oauth, timeout, time,
                        handlers?.Invoke(oauth.TokenUrl)), null)
                    : none;
            case OAuth2RefreshToken:
                return OAuth2RefreshTokenOptions.TryParse(auth, errors) is { } refresh
                    ? new Parsed(null, (time, handlers) => new OAuth2RefreshTokenAuthenticator(refresh, timeout, time,
                        handlers?.Invoke(refresh.TokenUrl)), null)
                    : none;
            case Session:
                return SessionOptions.TryParse(auth, baseUrl, errors) is { } session
                    ? new Parsed(null, (time, handlers) => new SessionAuthenticator(session, timeout, time,
                        handlers?.Invoke(session.LoginUrl), retryAfter), null, LoginUrl: session.LoginUrl, Inject: session.Inject)
                    : none;
            case "api_key" or "bearer" or "basic":
                if (!Authenticators.TryCreate(auth, out var authenticator, out var error))
                {
                    errors.Add(error!);
                    return none;
                }

                return new Parsed(authenticator, null, null);
            default:
                errors.Add($"unknown auth type '{type}' (accepted: api_key, bearer, basic, {AzureAd}, {OAuth2ClientCredentials}, {OAuth2RefreshToken}, {Session})");
                return none;
        }
    }
}
