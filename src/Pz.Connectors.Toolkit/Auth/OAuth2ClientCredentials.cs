namespace Pz.Connectors.Toolkit.Auth;

/// <summary>OAuth2 client-credentials settings (RFC 6749 section 4.4). <see cref="Params"/> are extra
/// token-request form fields (e.g. <c>audience</c>); the reserved protocol fields cannot be set there.</summary>
public sealed record OAuth2ClientCredentialsOptions(
    Uri TokenUrl,
    string ClientId,
    string ClientSecret,
    string? Scope,
    bool BasicClientAuth,
    IReadOnlyDictionary<string, string> Params)
{
    private static readonly string[] ReservedParams = ["grant_type", "client_id", "client_secret", "scope"];

    /// <summary>Parses the YAML <c>auth:</c> map. Every problem is appended to <paramref name="errors"/>;
    /// the result is null when any was found. The token URL must be https (plain http only on loopback)
    /// because the client secret travels in the request.</summary>
    public static OAuth2ClientCredentialsOptions? TryParse(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        var before = errors.Count;

        var url = OAuth2Fields.TokenUrl(auth, errors);
        var clientId = OAuth2Fields.RequiredText(auth, "client_id", errors);
        var clientSecret = OAuth2Fields.RequiredText(auth, "client_secret", errors);
        var scope = OAuth2Fields.OptionalText(auth, "scope", errors);
        var basic = OAuth2Fields.BasicClientAuth(auth, errors);
        var extra = OAuth2Fields.Params(auth, ReservedParams, errors);

        return errors.Count > before ? null : new OAuth2ClientCredentialsOptions(url!, clientId!, clientSecret!, scope, basic, extra);
    }
}

/// <summary>Fetches bearer tokens with the OAuth2 client-credentials grant. Owns its HttpClient (no
/// redirects: the secret must not follow a Location). Failure messages carry only the endpoint's
/// scheme://authority, the status and the server's OAuth error fields, never the secret or token.</summary>
/// <remarks>An <c>invalid_client</c> or <c>unauthorized_client</c> answer (a wrong or revoked secret, a client not
/// allowed this grant) is remembered for the run (<see cref="CachedTokenAuthenticator.SignInKey"/>): the other nodes
/// on the connection fail at once instead of each sending the same refused secret.</remarks>
public sealed class OAuth2ClientCredentialsAuthenticator : CachedTokenAuthenticator
{
    private static readonly string[] TerminalErrors = ["invalid_client", "unauthorized_client"];

    private readonly OAuth2ClientCredentialsOptions _options;
    private readonly OAuth2TokenEndpoint _endpoint;
    private readonly string _signInKey;

    public OAuth2ClientCredentialsAuthenticator(OAuth2ClientCredentialsOptions options, TimeSpan timeout, TimeProvider time)
        : this(options, timeout, time, handler: null)
    {
    }

    /// <param name="handler">The transport for the token request (a proxy, extra trust anchors, a client
    /// certificate); the authenticator owns and disposes it. It must not follow redirects, or the secret
    /// could follow a Location. Null builds the default handler, which follows none.</param>
    /// <remarks>A separate overload rather than an optional parameter, so code compiled against the
    /// three-argument constructor keeps binding to it.</remarks>
    public OAuth2ClientCredentialsAuthenticator(OAuth2ClientCredentialsOptions options, TimeSpan timeout, TimeProvider time,
        HttpMessageHandler? handler)
        : base(time)
    {
        _options = options;
        _endpoint = new OAuth2TokenEndpoint(options.TokenUrl, options.ClientId, options.ClientSecret, options.BasicClientAuth,
            options.Scope, timeout, time, handler);
        _signInKey = Fingerprint(
            [
                "oauth2_client_credentials", options.TokenUrl.AbsoluteUri, options.ClientId, options.ClientSecret,
                options.Scope, options.BasicClientAuth ? "basic" : "body",
                .. options.Params.OrderBy(p => p.Key, StringComparer.Ordinal).SelectMany(p => new[] { p.Key, p.Value }),
            ]);
    }

    protected override string? SignInKey => _signInKey;

    public override void Dispose()
    {
        base.Dispose();
        _endpoint.Dispose();
    }

    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        var grant = new List<KeyValuePair<string, string>> { new("grant_type", "client_credentials") };
        grant.AddRange(_options.Params);
        var (token, _) = await _endpoint.RequestAsync(grant, errorHints: null, ct, TerminalErrors).ConfigureAwait(false);
        return token;
    }
}
