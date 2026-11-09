using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>OAuth2 refresh-token settings (RFC 6749 section 6). <see cref="ClientSecret"/> is null for a public
/// client, which then sends only its id in the body. <see cref="Params"/> are extra token-request form fields;
/// the reserved protocol fields cannot be set there. <see cref="IgnoreRotation"/> is <c>rotation: ignore</c>.</summary>
public sealed record OAuth2RefreshTokenOptions(
    Uri TokenUrl,
    string ClientId,
    string? ClientSecret,
    string RefreshToken,
    string? Scope,
    bool BasicClientAuth,
    IReadOnlyDictionary<string, string> Params,
    bool IgnoreRotation = false)
{
    private static readonly string[] ReservedParams = ["grant_type", "client_id", "client_secret", "scope", "refresh_token"];

    /// <summary>Parses the YAML <c>auth:</c> map. Every problem is appended to <paramref name="errors"/>; the
    /// result is null when any was found. The token URL must be https (plain http only on loopback) because the
    /// refresh token travels in the request.</summary>
    public static OAuth2RefreshTokenOptions? TryParse(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        var before = errors.Count;

        var url = OAuth2Fields.TokenUrl(auth, errors);
        var clientId = OAuth2Fields.RequiredText(auth, "client_id", errors);
        var clientSecret = OAuth2Fields.OptionalText(auth, "client_secret", errors);
        var refreshToken = OAuth2Fields.RequiredText(auth, "refresh_token", errors);
        var scope = OAuth2Fields.OptionalText(auth, "scope", errors);
        var basic = OAuth2Fields.BasicClientAuth(auth, errors);
        var extra = OAuth2Fields.Params(auth, ReservedParams, errors);
        if (basic && clientSecret is null)
        {
            errors.Add("auth.client_auth basic needs a client_secret; a public client (no secret) uses client_auth body");
        }

        var ignore = false;
        switch (OAuth2Fields.OptionalText(auth, "rotation", errors))
        {
            case null or "refuse":
                break;
            case "ignore":
                ignore = true;
                break;
            default:
                errors.Add("auth.rotation must be one of: refuse, ignore");
                break;
        }

        return errors.Count > before
            ? null
            : new OAuth2RefreshTokenOptions(url!, clientId!, clientSecret, refreshToken!, scope, basic, extra, ignore);
    }
}

/// <summary>Exchanges a long-lived refresh token for bearer tokens. pz cannot store a rotated refresh token, so
/// a response that issues a different <c>refresh_token</c> fails the read (PZ0366) and its access token is never
/// used, unless <see cref="OAuth2RefreshTokenOptions.IgnoreRotation"/> is set: then the access token is used, the
/// configured refresh token stays in use for later refreshes, and a notice says so. A PZ0366 refusal and an
/// <c>invalid_grant</c> answer are remembered for the process (<see cref="CachedTokenAuthenticator.SignInKey"/>),
/// so a refused refresh token is never replayed. Owns its HttpClient (no redirects). Messages never carry the
/// refresh token, the secret or a token.</summary>
public sealed class OAuth2RefreshTokenAuthenticator : CachedTokenAuthenticator
{
    /// <summary>The PZ code of a rotated refresh token. Connector projects cannot reference Pz.Core's catalog,
    /// so the value is repeated here and must match <c>PzErrorCode.OAuth2RefreshTokenRotated</c>.</summary>
    public const string RotatedCode = "PZ0366";

    private static readonly Dictionary<string, string> Hints = new(StringComparer.Ordinal)
    {
        ["invalid_grant"] = "the refresh token was revoked or has expired; authorize again to obtain a new one and "
            + "update auth.refresh_token",
    };

    private static readonly string[] TerminalErrors = ["invalid_grant"];

    private readonly OAuth2RefreshTokenOptions _options;
    private readonly OAuth2TokenEndpoint _endpoint;
    private readonly string _signInKey;

    /// <param name="handler">The transport for the token request (a proxy, extra trust anchors, a client
    /// certificate); the authenticator owns and disposes it. It must not follow redirects. Null builds the
    /// default handler, which follows none.</param>
    public OAuth2RefreshTokenAuthenticator(OAuth2RefreshTokenOptions options, TimeSpan timeout, TimeProvider time,
        HttpMessageHandler? handler = null)
        : base(time)
    {
        _options = options;
        _endpoint = new OAuth2TokenEndpoint(options.TokenUrl, options.ClientId, options.ClientSecret, options.BasicClientAuth,
            options.Scope, timeout, time, handler);
        _signInKey = Fingerprint(
            [
                "oauth2_refresh_token", options.TokenUrl.AbsoluteUri, options.ClientId, options.ClientSecret, options.RefreshToken,
                options.Scope, options.BasicClientAuth ? "basic" : "body", options.IgnoreRotation ? "ignore" : "refuse",
                .. options.Params.OrderBy(p => p.Key, StringComparer.Ordinal).SelectMany(p => new[] { p.Key, p.Value }),
            ]);
    }

    /// <summary>Where the <c>rotation: ignore</c> notice goes; null drops it. The engine shows each distinct notice
    /// once per run.</summary>
    public Action<string>? Notice { get; set; }

    protected override string? SignInKey => _signInKey;

    public override void Dispose()
    {
        base.Dispose();
        _endpoint.Dispose();
    }

    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        var grant = new List<KeyValuePair<string, string>>
        {
            new("grant_type", "refresh_token"),
            new("refresh_token", _options.RefreshToken),
        };
        grant.AddRange(_options.Params);
        var (token, response) = await _endpoint.RequestAsync(grant, Hints, ct, TerminalErrors, [_options.RefreshToken])
            .ConfigureAwait(false);
        if (!Rotated(response))
        {
            return token;
        }

        if (_options.IgnoreRotation)
        {
            Notice?.Invoke(
                $"token endpoint {_endpoint.Endpoint} issued a new refresh_token; auth.rotation is ignore, so pz keeps using "
                + "the configured auth.refresh_token. If the provider revokes it, the next refresh fails with invalid_grant");
            return token;
        }

        throw Terminal(new PzConnectorException(
            $"{RotatedCode}: token endpoint {_endpoint.Endpoint} issued a new refresh_token (it rotates refresh tokens). "
            + "pz cannot store a rotated token, and the configured auth.refresh_token may now be revoked, so the next run "
            + "would fail too; the new access token was not used. If the provider keeps old refresh tokens valid (Microsoft "
            + "Entra and Google do), set auth.rotation: ignore; otherwise configure the app or provider to issue "
            + "non-rotating refresh tokens, then authorize again and update auth.refresh_token",
            isTransient: false));
    }

    private bool Rotated(JsonObject response)
    {
        try
        {
            return response["refresh_token"] is JsonValue issued && issued.TryGetValue<string>(out var next)
                && !string.IsNullOrEmpty(next) && !string.Equals(next, _options.RefreshToken, StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // A duplicated refresh_token key: which one is meant cannot be known, so the answer is not used.
            throw new PzConnectorException(
                $"token endpoint {_endpoint.Endpoint} answered with a JSON response that could not be read (duplicate keys)",
                isTransient: false);
        }
    }
}
