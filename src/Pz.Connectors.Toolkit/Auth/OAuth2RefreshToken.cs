using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>OAuth2 refresh-token settings (RFC 6749 section 6). <see cref="ClientSecret"/> is null for a public
/// client, which then sends only its id in the body. <see cref="Params"/> are extra token-request form fields;
/// the reserved protocol fields cannot be set there.</summary>
public sealed record OAuth2RefreshTokenOptions(
    Uri TokenUrl,
    string ClientId,
    string? ClientSecret,
    string RefreshToken,
    string? Scope,
    bool BasicClientAuth,
    IReadOnlyDictionary<string, string> Params)
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

        return errors.Count > before
            ? null
            : new OAuth2RefreshTokenOptions(url!, clientId!, clientSecret, refreshToken!, scope, basic, extra);
    }
}

/// <summary>Exchanges a long-lived refresh token for bearer tokens. Only non-rotating refresh tokens are
/// supported: a response that issues a different <c>refresh_token</c> fails the read (PZ0366) and its access
/// token is never used, because the new refresh token cannot be stored and the configured one may already be
/// revoked. Owns its HttpClient (no redirects). Messages never carry the refresh token, secret or token.</summary>
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

    private readonly OAuth2RefreshTokenOptions _options;
    private readonly OAuth2TokenEndpoint _endpoint;

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
    }

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
        var (token, response) = await _endpoint.RequestAsync(grant, Hints, ct).ConfigureAwait(false);
        if (response["refresh_token"] is JsonValue issued && issued.TryGetValue<string>(out var next)
            && !string.IsNullOrEmpty(next) && !string.Equals(next, _options.RefreshToken, StringComparison.Ordinal))
        {
            throw new PzConnectorException(
                $"{RotatedCode}: token endpoint {_endpoint.Endpoint} issued a new refresh_token (it rotates refresh tokens). "
                + "pz cannot store a rotated token, and the configured auth.refresh_token may now be revoked, so the next run "
                + "would fail too; the new access token was not used. Configure the app or provider to issue non-rotating "
                + "refresh tokens (some providers, such as Microsoft Entra, return a new value on every refresh), then "
                + "authorize again and update auth.refresh_token",
                isTransient: false);
        }

        return token;
    }
}
