using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Azure;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Microsoft.Identity.Client;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http;

/// <summary>Microsoft Entra ID (Azure AD) sign-in settings. <see cref="CertificatePath"/> is already
/// resolved against the project's <c>base_dir</c>.</summary>
internal sealed record AzureAdOptions(
    string Method,
    string? TenantId,
    string? ClientId,
    string? ClientSecret,
    string? CertificatePath,
    string? CertificatePassword,
    string Scope,
    Uri? AuthorityHost)
{
    private static readonly string[] Methods = ["service_principal", "managed_identity", "credential_chain"];

    /// <summary>Parses the YAML <c>auth:</c> map. Every problem is appended to <paramref name="errors"/>;
    /// the result is null when any was found.</summary>
    public static AzureAdOptions? TryParse(IReadOnlyDictionary<string, object?> auth, string? baseDir, List<string> errors, bool hasTransport = false)
    {
        var before = errors.Count;
        var refused = new HashSet<string>();
        string? Text(string key)
        {
            auth.TryGetValue(key, out var raw);
            if (YamlScalarText.NonScalarKind(raw) is { } kind)
            {
                errors.Add($"auth.{key} must be a single value, not a {kind}");
                refused.Add(key);
                return null;
            }

            return YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
        }

        var method = Text("method");
        if (method is null && !refused.Contains("method"))
        {
            errors.Add($"auth.method is required (one of: {string.Join(", ", Methods)})");
        }
        else if (method is not null && !Methods.Contains(method))
        {
            errors.Add($"auth.method must be one of: {string.Join(", ", Methods)}");
        }

        if (method == "credential_chain" && hasTransport)
        {
            errors.Add("auth.method credential_chain cannot use proxy or tls: its developer-tool credentials run outside pz");
        }

        var scope = Text("scope");
        if (scope is null && !refused.Contains("scope"))
        {
            errors.Add("auth.scope is required (e.g. 'https://graph.microsoft.com/.default')");
        }

        var tenantId = Text("tenant_id");
        var clientId = Text("client_id");
        var secret = Text("client_secret");
        var certificate = Text("certificate_path");
        var password = Text("certificate_password");

        if (method == "service_principal")
        {
            if (tenantId is null && !refused.Contains("tenant_id"))
            {
                errors.Add("auth.tenant_id is required for method service_principal");
            }
            else if (tenantId is not null && !IsValidTenantId(tenantId))
            {
                errors.Add($"auth.tenant_id '{tenantId}' is not a valid tenant id (a GUID or a domain such as contoso.onmicrosoft.com)");
            }

            if (clientId is null && !refused.Contains("client_id"))
            {
                errors.Add("auth.client_id is required for method service_principal");
            }

            if ((secret is null) == (certificate is null) && !refused.Contains("client_secret") && !refused.Contains("certificate_path"))
            {
                errors.Add("auth needs exactly one of client_secret or certificate_path for method service_principal");
            }
        }

        Uri? authority = null;
        if (Text("authority_host") is { } host)
        {
            if (!Uri.TryCreate(host, UriKind.Absolute, out authority) || authority.Scheme != Uri.UriSchemeHttps)
            {
                errors.Add("auth.authority_host must be an absolute https URL");
                authority = null;
            }
        }

        return errors.Count > before
            ? null
            : new AzureAdOptions(method!, tenantId, clientId, secret, ProjectRelativePath.Resolve(HomePath.Expand(certificate), baseDir), password, scope!, authority);
    }

    // The credential library accepts exactly ASCII letters, ASCII digits, '-' and '.' (so GUIDs, domains and the
    // well-known 'common'/'organizations'/'consumers' all pass) and throws on anything else. Only service_principal
    // reads tenant_id; the other methods never hand it to the library.
    private static bool IsValidTenantId(string tenantId)
        => tenantId.All(c => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '-' or '.');

    /// <summary>What a refused service-principal sign-in is remembered under for the run (endpoint, tenant, client and
    /// secret or certificate, fingerprinted, never stored); null for managed identity and the credential chain, whose
    /// failures are about the machine they run on, and send no secret an identity provider could count against an
    /// account.</summary>
    public string?[]? SignInKeyParts => Method == "service_principal"
        ? ["azure_ad", Method, AuthorityHost?.AbsoluteUri, TenantId, ClientId, ClientSecret, CertificatePath,
            CertificatePassword, Scope]
        : null;

    /// <summary>Non-blocking <c>pz validate</c> diagnostics for these options.</summary>
    public IReadOnlyList<string> Warnings()
        => Scope.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? []
            : [$"auth.scope '{Scope}' does not end in '/.default'; the client-credentials grant accepts only the "
                + "'<resource>/.default' form, e.g. 'https://graph.microsoft.com/.default'"];

    /// <summary>The options every credential is built from. Connectors never retry internally (the engine retries
    /// the transient exception), and a black-holed authority must fail within the connection timeout.
    /// <paramref name="handlers"/> (the connection's 'proxy'/'tls' handler source, null without either) becomes
    /// service_principal's transport only. managed_identity talks to a local endpoint that a proxy must not see
    /// (169.254.169.254 is link-local), and credential_chain runs developer tools outside pz, which validation
    /// already refuses together with 'proxy'/'tls'.</summary>
    internal TokenCredentialOptions CreateCredentialOptions(TimeSpan timeout, Func<Uri, HttpMessageHandler>? handlers = null)
    {
        TokenCredentialOptions options = Method switch
        {
            "service_principal" when CertificatePath is not null => new ClientCertificateCredentialOptions(),
            "service_principal" => new ClientSecretCredentialOptions(),
            "managed_identity" => new ManagedIdentityCredentialOptions(
                ClientId is { } id ? ManagedIdentityId.FromUserAssignedClientId(id) : ManagedIdentityId.SystemAssigned),
            _ => new DefaultAzureCredentialOptions(),
        };
        // Managed identity talks to the local identity endpoint; an authority host does not apply.
        if (AuthorityHost is not null && Method != "managed_identity")
        {
            options.AuthorityHost = AuthorityHost;
        }

        options.Retry.MaxRetries = 0;
        options.Retry.NetworkTimeout = timeout;
        if (handlers is not null && Method == "service_principal")
        {
            options.Transport = new HttpClientTransport(handlers(AuthorityHost ?? AzureAuthorityHosts.AzurePublicCloud));
        }

        return options;
    }

    /// <summary>Builds the Azure.Identity credential. <paramref name="owned"/> is the loaded certificate
    /// (null unless a password-protected certificate was loaded); the caller disposes it with the credential.
    /// When construction is refused it is already disposed and a <see cref="PzConnectorException"/> is thrown.
    /// A transport built from <paramref name="handlers"/> is owned the same way.</summary>
    public TokenCredential CreateCredential(TimeSpan timeout, out IDisposable? owned,
        Func<Uri, HttpMessageHandler>? handlers = null)
    {
        var options = CreateCredentialOptions(timeout, handlers);
        // Only a transport built from the handler source is owned; the library's shared default never is.
        owned = handlers is not null && Method == "service_principal" ? options.Transport as IDisposable : null;
        try
        {
            switch (options)
            {
                case ClientCertificateCredentialOptions certOptions:
                    var path = CertificatePath!;
                    if (CertificatePassword is not { } pfxPassword)
                    {
                        return new ClientCertificateCredential(TenantId, ClientId, path, certOptions);
                    }

                    var certificate = LoadProtectedCertificate(path, pfxPassword);
                    owned = owned is null ? certificate : new DisposeBoth(certificate, owned);
                    return new ClientCertificateCredential(TenantId, ClientId, certificate, certOptions);
                case ClientSecretCredentialOptions secretOptions:
                    return new ClientSecretCredential(TenantId, ClientId, ClientSecret, secretOptions);
                case ManagedIdentityCredentialOptions identityOptions:
                    return new ManagedIdentityCredential(identityOptions);
                default:
                    return new DefaultAzureCredential((DefaultAzureCredentialOptions)options);
            }
        }
        catch (Exception ex)
        {
            // The caller never receives a credential to own them, so a loaded certificate and a transport
            // built above are released here, whatever refused construction.
            owned?.Dispose();
            if (ex is ArgumentException)
            {
                // The library's text can echo the refused settings; only the auth block is named.
                throw new PzConnectorException(
                    "azure_ad: the credential could not be created from the auth settings (check tenant_id, client_id and authority_host)",
                    isTransient: false);
            }

            throw;
        }
    }

    /// <summary>The authenticator for these options; it owns and disposes any certificate it loaded.</summary>
    public AzureAdAuthenticator CreateAuthenticator(TimeSpan timeout, TimeProvider time,
        Func<Uri, HttpMessageHandler>? handlers = null)
    {
        var credential = CreateCredential(timeout, out var owned, handlers);
        return new AzureAdAuthenticator(credential, Scope, Method, time, owned, SignInKeyParts);
    }

    // The path overload of the credential cannot take a password, so the file is loaded here. PEM text means
    // an encrypted PEM; anything else is PKCS#12. Ephemeral keys keep the private key off disk.
    private static X509Certificate2 LoadProtectedCertificate(string path, string password)
    {
        try
        {
            using var reader = new StreamReader(path);
            var buffer = new char[16];
            var read = reader.ReadBlock(buffer, 0, buffer.Length);
            return new string(buffer, 0, read).StartsWith("-----BEGIN", StringComparison.Ordinal)
                ? X509Certificate2.CreateFromEncryptedPemFile(path, password)
                : X509CertificateLoader.LoadPkcs12FromFile(path, password, X509KeyStorageFlags.EphemeralKeySet);
        }
        catch (Exception ex) when (ex is CryptographicException or IOException or UnauthorizedAccessException or ArgumentException)
        {
            // The framework text can echo paths and key details; only the config field is named.
            throw new PzConnectorException(
                "auth.certificate_path could not be loaded (a missing file, an unreadable file or a wrong certificate_password; "
                + "a password-protected certificate must be PKCS#12 or an encrypted PEM)",
                isTransient: false, code: PzConnectorErrorCodes.Config);
        }
    }
}

/// <summary>Fetches bearer tokens from any Azure.Identity credential. Failures are reported through
/// <see cref="AzureAdAuth.Classify"/>. <paramref name="owned"/> (a loaded certificate) is disposed with it.</summary>
/// <remarks>With <paramref name="signInKey"/> (a service principal), a refusal of the client's credentials
/// (<see cref="AzureAdAuth.IsCredentialRefusal"/>) is remembered for the run: the other nodes on the connection fail at
/// once instead of each sending the same wrong secret to Entra.</remarks>
internal sealed class AzureAdAuthenticator(TokenCredential credential, string scope, string method, TimeProvider time,
    IDisposable? owned = null, string?[]? signInKey = null)
    : CachedTokenAuthenticator(time)
{
    private readonly string? _signInKey = signInKey is null ? null : Fingerprint(signInKey);

    protected override string? SignInKey => _signInKey;

    public override void Dispose()
    {
        base.Dispose();
        owned?.Dispose();
    }

    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        try
        {
            var token = await credential.GetTokenAsync(new TokenRequestContext([scope]), ct).ConfigureAwait(false);
            return new FetchedToken(token.Token, token.ExpiresOn);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested
            && ex is AuthenticationFailedException or CredentialUnavailableException or RequestFailedException or TimeoutException or TaskCanceledException)
        {
            var failure = AzureAdAuth.Classify(ex, method, scope);
            throw !failure.IsTransient && _signInKey is not null && AzureAdAuth.IsCredentialRefusal(ex)
                ? Terminal(failure)
                : failure;
        }
        catch (ArgumentException) when (!ct.IsCancellationRequested)
        {
            // The SDK rejects malformed settings (e.g. a bad tenant_id) with an exception whose text can echo them.
            throw new PzConnectorException(
                $"azure_ad sign-in failed ({method}): the credential settings were refused; check tenant_id, client_id and scope",
                isTransient: false);
        }
    }
}

internal static partial class AzureAdAuth
{
    [GeneratedRegex(@"AADSTS\d+")]
    private static partial Regex AadstsCode();

    // Entra's "this scope/resource is not valid for the client-credentials grant" answers.
    [GeneratedRegex(@"AADSTS(?:70011|1002012|500011)(?!\d)")]
    private static partial Regex InvalidScopeCode();

    // Entra's answers that the client's own credentials are wrong for good: an invalid or expired secret, an unknown
    // application or tenant, a certificate assertion that does not verify, a disabled application. A clock-skewed
    // assertion (AADSTS700024) and throttling are not among them.
    [GeneratedRegex(@"AADSTS(?:7000215|7000222|700016|90002|700027|7000112)(?!\d)")]
    private static partial Regex CredentialRefusalCode();

    /// <summary>True when <paramref name="ex"/> (anywhere in its chain) says the client's credentials were refused:
    /// an MSAL <c>invalid_client</c>/<c>unauthorized_client</c> error or one of the AADSTS codes above.</summary>
    internal static bool IsCredentialRefusal(Exception ex)
    {
        for (var inner = ex; inner is not null; inner = inner.InnerException)
        {
            if (inner is MsalServiceException { ErrorCode: "invalid_client" or "unauthorized_client" }
                || CredentialRefusalCode().IsMatch(inner.Message))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Maps an Azure.Identity failure to a connector exception. The SDK message can carry trace
    /// and correlation IDs, URLs and server text, so only the first AADSTS code, the configured scope and fixed hints are used.</summary>
    internal static PzConnectorException Classify(Exception ex, string method, string scope)
    {
        if (ex is CredentialUnavailableException)
        {
            return new PzConnectorException(
                method == "managed_identity"
                    ? "no managed identity is available here (are you running outside Azure? use `credential_chain` to fall back to `az login`)"
                    : $"azure_ad sign-in unavailable ({method}): no credential could be used; check the environment, or use method service_principal",
                isTransient: false);
        }

        for (var inner = ex; inner is not null; inner = inner.InnerException)
        {
            if (inner is RequestFailedException { Status: 408 or 429 or >= 500 and <= 599 }
                or MsalServiceException { StatusCode: 408 or 429 or >= 500 and <= 599 }
                or MsalServiceException { ErrorCode: "request_timeout" or "service_not_available" }
                or HttpRequestException or TimeoutException or TaskCanceledException)
            {
                return new PzConnectorException($"azure_ad token request failed temporarily ({method}); it will be retried", isTransient: true);
            }
        }

        for (var inner = ex; inner is not null; inner = inner.InnerException)
        {
            var scopeCode = InvalidScopeCode().Match(inner.Message);
            if (inner is MsalServiceException { ErrorCode: "invalid_scope" or "invalid_resource" } || scopeCode.Success)
            {
                // The scope is configuration, not a secret, so it is safe to echo.
                return new PzConnectorException(
                    $"azure_ad sign-in refused scope '{scope}'{(scopeCode.Success ? $" ({scopeCode.Value})" : "")}: "
                    + "use the API's '<resource>/.default' scope, e.g. 'https://graph.microsoft.com/.default'",
                    isTransient: false);
            }
        }

        var code = AadstsCode().Match(ex.Message) is { Success: true } m ? $" {m.Value}" : "";
        return new PzConnectorException(
            $"azure_ad sign-in failed ({method}){code}: check tenant_id, client_id and the secret or certificate", isTransient: false);
    }
}
