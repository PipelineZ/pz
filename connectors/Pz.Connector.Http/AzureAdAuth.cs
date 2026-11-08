using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Azure;
using Azure.Core;
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
    public static AzureAdOptions? TryParse(IReadOnlyDictionary<string, object?> auth, string? baseDir, List<string> errors)
    {
        var before = errors.Count;

        var method = Text(auth, "method");
        if (method is null)
        {
            errors.Add($"auth.method is required (one of: {string.Join(", ", Methods)})");
        }
        else if (!Methods.Contains(method))
        {
            errors.Add($"auth.method must be one of: {string.Join(", ", Methods)}");
        }

        var scope = Text(auth, "scope");
        if (scope is null)
        {
            errors.Add("auth.scope is required (e.g. 'https://graph.microsoft.com/.default')");
        }

        var tenantId = Text(auth, "tenant_id");
        var clientId = Text(auth, "client_id");
        var secret = Text(auth, "client_secret");
        var certificate = Text(auth, "certificate_path");
        var password = Text(auth, "certificate_password");

        if (method == "service_principal")
        {
            if (tenantId is null)
            {
                errors.Add("auth.tenant_id is required for method service_principal");
            }

            if (clientId is null)
            {
                errors.Add("auth.client_id is required for method service_principal");
            }

            if ((secret is null) == (certificate is null))
            {
                errors.Add("auth needs exactly one of client_secret or certificate_path for method service_principal");
            }
        }

        Uri? authority = null;
        if (Text(auth, "authority_host") is { } host)
        {
            if (!Uri.TryCreate(host, UriKind.Absolute, out authority) || authority.Scheme != Uri.UriSchemeHttps)
            {
                errors.Add("auth.authority_host must be an absolute https URL");
                authority = null;
            }
        }

        return errors.Count > before
            ? null
            : new AzureAdOptions(method!, tenantId, clientId, secret, ProjectRelativePath.Resolve(certificate, baseDir), password, scope!, authority);
    }

    /// <summary>Non-blocking <c>pz validate</c> diagnostics for these options.</summary>
    public IReadOnlyList<string> Warnings()
        => Scope.EndsWith("/.default", StringComparison.OrdinalIgnoreCase)
            ? []
            : [$"auth.scope '{Scope}' does not end in '/.default'; the client-credentials grant accepts only the "
                + "'<resource>/.default' form, e.g. 'https://graph.microsoft.com/.default'"];

    /// <summary>The options every credential is built from. Connectors never retry internally (the engine retries
    /// the transient exception), and a black-holed authority must fail within the connection timeout.</summary>
    internal TokenCredentialOptions CreateCredentialOptions(TimeSpan timeout)
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
        return options;
    }

    /// <summary>Builds the Azure.Identity credential. <paramref name="owned"/> is the loaded certificate
    /// (null unless a password-protected certificate was loaded); the caller disposes it with the credential.</summary>
    public TokenCredential CreateCredential(TimeSpan timeout, out IDisposable? owned)
    {
        owned = null;
        var options = CreateCredentialOptions(timeout);
        switch (options)
        {
            case ClientCertificateCredentialOptions certOptions:
                var path = CertificatePath!;
                if (CertificatePassword is not { } pfxPassword)
                {
                    return new ClientCertificateCredential(TenantId, ClientId, path, certOptions);
                }

                var certificate = LoadProtectedCertificate(path, pfxPassword);
                owned = certificate;
                return new ClientCertificateCredential(TenantId, ClientId, certificate, certOptions);
            case ClientSecretCredentialOptions secretOptions:
                return new ClientSecretCredential(TenantId, ClientId, ClientSecret, secretOptions);
            case ManagedIdentityCredentialOptions identityOptions:
                return new ManagedIdentityCredential(identityOptions);
            default:
                return new DefaultAzureCredential((DefaultAzureCredentialOptions)options);
        }
    }

    /// <summary>The authenticator for these options; it owns and disposes any certificate it loaded.</summary>
    public AzureAdAuthenticator CreateAuthenticator(TimeSpan timeout, TimeProvider time)
    {
        var credential = CreateCredential(timeout, out var owned);
        return new AzureAdAuthenticator(credential, Scope, Method, time, owned);
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
                isTransient: false);
        }
    }

    private static string? Text(IReadOnlyDictionary<string, object?> auth, string key)
        => auth.TryGetValue(key, out var raw) && YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
}

/// <summary>Fetches bearer tokens from any Azure.Identity credential. Failures are reported through
/// <see cref="AzureAdAuth.Classify"/>. <paramref name="owned"/> (a loaded certificate) is disposed with it.</summary>
internal sealed class AzureAdAuthenticator(TokenCredential credential, string scope, string method, TimeProvider time, IDisposable? owned = null)
    : CachedTokenAuthenticator(time)
{
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
            throw AzureAdAuth.Classify(ex, method, scope);
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
