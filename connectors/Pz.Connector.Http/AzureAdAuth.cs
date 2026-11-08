using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Azure;
using Azure.Core;
using Azure.Identity;
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

    public TokenCredential CreateCredential()
    {
        switch (Method)
        {
            case "service_principal" when CertificatePath is { } path:
                var certOptions = new ClientCertificateCredentialOptions();
                if (AuthorityHost is not null)
                {
                    certOptions.AuthorityHost = AuthorityHost;
                }

                // The path overload cannot take a password, so a protected file is loaded here.
                return CertificatePassword is { } pfxPassword
                    ? new ClientCertificateCredential(TenantId, ClientId, X509CertificateLoader.LoadPkcs12FromFile(path, pfxPassword), certOptions)
                    : new ClientCertificateCredential(TenantId, ClientId, path, certOptions);
            case "service_principal":
                var secretOptions = new ClientSecretCredentialOptions();
                if (AuthorityHost is not null)
                {
                    secretOptions.AuthorityHost = AuthorityHost;
                }

                return new ClientSecretCredential(TenantId, ClientId, ClientSecret, secretOptions);
            case "managed_identity":
                // Managed identity talks to the local identity endpoint; an authority host does not apply.
                return ClientId is { } id
                    ? new ManagedIdentityCredential(ManagedIdentityId.FromUserAssignedClientId(id))
                    : new ManagedIdentityCredential();
            default:
                var chainOptions = new DefaultAzureCredentialOptions();
                if (AuthorityHost is not null)
                {
                    chainOptions.AuthorityHost = AuthorityHost;
                }

                return new DefaultAzureCredential(chainOptions);
        }
    }

    private static string? Text(IReadOnlyDictionary<string, object?> auth, string key)
        => auth.TryGetValue(key, out var raw) && YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
}

/// <summary>Fetches bearer tokens from any Azure.Identity credential. Failures are reported through
/// <see cref="AzureAdAuth.Classify"/>.</summary>
internal sealed class AzureAdAuthenticator(TokenCredential credential, string scope, TimeProvider time, string method = "")
    : CachedTokenAuthenticator(time)
{
    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        try
        {
            var token = await credential.GetTokenAsync(new TokenRequestContext([scope]), ct).ConfigureAwait(false);
            return new FetchedToken(token.Token, token.ExpiresOn);
        }
        catch (Exception ex) when (ex is AuthenticationFailedException or CredentialUnavailableException or RequestFailedException)
        {
            throw AzureAdAuth.Classify(ex, method);
        }
    }
}

internal static partial class AzureAdAuth
{
    [GeneratedRegex(@"AADSTS\d+")]
    private static partial Regex AadstsCode();

    /// <summary>Maps an Azure.Identity failure to a connector exception. The SDK message can carry trace
    /// and correlation IDs, URLs and server text, so only the first AADSTS code and fixed hints are used.</summary>
    internal static PzConnectorException Classify(Exception ex, string method)
    {
        if (ex is CredentialUnavailableException)
        {
            var hint = method == "managed_identity"
                ? "no managed identity is available in this environment; use method credential_chain to also try developer and CLI credentials"
                : "no credential in the chain could be used; check the environment, or set method to service_principal";
            return new PzConnectorException($"azure_ad sign-in unavailable ({method}): {hint}", isTransient: false);
        }

        for (var inner = ex; inner is not null; inner = inner.InnerException)
        {
            if (inner is RequestFailedException { Status: 408 or 429 or >= 500 and <= 599 } || inner is HttpRequestException)
            {
                return new PzConnectorException($"azure_ad token request failed temporarily ({method}); it will be retried", isTransient: true);
            }
        }

        var code = AadstsCode().Match(ex.Message) is { Success: true } m ? $" {m.Value}" : "";
        return new PzConnectorException(
            $"azure_ad sign-in failed ({method}){code}: check tenant_id, client_id and the secret or certificate", isTransient: false);
    }
}
