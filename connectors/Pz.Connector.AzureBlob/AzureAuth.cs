using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>One config, two clients: `auth` selects the method; each method maps to exactly one DuckDB
/// CREATE SECRET shape (here) and one Azure SDK client. Required-field checks are offline and aggregate.
/// Every secret literal is single-quote-escaped.</summary>
internal static class AzureAuth
{
    private static readonly string[] Methods =
        ["connection_string", "account_key", "service_principal", "credential_chain", "managed_identity"];

    public static ValidationResult Validate(ConnectorConfig config)
    {
        var errors = new List<string>();
        var auth = config.GetString("auth");
        if (string.IsNullOrEmpty(auth))
        {
            return ValidationResult.Failed("azure connection requires 'auth' (one of: " + string.Join(", ", Methods) + ")");
        }

        if (!Methods.Contains(auth))
        {
            return ValidationResult.Failed($"azure connection 'auth' must be one of {string.Join(", ", Methods)} (got '{auth}')");
        }

        foreach (var field in RequiredFields(auth))
        {
            if (string.IsNullOrEmpty(config.GetString(field)))
            {
                errors.Add($"azure '{auth}' auth requires '{field}'");
            }
        }

        return errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]);
    }

    private static string[] RequiredFields(string auth) => auth switch
    {
        "connection_string" => ["connection_string"],
        "account_key" => ["account_name", "account_key"],
        "service_principal" => ["tenant_id", "client_id", "client_secret", "account_name"],
        "credential_chain" => ["account_name"],
        "managed_identity" => ["account_name"],
        _ => [],
    };

    public static string CreateSecretSql(ConnectorConfig config, string secretName)
    {
        var auth = config.GetString("auth");
        var body = auth switch
        {
            "connection_string" => $"connection_string '{E(config.GetString("connection_string"))}'",
            "account_key" => $"connection_string '{E(AssembleConnectionString(config))}'",
            "service_principal" =>
                $"provider service_principal, tenant_id '{E(config.GetString("tenant_id"))}', " +
                $"client_id '{E(config.GetString("client_id"))}', client_secret '{E(config.GetString("client_secret"))}', " +
                $"account_name '{E(config.GetString("account_name"))}'",
            "credential_chain" => "provider credential_chain" +
                (config.GetString("chain") is { Length: > 0 } chain ? $", chain '{E(chain)}'" : "") +
                $", account_name '{E(config.GetString("account_name"))}'",
            "managed_identity" => "provider managed_identity" +
                $", account_name '{E(config.GetString("account_name"))}'" +
                (config.GetString("client_id") is { Length: > 0 } cid ? $", client_id '{E(cid)}'" : ""),
            _ => throw new PzConnectorException($"azure: unsupported auth '{auth}'", isTransient: false),
        };

        return $"create or replace secret {secretName} (type azure, {body})";
    }

    /// <summary>The universal blob-stream tier's per-container client (<see cref="AzureParquetReader"/>
    /// et al) -- see <see cref="CreateBlobServiceClient"/> for the auth→client mapping.</summary>
    public static BlobContainerClient CreateBlobContainerClient(ConnectorConfig config, string container) =>
        CreateBlobServiceClient(config).GetBlobContainerClient(container);

    /// <summary>The account-level client underlying <see cref="CreateBlobContainerClient"/> -- also the
    /// one <see cref="AzureConnector.CheckConnectionAsync"/> probes directly (a service-level call needs
    /// no container name, so it works for a sink-only connection that never declares one). Same
    /// method→credential mapping as <see cref="CreateSecretSql"/>: connection_string / account_key both
    /// flow through a connection string (account_key reusing the exact <see cref="AssembleConnectionString"/>
    /// the secret path uses -- one helper, no duplication); service_principal / credential_chain /
    /// managed_identity build a service URI (<c>endpoint</c> override, else
    /// <c>https://&lt;account_name&gt;.blob.core.windows.net</c>) plus an <see cref="Azure.Core.TokenCredential"/>.</summary>
    public static BlobServiceClient CreateBlobServiceClient(ConnectorConfig config)
    {
        var auth = config.GetString("auth");
        try
        {
            return auth switch
            {
                "connection_string" => new BlobServiceClient(config.GetString("connection_string")),
                "account_key" => new BlobServiceClient(AssembleConnectionString(config)),
                "service_principal" => new BlobServiceClient(BlobServiceUri(config), ClientSecret(config)),
                "credential_chain" => new BlobServiceClient(BlobServiceUri(config), new DefaultAzureCredential()),
                "managed_identity" => new BlobServiceClient(BlobServiceUri(config), ManagedIdentity(config)),
                _ => throw new PzConnectorException($"azure: unsupported auth '{auth}'", isTransient: false),
            };
        }
        catch (Exception ex) when (ex is FormatException or UriFormatException)
        {
            throw new PzConnectorException(
                $"azure '{MalformedField(config, auth)}' is malformed: {ex.Message}", isTransient: false, innerException: ex);
        }
    }

    /// <summary>The ADLS Gen2 sibling of <see cref="CreateBlobContainerClient"/> for <c>abfss://</c>
    /// listing. Same auth mapping; every non-connection-string arm (including <c>account_key</c>) targets
    /// the <c>dfs</c> endpoint (<c>https://&lt;account_name&gt;.dfs.core.windows.net</c>, or <c>endpoint</c>
    /// when set) directly via a client/credential pair rather than a connection string:
    /// <see cref="AssembleConnectionString"/> bakes in <c>BlobEndpoint=&lt;endpoint&gt;</c> when a custom
    /// <c>endpoint</c> is configured, so a <see cref="DataLakeServiceClient"/> built from it would target
    /// the wrong (blob, not dfs) host. Building from <see cref="StorageSharedKeyCredential"/> +
    /// <see cref="DfsServiceUri"/> instead (matching the three token-credential arms) sidesteps that.</summary>
    public static DataLakeFileSystemClient CreateDataLakeFileSystemClient(ConnectorConfig config, string fileSystem)
    {
        var auth = config.GetString("auth");
        try
        {
            return auth switch
            {
                "connection_string" =>
                    new DataLakeServiceClient(config.GetString("connection_string")).GetFileSystemClient(fileSystem),
                "account_key" =>
                    new DataLakeServiceClient(DfsServiceUri(config), SharedKeyCredential(config)).GetFileSystemClient(fileSystem),
                "service_principal" =>
                    new DataLakeServiceClient(DfsServiceUri(config), ClientSecret(config)).GetFileSystemClient(fileSystem),
                "credential_chain" =>
                    new DataLakeServiceClient(DfsServiceUri(config), new DefaultAzureCredential()).GetFileSystemClient(fileSystem),
                "managed_identity" =>
                    new DataLakeServiceClient(DfsServiceUri(config), ManagedIdentity(config)).GetFileSystemClient(fileSystem),
                _ => throw new PzConnectorException($"azure: unsupported auth '{auth}'", isTransient: false),
            };
        }
        catch (Exception ex) when (ex is FormatException or UriFormatException)
        {
            throw new PzConnectorException(
                $"azure '{MalformedField(config, auth)}' is malformed: {ex.Message}", isTransient: false, innerException: ex);
        }
    }

    /// <summary>Names the config field whose malformed value a <see cref="FormatException"/>/
    /// <see cref="UriFormatException"/> out of client construction most likely traces back to -- never the
    /// value itself (secret hygiene): connection_string/account_key both funnel through a connection
    /// string; everything else builds a service <see cref="Uri"/> from `endpoint` when set, else
    /// `account_name` (see <see cref="ServiceUri"/>) -- so blame whichever of those two actually fed the
    /// URI, not `endpoint` unconditionally.</summary>
    private static string MalformedField(ConnectorConfig config, string? auth) => auth switch
    {
        "connection_string" or "account_key" => "connection_string",
        _ => config.GetString("endpoint") is { Length: > 0 } ? "endpoint" : "account_name",
    };

    private static ClientSecretCredential ClientSecret(ConnectorConfig config) => new(
        config.GetString("tenant_id"), config.GetString("client_id"), config.GetString("client_secret"));

    private static StorageSharedKeyCredential SharedKeyCredential(ConnectorConfig config) =>
        new(config.GetString("account_name"), config.GetString("account_key"));

    // ManagedIdentityCredential(string, TokenCredentialOptions) is obsolete as of the Azure.Identity
    // bump that came with Azure.Storage.Blobs 12.29.1's Azure.Core floor; ManagedIdentityId is the
    // replacement shape, same client-id-or-system-assigned behavior.
    private static ManagedIdentityCredential ManagedIdentity(ConnectorConfig config) =>
        new(config.GetString("client_id") is { Length: > 0 } clientId
            ? ManagedIdentityId.FromUserAssignedClientId(clientId)
            : ManagedIdentityId.SystemAssigned);

    private static Uri BlobServiceUri(ConnectorConfig config) => ServiceUri(config, "blob");

    private static Uri DfsServiceUri(ConnectorConfig config) => ServiceUri(config, "dfs");

    private static Uri ServiceUri(ConnectorConfig config, string service) =>
        config.GetString("endpoint") is { Length: > 0 } endpoint
            ? new Uri(endpoint)
            : new Uri($"https://{config.GetString("account_name")}.{service}.core.windows.net");

    private static string AssembleConnectionString(ConnectorConfig config)
    {
        var name = config.GetString("account_name");
        var key = config.GetString("account_key");
        var suffix = config.GetString("endpoint") is { Length: > 0 } ep
            ? $";BlobEndpoint={ep}"
            : ";EndpointSuffix=core.windows.net";
        return $"DefaultEndpointsProtocol=https;AccountName={name};AccountKey={key}{suffix}";
    }

    public static string SecretName(string subject) =>
        "pz_azure_" + new string([.. subject.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    private static string E(string? value) => AzureUrl.Escape(value ?? "");
}
