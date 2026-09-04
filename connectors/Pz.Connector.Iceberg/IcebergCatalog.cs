using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>The catalog key matrix: which connection keys each catalog requires and which it
/// forbids. Validation is offline and aggregate — every stray or missing key is one error naming
/// the catalog it belongs to — so an author fixes a whole block in one pass.</summary>
internal static class IcebergCatalog
{
    internal const string Rest = "rest";
    internal const string Glue = "glue";
    internal const string S3Tables = "s3_tables";
    internal const string Files = "files";

    internal const string StorageS3 = "s3";
    internal const string StorageAzure = "azure";

    private static readonly string[] RestAuthKeys = ["token", "client_id", "client_secret", "oauth2_server_uri", "oauth2_scope"];
    private static readonly string[] StorageKeysNeedingCredentials = ["storage_endpoint", "storage_url_style", "storage_use_ssl"];

    internal static readonly string[] AzureAuthMethods = ["connection_string", "account_key", "service_principal", "credential_chain"];

    private static readonly string[] S3OnlyKeys = ["storage_key_id", "storage_secret_key", "storage_region", "storage_url_style", "storage_use_ssl"];
    private static readonly string[] AzureOnlyKeys =
        ["storage_auth", "storage_connection_string", "storage_account_name", "storage_account_key",
         "storage_tenant_id", "storage_client_id", "storage_client_secret", "storage_chain"];

    internal static string Of(ConnectorConfig config) => config.GetString("catalog") ?? Rest;

    internal static bool IsAws(string catalog) => catalog is Glue or S3Tables;

    /// <summary>True when the connection carries a catalog credential (a bearer token or an OAuth2
    /// client pair) that must ride a <c>type iceberg</c> secret; false attaches with
    /// <c>authorization_type 'none'</c>.</summary>
    internal static bool HasCatalogCredentials(ConnectorConfig config) =>
        config.GetString("token") is { Length: > 0 } || config.GetString("client_id") is { Length: > 0 };

    /// <summary>The storage family: explicit <c>storage</c>, else inferred from a <c>files</c> root's
    /// scheme, else s3 (the connector's original and only family before Azure).</summary>
    internal static string StorageOf(ConnectorConfig config)
    {
        if (config.GetString("storage") is { Length: > 0 } explicitStorage)
        {
            return explicitStorage;
        }

        return Of(config) == Files && config.GetString("root") is { Length: > 0 } root && IsAzureUrl(root)
            ? StorageAzure
            : StorageS3;
    }

    internal static bool IsAzureUrl(string value) =>
        value.StartsWith("az://", StringComparison.OrdinalIgnoreCase) ||
        value.StartsWith("azure://", StringComparison.OrdinalIgnoreCase) ||
        value.StartsWith("abfss://", StringComparison.OrdinalIgnoreCase);

    internal static string? StorageAuth(ConnectorConfig config) =>
        config.GetString("storage_auth") is { Length: > 0 } auth ? auth : null;

    internal static string[] AzureRequiredFields(string auth) => auth switch
    {
        "connection_string" => ["storage_connection_string"],
        "account_key" => ["storage_account_name", "storage_account_key"],
        "service_principal" => ["storage_tenant_id", "storage_client_id", "storage_client_secret", "storage_account_name"],
        "credential_chain" => ["storage_account_name"],
        _ => [],
    };

    /// <summary>True when the connection carries an explicit data-plane credential of either family
    /// (S3 keys, or an Azure auth method); false leaves the data plane to the catalog's vending or
    /// the ambient credential chain.</summary>
    internal static bool HasStorageCredentials(ConnectorConfig config) =>
        config.GetString("storage_key_id") is { Length: > 0 } ||
        (StorageOf(config) == StorageAzure && StorageAuth(config) is not null);

    internal static IReadOnlyList<string> Validate(ConnectorConfig config)
    {
        var catalog = Of(config);
        var errors = new List<string>();

        void Require(string key) { if (config.GetString(key) is not { Length: > 0 }) { errors.Add($"catalog '{catalog}' requires '{key}'"); } }
        void Forbid(IEnumerable<string> keys, string owner)
        {
            foreach (var key in keys)
            {
                if (config.Values.ContainsKey(key))
                {
                    errors.Add($"'{key}' belongs to catalog '{owner}' and is not valid for catalog '{catalog}'");
                }
            }
        }

        // DuckDB attaches a URL-shaped warehouse read-only, silently -- a catalog connection meant
        // for writes needs the catalog's warehouse NAME (or ARN/account form), never its own storage
        // location.
        void ForbidUrlWarehouse()
        {
            // Never echo the value: a pasted URL may carry `user:pass@` credentials, and this
            // message can reach plan.json / a NodeResult.
            if (config.GetString("warehouse") is { Length: > 0 } warehouse && IcebergSql.IsUrl(warehouse))
            {
                errors.Add("'warehouse' looks like a URL; DuckDB attaches a URL-shaped " +
                    "warehouse read-only -- give the catalog's warehouse NAME instead");
            }
        }

        switch (catalog)
        {
            case Rest:
                Require("endpoint");
                if (config.GetString("endpoint") is { Length: > 0 } endpoint && !IsHttpUrl(endpoint))
                {
                    errors.Add("'endpoint' must be an http:// or https:// URL");
                }

                Forbid(["root"], Files);
                ForbidUrlWarehouse();
                ValidateRestAuth(config, errors);
                break;
            case Glue:
                Forbid(["endpoint", .. RestAuthKeys], Rest);
                Forbid(["root"], Files);
                ForbidUrlWarehouse();
                break;
            case S3Tables:
                Require("warehouse");
                Forbid(["endpoint", .. RestAuthKeys], Rest);
                Forbid(["root"], Files);
                ForbidUrlWarehouse();
                break;
            case Files:
                Require("root");
                Forbid(["endpoint", "warehouse", "nested_namespaces", .. RestAuthKeys], Rest);
                break;
            default:
                errors.Add($"unknown catalog '{catalog}' (expected rest, glue, s3_tables or files)");
                break;
        }

        var storage = StorageOf(config);
        if (storage is not (StorageS3 or StorageAzure))
        {
            errors.Add($"'storage' must be 's3' or 'azure' (got '{storage}')");
            return errors;
        }

        if (storage == StorageS3)
        {
            foreach (var key in AzureOnlyKeys.Where(k => config.Values.ContainsKey(k)))
            {
                errors.Add($"'{key}' belongs to storage 'azure' and is not valid for storage 's3'");
            }

            if (catalog == Files && config.GetString("root") is { Length: > 0 } azureRoot && IsAzureUrl(azureRoot))
            {
                errors.Add("'root' is an Azure URL; declare storage 'azure' (or omit 'storage' to infer it)");
            }

            ValidateS3Storage(config, catalog, errors);
        }
        else
        {
            ValidateAzureStorage(config, catalog, errors);
        }

        return errors;
    }

    private static void ValidateS3Storage(ConnectorConfig config, string catalog, List<string> errors)
    {
        var hasKey = config.GetString("storage_key_id") is { Length: > 0 };
        var hasSecret = config.GetString("storage_secret_key") is { Length: > 0 };
        if (hasKey != hasSecret)
        {
            errors.Add("'storage_key_id' and 'storage_secret_key' must be declared together");
        }

        if (!hasKey && !hasSecret)
        {
            foreach (var key in StorageKeysNeedingCredentials.Where(k => config.Values.ContainsKey(k)))
            {
                errors.Add($"'{key}' requires 'storage_key_id' and 'storage_secret_key'");
            }
        }

        if (hasKey && hasSecret && catalog == Files &&
            config.GetString("root") is { Length: > 0 } root && !IcebergSql.IsUrl(root))
        {
            errors.Add("'storage_key_id' and 'storage_secret_key' require an object-store 'root' (a URL such as s3://bucket/prefix/)");
        }
    }

    /// <summary>Azure keys mirror the azureblob connector's <c>auth</c> methods field-for-field
    /// (prefixed <c>storage_</c> because <c>client_id</c>/<c>client_secret</c> already name the REST
    /// catalog's OAuth2 pair here). An AWS catalog never stores on Azure; a bare <c>files</c> root
    /// has nobody to vend for it, so it needs an explicit method.</summary>
    private static void ValidateAzureStorage(ConnectorConfig config, string catalog, List<string> errors)
    {
        foreach (var key in S3OnlyKeys.Where(k => config.Values.ContainsKey(k)))
        {
            errors.Add($"'{key}' belongs to storage 's3' and is not valid for storage 'azure'");
        }

        if (IsAws(catalog))
        {
            errors.Add($"storage 'azure' is not valid for catalog '{catalog}' (an AWS catalog stores its tables on S3)");
        }

        if (catalog == Files && config.GetString("root") is { Length: > 0 } root && !IsAzureUrl(root))
        {
            errors.Add("'root' is not an Azure URL (az://, azure://, abfss://) but storage is 'azure'");
        }

        var auth = StorageAuth(config);
        if (auth is null)
        {
            if (catalog == Files)
            {
                errors.Add("catalog 'files' with storage 'azure' requires 'storage_auth' (nothing vends credentials for a bare root)");
            }

            foreach (var key in AzureOnlyKeys.Where(k => k != "storage_auth" && config.Values.ContainsKey(k)))
            {
                errors.Add($"'{key}' requires 'storage_auth'");
            }

            // storage_endpoint is a shared key (also valid under s3), so it is deliberately absent
            // from AzureOnlyKeys -- without this check it would pass validation here and then be
            // silently dropped: no storage_auth means no secret is ever built to carry it.
            if (config.Values.ContainsKey("storage_endpoint"))
            {
                errors.Add("'storage_endpoint' requires 'storage_auth'");
            }

            return;
        }

        if (!AzureAuthMethods.Contains(auth))
        {
            errors.Add($"'storage_auth' must be one of {string.Join(", ", AzureAuthMethods)} (got '{auth}')");
            return;
        }

        foreach (var field in AzureRequiredFields(auth).Where(f => config.GetString(f) is not { Length: > 0 }))
        {
            errors.Add($"storage_auth '{auth}' requires '{field}'");
        }

        if (auth != "credential_chain" && config.Values.ContainsKey("storage_chain"))
        {
            errors.Add("'storage_chain' applies to storage_auth 'credential_chain' only");
        }

        if (auth != "account_key" && config.Values.ContainsKey("storage_endpoint"))
        {
            errors.Add("'storage_endpoint' under storage 'azure' applies to storage_auth 'account_key' only");
        }
    }

    /// <summary>A bearer <c>token</c> and an OAuth2 client pair are alternatives, never combined;
    /// the pair is declared together; the OAuth2 tuning keys mean nothing without the pair.</summary>
    private static void ValidateRestAuth(ConnectorConfig config, List<string> errors)
    {
        var hasToken = config.GetString("token") is { Length: > 0 };
        var hasClientId = config.GetString("client_id") is { Length: > 0 };
        var hasClientSecret = config.GetString("client_secret") is { Length: > 0 };
        if (hasToken && (hasClientId || hasClientSecret))
        {
            errors.Add("declare either 'token' or 'client_id'/'client_secret', not both");
        }

        if (hasClientId != hasClientSecret)
        {
            errors.Add("'client_id' and 'client_secret' must be declared together");
        }

        if (!hasClientId && !hasClientSecret)
        {
            foreach (var key in new[] { "oauth2_server_uri", "oauth2_scope" }.Where(k => config.Values.ContainsKey(k)))
            {
                errors.Add($"'{key}' requires 'client_id' and 'client_secret'");
            }
        }
    }

    internal static bool IsHttpUrl(string value) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https";
}
