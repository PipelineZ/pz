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

    private static readonly string[] RestAuthKeys = ["token", "client_id", "client_secret", "oauth2_server_uri", "oauth2_scope"];
    private static readonly string[] StorageKeysNeedingCredentials = ["storage_endpoint", "storage_url_style", "storage_use_ssl"];

    internal static string Of(ConnectorConfig config) => config.GetString("catalog") ?? Rest;

    internal static bool IsAws(string catalog) => catalog is Glue or S3Tables;

    /// <summary>True when the connection carries a catalog credential (a bearer token or an OAuth2
    /// client pair) that must ride a <c>type iceberg</c> secret; false attaches with
    /// <c>authorization_type 'none'</c>.</summary>
    internal static bool HasCatalogCredentials(ConnectorConfig config) =>
        config.GetString("token") is { Length: > 0 } || config.GetString("client_id") is { Length: > 0 };

    internal static bool HasStorageCredentials(ConnectorConfig config) =>
        config.GetString("storage_key_id") is { Length: > 0 };

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

        return errors;
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
