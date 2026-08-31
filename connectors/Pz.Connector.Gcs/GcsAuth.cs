using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs;

/// <summary>The gcs auth matrix: `auth` selects the method, and the method decides which data plane
/// exists. <c>hmac</c> (interop key pair) is the ONLY method DuckDB's native <c>gs://</c> tier can
/// authenticate — it maps to <see cref="GcsSql.CreateSecretSql"/> and never to an SDK client.
/// <c>service_account</c>/<c>adc</c> are OAuth-only and map to a <see cref="StorageClient"/> for the
/// universal write tier — the SDK cannot sign with hmac keys, so the two planes are disjoint by
/// construction, not by policy. Required-field checks are offline and aggregate (the AzureAuth
/// shape).</summary>
internal static class GcsAuth
{
    private static readonly string[] Methods = ["hmac", "service_account", "adc"];

    public static bool IsHmac(ConnectorConfig config) =>
        string.Equals(config.GetString("auth"), "hmac", StringComparison.Ordinal);

    public static ValidationResult Validate(ConnectorConfig config)
    {
        var auth = config.GetString("auth");
        if (string.IsNullOrEmpty(auth))
        {
            return ValidationResult.Failed("gcs connection requires 'auth' (one of: " + string.Join(", ", Methods) + ")");
        }

        if (!Methods.Contains(auth))
        {
            return ValidationResult.Failed($"gcs connection 'auth' must be one of {string.Join(", ", Methods)} (got '{auth}')");
        }

        var errors = new List<string>();
        switch (auth)
        {
            case "hmac":
                foreach (var field in (string[])["key_id", "secret"])
                {
                    if (string.IsNullOrEmpty(config.GetString(field)))
                    {
                        errors.Add($"gcs 'hmac' auth requires '{field}'");
                    }
                }

                break;
            case "service_account":
                var hasFile = !string.IsNullOrEmpty(config.GetString("key_file"));
                var hasJson = !string.IsNullOrEmpty(config.GetString("key_json"));
                if (hasFile == hasJson)
                {
                    errors.Add(hasFile
                        ? "gcs 'service_account' auth takes 'key_file' or 'key_json', not both"
                        : "gcs 'service_account' auth requires 'key_file' or 'key_json'");
                }

                break;
        }

        return errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]);
    }

    /// <summary>The universal write tier's SDK client for the OAuth methods. Construction is offline
    /// (a token is minted only by the first real call); hmac has no SDK client at all — DuckDB's
    /// native tier is its entire data plane, so asking for one is a permanent, named refusal.
    /// <c>endpoint</c>, when set, overrides the service base URI (emulator/proxy setups).</summary>
    public static StorageClient CreateStorageClient(ConnectorConfig config)
    {
        var auth = config.GetString("auth");
        var credential = auth switch
        {
            "service_account" => ServiceAccountCredential(config),
            "adc" => GoogleCredential.GetApplicationDefault(),
            "hmac" => throw new PzConnectorException(
                "gcs 'hmac' auth has no SDK client: hmac keys drive only the native DuckDB tier " +
                "(reads and native COPY writes); use 'service_account' or 'adc' for SDK-backed writes",
                isTransient: false),
            _ => throw new PzConnectorException($"gcs: unsupported auth '{auth}'", isTransient: false),
        };

        var builder = new StorageClientBuilder { Credential = credential };
        if (config.GetString("endpoint") is { Length: > 0 } endpoint)
        {
            builder.BaseUri = endpoint;
        }

        return builder.Build();
    }

    private static GoogleCredential ServiceAccountCredential(ConnectorConfig config)
    {
        if (config.GetString("key_file") is { Length: > 0 } keyFile)
        {
            try
            {
                return GoogleCredential.FromFile(keyFile);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
            {
                // Never the file's contents in the message (secret hygiene) -- the path plus the
                // loader's own reason is enough to act on.
                throw new PzConnectorException(
                    $"gcs 'key_file' could not be loaded from '{keyFile}': {ex.Message}",
                    isTransient: false, innerException: ex);
            }
        }

        try
        {
            return GoogleCredential.FromJson(config.GetString("key_json"));
        }
        catch (Exception ex)
        {
            // The parser throws serializer-specific exception types; whatever the shape, a key that
            // does not parse is permanent. The raw json never appears in the message.
            throw new PzConnectorException(
                $"gcs 'key_json' is not a valid service-account key: {ex.Message}",
                isTransient: false, innerException: ex);
        }
    }
}
