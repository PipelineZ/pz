namespace Pz.TestSupport;

/// <summary>The opt-in lane against a real storage account over HTTPS, run by hand (CI covers TLS with Azurite
/// over HTTPS). Tests gated on <see cref="SkipUnlessConfigured"/> run only when
/// <c>PZ_TEST_AZURE_ACCOUNT</c> names a storage account. Auth is <c>PZ_TEST_AZURE_ACCOUNT_KEY</c> when set,
/// otherwise the Azure CLI credential (an <c>az login</c> session), which needs
/// Storage Blob Data Contributor on the container. Every test writes under its own <see cref="NewPrefix"/>
/// and deletes it afterwards.</summary>
public static class LiveAzure
{
    public const string Category = "AzureLive";

    public static string? Account => Env("PZ_TEST_AZURE_ACCOUNT");

    public static string? AccountKey => Env("PZ_TEST_AZURE_ACCOUNT_KEY");

    public static string Container => Env("PZ_TEST_AZURE_CONTAINER") ?? "pz-ci";

    public static Uri ContainerUri => new($"https://{Account}.blob.core.windows.net/{Container}");

    public static void SkipUnlessConfigured() => Skip.If(Account is null, "PZ_TEST_AZURE_ACCOUNT is not set");

    /// <summary>A unique blob prefix per test, under one root so a lifecycle expiry rule can sweep what a
    /// killed run leaves behind.</summary>
    public static string NewPrefix(string test) => $"pz-live/{test}-{Guid.NewGuid():N}";

    /// <summary>The azureblob connection keys (and, with <paramref name="prefix"/>, the iceberg
    /// <c>storage_</c>-prefixed ones) for whichever auth the environment provides.</summary>
    public static Dictionary<string, object?> AuthKeys(string prefix = "") => AccountKey is { } key
        ? new()
        {
            [$"{prefix}auth"] = "account_key",
            [$"{prefix}account_name"] = Account,
            [$"{prefix}account_key"] = key,
        }
        : new()
        {
            [$"{prefix}auth"] = "credential_chain",
            [$"{prefix}account_name"] = Account,
            [$"{prefix}chain"] = "cli",
        };

    private static string? Env(string name) => Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : null;
}
