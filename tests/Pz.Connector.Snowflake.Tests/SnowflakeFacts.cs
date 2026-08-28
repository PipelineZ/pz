namespace Pz.Connector.Snowflake.Tests;

/// <summary>Env-var gate for live Snowflake facts (no container exists for Snowflake, so the
/// docker SKIP convention becomes an env-var SKIP convention). Required:
/// PZ_SNOWFLAKE_ACCOUNT, PZ_SNOWFLAKE_USER, PZ_SNOWFLAKE_PRIVATE_KEY_PATH,
/// PZ_SNOWFLAKE_DATABASE, PZ_SNOWFLAKE_WAREHOUSE. Optional: PZ_SNOWFLAKE_ROLE.</summary>
internal static class SnowflakeFacts
{
    private static readonly string[] Required =
    [
        "PZ_SNOWFLAKE_ACCOUNT", "PZ_SNOWFLAKE_USER", "PZ_SNOWFLAKE_PRIVATE_KEY_PATH",
        "PZ_SNOWFLAKE_DATABASE", "PZ_SNOWFLAKE_WAREHOUSE",
    ];

    public static void SkipUnlessConfigured() => Skip.If(
        Required.Any(v => string.IsNullOrEmpty(Environment.GetEnvironmentVariable(v))),
        "PZ_SNOWFLAKE_* env vars not set -- live snowflake acceptance skipped");

    public static Dictionary<string, object?> Config()
    {
        var config = new Dictionary<string, object?>
        {
            ["account"] = Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_ACCOUNT"),
            ["user"] = Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_USER"),
            ["private_key_path"] = Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_PRIVATE_KEY_PATH"),
            ["database"] = Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_DATABASE"),
            ["warehouse"] = Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_WAREHOUSE"),
        };
        if (Environment.GetEnvironmentVariable("PZ_SNOWFLAKE_ROLE") is { Length: > 0 } role)
        {
            config["role"] = role;
        }

        return config;
    }
}
