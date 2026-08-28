using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake;

/// <summary>Transience classification for engine retries. Network shapes (timeouts, IO,
/// HTTP transport) retry; auth, SQL compilation, and missing-object errors do not.</summary>
internal static class SfErrors
{
    public static bool IsTransient(Exception ex) => ex switch
    {
        TimeoutException or IOException or System.Net.Http.HttpRequestException => true,
        TaskCanceledException tce when tce.InnerException is TimeoutException => true,
        SnowflakeDbException sf => IsTransientSnowflakeCode(sf),
        _ when ex.InnerException is not null => IsTransient(ex.InnerException),
        _ => false,
    };

    // SqlState 08xxx = connection exceptions (the driver's own CONNECTION_FAILURE_SSTATE is
    // "08006"); auth (390100-390195), compilation (001003), and missing-object (002003) errors
    // carry other SqlStates and are permanent.
    private static bool IsTransientSnowflakeCode(SnowflakeDbException sf) =>
        sf.SqlState is { Length: 5 } state && state.StartsWith("08", StringComparison.Ordinal);
}
