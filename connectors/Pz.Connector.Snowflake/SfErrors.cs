using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake;

/// <summary>Transience classification for engine retries. Network shapes (timeouts, IO,
/// HTTP transport) retry; auth, SQL compilation, and missing-object errors do not.</summary>
internal static class SfErrors
{
    // Client-side network errors (e.g. the driver's own REQUEST_TIMEOUT/IO_ERROR_ON_GETPUT_COMMAND,
    // vendor codes 270007/270058) carry an EMPTY SqlState -- the transport failure that caused them
    // is only reachable via InnerException -- so the SnowflakeDbException arm must fall through to
    // the inner-exception recursion rather than short-circuit it.
    public static bool IsTransient(Exception ex) => ex switch
    {
        TimeoutException or IOException or System.Net.Http.HttpRequestException => true,
        TaskCanceledException tce when tce.InnerException is TimeoutException => true,
        SnowflakeDbException sf => IsTransientSnowflakeCode(sf) ||
            (sf.InnerException is not null && IsTransient(sf.InnerException)),
        _ when ex.InnerException is not null => IsTransient(ex.InnerException),
        _ => false,
    };

    // SqlState 08xxx = connection exceptions (the driver's own CONNECTION_FAILURE_SSTATE is
    // "08006"); auth (390100-390195), compilation (001003), and missing-object (002003) errors
    // carry other SqlStates and are permanent. Vendor codes 270007 (REQUEST_TIMEOUT) and 270058
    // (IO_ERROR_ON_GETPUT_COMMAND, e.g. a PUT/GET transfer failure) are client-side network shapes
    // the driver raises with no SqlState at all, so they're checked by ErrorCode directly.
    private static bool IsTransientSnowflakeCode(SnowflakeDbException sf) =>
        (sf.SqlState is { Length: 5 } state && state.StartsWith("08", StringComparison.Ordinal)) ||
        sf.ErrorCode is 270007 or 270058;
}
