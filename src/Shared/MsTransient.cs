using Microsoft.Data.SqlClient;

namespace Pz.Shared;

/// <summary>Pure, offline-testable classifier for whether a <see cref="SqlException"/> raised against
/// SQL Server (on-prem or Azure SQL) is worth a retry -- see
/// <c>Pz.Connectors.Abstractions.PzConnectorException.IsTransient</c>. <see cref="SqlException.IsTransient"/>
/// (Microsoft.Data.SqlClient's own signal) covers Azure SQL's connection-resiliency reconnect cases only:
/// a deadlock victim (1205) and a client-side command timeout (-2) both come back with
/// <c>IsTransient == false</c>, so every raise site that forwards the driver flag unchanged treats a
/// classic transient condition as permanent. This is a closed list of the error numbers Microsoft
/// documents as retryable (statement-level deadlock/lock-timeout, login-process transport drops,
/// resource-throttling and Azure SQL failover) plus the client-side command-timeout sentinel, mirroring
/// how <c>AzureTransient</c>/<c>GcsTransient</c> classify their own SDKs' failures.
///
/// Linked (not project-referenced) into both <c>Pz.Connector.SqlServer</c> and <c>Pz.State.SqlServer</c>
/// so both can call it without a forbidden connector-to-state or state-to-connector project reference.</summary>
internal static class MsTransient
{
    private static readonly HashSet<int> TransientErrorNumbers =
    [
        1205, // deadlock victim -- rerun the transaction
        -2, // client-side command timeout (SqlClient's own sentinel, not a server error number)
        233, // pre-login transport drop (shared memory/named pipes: "no process is on the other end of the pipe")
        64, // pre-login transport drop (TCP: "the specified network name is no longer available")
        10053, // a transport-level error occurred while receiving results
        10054, // an existing connection was forcibly closed by the remote host
        10060, // network connection timed out
        40613, // Azure SQL: database not currently available (mid-failover/scale)
        40197, // Azure SQL: service encountered an error processing the request
        40501, // Azure SQL: service currently busy (throttling)
        49918, // Azure SQL: not enough resources to process request
        49919, // Azure SQL: too many create/update operations in progress for the subscription
        49920, // Azure SQL: too many operations in progress for the subscription
    ];

    public static bool IsTransient(SqlException ex) => TransientErrorNumbers.Contains(ex.Number) || ex.IsTransient;
}
