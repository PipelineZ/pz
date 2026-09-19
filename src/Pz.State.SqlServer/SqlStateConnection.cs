using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.Shared;

namespace Pz.State.SqlServer;

/// <summary>Opens connections to the state database and maps every SQL-level failure onto PZ0518
/// (never reached) or PZ0529 (reached, the operation failed).
///
/// It takes a connection STRING, not a connector: the state store must be usable by management verbs
/// (`pz state show`, `pz cdc status`) without package restore or ALC loading, which is the whole reason
/// this lives in a directly-referenced assembly rather than behind the connector ABI.
///
/// Secret hygiene: the connection string never reaches an error message. Failures are reported with the
/// server and database only, both read back off SqlConnectionStringBuilder, plus the SQL error number
/// where the cause is a <see cref="SqlException"/> -- never its message text, which can echo back
/// operator-supplied identifiers.
///
/// **Retry.** A failure classified transient by <see cref="MsTransient"/> -- the same closed list the
/// SqlServer connector uses -- is retried up to <see cref="_maxAttempts"/> times with a delay through the
/// constructor's <see cref="TimeProvider"/> (never a wall-clock sleep, so a test can prove the retry
/// count deterministically), both at connect time (<see cref="Open"/>) and around a caller's operation
/// (<see cref="Execute{T}"/>). Every retried operation here is either read-only, naturally idempotent
/// (DELETE, or an upsert that writes the same end state twice), or -- <see cref="SqlKeyedStateStore{T}.Set"/>'s
/// compare-and-swap -- cannot write twice on replay, and recognizes its own earlier write when only the
/// acknowledgement was lost.</summary>
public sealed class SqlStateConnection(
    string connectionString, string schema, TimeProvider? timeProvider = null, int maxAttempts = 3)
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private static readonly TimeSpan RetryBaseDelay = TimeSpan.FromMilliseconds(200);

    public string Schema { get; } = schema;

    public SqlConnection Open()
    {
        for (var attempt = 1; ; attempt++)
        {
            var connection = new SqlConnection(connectionString);
            try
            {
                connection.Open();
                return connection;
            }
            catch (Exception ex) when (ex is SqlException or InvalidOperationException)
            {
                connection.Dispose();
                if (ex is SqlException sql && MsTransient.IsTransient(sql) && attempt < maxAttempts)
                {
                    Delay(attempt);
                    continue;
                }

                throw Unavailable(ex);
            }
        }
    }

    /// <summary>Runs <paramref name="operation"/> against a freshly opened connection, retrying the
    /// whole thing -- reconnect included, since a transient failure can leave the old connection unusable
    /// -- when it fails with a <see cref="MsTransient"/>-classified <see cref="SqlException"/>. A
    /// non-transient failure, or one past the retry budget, is <see cref="QueryFailed"/> (PZ0529): by
    /// construction the connection opened fine (<see cref="Open"/> throws its own PZ0518 before this ever
    /// runs), so the store IS reachable and only this operation failed.</summary>
    public T Execute<T>(Func<SqlConnection, T> operation)
    {
        for (var attempt = 1; ; attempt++)
        {
            using var sqlConnection = Open();
            try
            {
                return operation(sqlConnection);
            }
            catch (Exception ex) when (ex is SqlException or InvalidOperationException)
            {
                if (ex is SqlException sql && MsTransient.IsTransient(sql) && attempt < maxAttempts)
                {
                    Delay(attempt);
                    continue;
                }

                throw QueryFailed(ex, ex is SqlException exhausted && MsTransient.IsTransient(exhausted) ? attempt : null);
            }
        }
    }

    private void Delay(int attempt) =>
        Task.Delay(RetryBaseDelay * attempt, _time).GetAwaiter().GetResult();

    public PzConfigException Unavailable(Exception cause)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        return new PzConfigException(new PzError(PzErrorCode.StateStoreUnavailable,
            $"cannot reach the state store on server '{builder.DataSource}', database " +
            $"'{builder.InitialCatalog}': {Describe(cause)}.",
            "project.yml", null,
            "check state.connection / PZ_STATE_CONNECTION_STRING, and that the database is reachable"));
    }

    /// <summary>The connection opened fine; this specific operation's SQL command failed (a permanent
    /// error, or a transient one past the retry budget). PZ0529, not PZ0518 -- see the class doc.</summary>
    public PzConfigException QueryFailed(Exception cause, int? transientAttempts = null)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        return new PzConfigException(new PzError(PzErrorCode.StateQueryFailed,
            $"the state store on server '{builder.DataSource}', database '{builder.InitialCatalog}' " +
            $"was reached, but the operation failed: {Describe(cause)}" +
            (transientAttempts is { } attempts ? $" after {attempts} attempts." : "."),
            "project.yml", null,
            transientAttempts is null
                ? "check the account in state.connection / PZ_STATE_CONNECTION_STRING has the required " +
                  "permissions on the state schema"
                : "this failure is usually temporary (a deadlock, a timeout, a dropped connection) -- run " +
                  "again, and check the load on the state database if it keeps happening"));
    }

    /// <summary>Never the driver's own message text (it can echo back operator-supplied identifiers) --
    /// just the error number, which is enough to look up in SQL Server's own error catalog.</summary>
    private static string Describe(Exception cause) =>
        cause is SqlException sql ? $"SqlException {sql.Number}" : cause.GetType().Name;

    /// <summary>A failure AFTER a successful <see cref="Open"/> -- the connection was fine, something
    /// SQL-level (most likely missing DDL rights) failed while reading or migrating the schema. This is
    /// PZ0519 ("...or a migration failed partway"), not PZ0518: the store IS reachable,
    /// so telling the operator to check connectivity would send them the wrong way.</summary>
    public PzConfigException MigrationFailed(Exception cause)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        return new PzConfigException(new PzError(PzErrorCode.StateSchemaVersionMismatch,
            $"the state schema migration failed partway on server '{builder.DataSource}', database " +
            $"'{builder.InitialCatalog}': {cause.GetType().Name}.",
            "project.yml", null,
            "grant DDL rights (CREATE SCHEMA/CREATE TABLE) on that database to the account in " +
            "state.connection / PZ_STATE_CONNECTION_STRING, then retry"));
    }

    /// <summary>The connection and the schema shape are both fine here -- another process is either
    /// actively migrating (or is stuck holding the lock on) this same schema, so PZ0528 gets its own
    /// "retry" next step rather than PZ0519's "check DDL rights", which would send the operator the
    /// wrong way.</summary>
    public PzConfigException MigrationLockTimedOut(int lockTimeoutMs, int sqlGetAppLockResult)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        return new PzConfigException(new PzError(PzErrorCode.StateSchemaMigrationLockTimedOut,
            $"could not acquire the schema-migration lock on server '{builder.DataSource}', database " +
            $"'{builder.InitialCatalog}' within {lockTimeoutMs}ms (sp_getapplock returned {sqlGetAppLockResult}).",
            "project.yml", null,
            "another process appears to be migrating this schema; retry, and if this persists, check for " +
            "a stuck or unusually long-running migration against the same state.connection"));
    }
}
