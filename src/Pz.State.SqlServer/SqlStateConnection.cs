using Microsoft.Data.SqlClient;
using Pz.Core.Validation;

namespace Pz.State.SqlServer;

/// <summary>Opens connections to the state database and maps every SQL-level failure onto PZ0518.
///
/// It takes a connection STRING, not a connector: the state store must be usable by management verbs
/// (`pz state show`, `pz cdc status`) without package restore or ALC loading, which is the whole reason
/// this lives in a directly-referenced assembly rather than behind the connector ABI.
///
/// Secret hygiene: the connection string never reaches an error message. Failures are reported with the
/// server and database only, both read back off SqlConnectionStringBuilder.</summary>
public sealed class SqlStateConnection(string connectionString, string schema)
{
    public string Schema { get; } = schema;

    public SqlConnection Open()
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
            throw Unavailable(ex);
        }
    }

    public PzConfigException Unavailable(Exception cause)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        return new PzConfigException(new PzError(PzErrorCode.StateStoreUnavailable,
            $"cannot reach the state store on server '{builder.DataSource}', database " +
            $"'{builder.InitialCatalog}': {cause.GetType().Name}.",
            "project.yml", null,
            "check state.connection / PZ_STATE_CONNECTION_STRING, and that the database is reachable"));
    }

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
}
