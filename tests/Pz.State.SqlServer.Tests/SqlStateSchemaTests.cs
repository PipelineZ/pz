using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.Engine.State;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlStateSchemaTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public void EnsureCurrent_creates_the_schema_and_stamps_the_version()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();

        SqlStateSchema.EnsureCurrent(connection);

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(connection));
    }

    [SkippableFact]
    public void EnsureCurrent_is_idempotent()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();

        SqlStateSchema.EnsureCurrent(connection);
        SqlStateSchema.EnsureCurrent(connection);

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(connection));
    }

    [SkippableFact]
    public void A_newer_store_version_is_PZ0519()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);
        SqlServerFixture.SetSchemaVersion(connection, SqlStateSchema.CurrentVersion + 1);

        var ex = Assert.Throws<PzConfigException>(() => SqlStateSchema.EnsureCurrent(connection));

        Assert.Equal(PzErrorCode.StateSchemaVersionMismatch, ex.Error.Code);
    }

    [SkippableFact]
    public void An_unreachable_server_is_PZ0518()
    {
        DockerFacts.SkipUnlessDocker();
        const string connectionString =
            "Server=127.0.0.1,1;Database=pz;User Id=sa;Password=no;TrustServerCertificate=true;Connect Timeout=2";
        var connection = new SqlStateConnection(connectionString, "pz");

        var ex = Assert.Throws<PzConfigException>(() => SqlStateSchema.EnsureCurrent(connection));

        Assert.Equal(PzErrorCode.StateStoreUnavailable, ex.Error.Code);
        // Secret hygiene: the message names the server/database (so an operator can act on it) but
        // never the connection string itself -- not the literal "Password" key, not its value, and not
        // the string verbatim.
        Assert.Contains("127.0.0.1", ex.Error.Message);
        Assert.DoesNotContain("Password", ex.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("no", ex.Error.Message.Split(' '), StringComparer.OrdinalIgnoreCase);
        Assert.DoesNotContain(connectionString, ex.Error.Message);
    }

    [SkippableFact]
    public void Missing_ddl_rights_after_connecting_is_PZ0519_not_PZ0518()
    {
        DockerFacts.SkipUnlessDocker();
        // Distinguishes "cannot connect" (PZ0518) from "connected fine, then DDL failed" (PZ0519):
        // this login authenticates successfully -- the server is reachable, the credentials are good --
        // but carries no CREATE SCHEMA/TABLE rights, so EnsureCurrent fails only after Open() succeeds.
        var connection = fixture.NewConnectionWithoutDdlRights();

        var ex = Assert.Throws<PzConfigException>(() => SqlStateSchema.EnsureCurrent(connection));

        Assert.Equal(PzErrorCode.StateSchemaVersionMismatch, ex.Error.Code);
        Assert.DoesNotContain("Password", ex.Error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Pz_LowPriv", ex.Error.Message);
    }

    /// <summary>Pins that the v1-to-v2 migration actually HEALS a database created before
    /// `scope`/`state_key` carried `COLLATE ... BIN2` -- not just that it stamps a version number.
    /// Builds a v1 database the old way (SQL Server's default, case-insensitive collation), bypassing
    /// SqlStateSchema entirely so the starting shape is exactly what an older store looks like.</summary>
    [SkippableFact]
    public void EnsureCurrent_heals_a_v1_databases_case_insensitive_collation()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();

        using (var sqlConnection = connection.Open())
        {
            using var createSchema = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = 'CREATE SCHEMA ' + QUOTENAME(@schema); EXEC(@sql);",
                sqlConnection);
            createSchema.Parameters.AddWithValue("@schema", connection.Schema);
            createSchema.ExecuteNonQuery();
        }

        using (var sqlConnection = connection.Open())
        {
            using var createTables = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = " +
                "'CREATE TABLE ' + QUOTENAME(@schema) + '.schema_version (version INT NOT NULL); ' + " +
                "'CREATE TABLE ' + QUOTENAME(@schema) + '.state (scope NVARCHAR(32) NOT NULL, " +
                "state_key NVARCHAR(512) NOT NULL, payload NVARCHAR(MAX) NOT NULL, version INT NOT NULL, " +
                "updated_at DATETIME2 NOT NULL, PRIMARY KEY (scope, state_key)); ' + " +
                "'INSERT INTO ' + QUOTENAME(@schema) + '.schema_version (version) VALUES (1);'; " +
                "EXEC(@sql);",
                sqlConnection);
            createTables.Parameters.AddWithValue("@schema", connection.Schema);
            createTables.ExecuteNonQuery();
        }

        Assert.Equal(1, SqlStateSchema.ReadVersion(connection));

        SqlStateSchema.EnsureCurrent(connection);

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(connection));

        using (var sqlConnection = connection.Open())
        {
            using var command = new SqlCommand(
                "SELECT collation_name FROM sys.columns " +
                "WHERE object_id = OBJECT_ID(@qualifiedTable) AND name = @column",
                sqlConnection);
            command.Parameters.AddWithValue("@qualifiedTable", $"{connection.Schema}.state");
            command.Parameters.AddWithValue("@column", "state_key");
            Assert.Equal("Latin1_General_100_BIN2", (string)command.ExecuteScalar()!);
        }

        // ...and "A"/"a" now survive as distinct keys -- proving the healed collation is load-bearing,
        // not just a stamped version number.
        var store = new SqlKeyedStateStore<KeyedStateStoreContract.TestEntry>(connection, "heal-check",
            readEntry: static entry =>
            {
                var value = entry.GetProperty("value").GetString();
                var runId = entry.GetProperty("runId").GetString();
                return value is null || runId is null ? null : new KeyedStateStoreContract.TestEntry(value, runId);
            },
            writeEntry: static (writer, e) =>
            {
                writer.WriteString("value", e.Value);
                writer.WriteString("runId", e.RunId);
            });

        store.Set("A", new KeyedStateStoreContract.TestEntry("upper", "run-1"));
        store.Set("a", new KeyedStateStoreContract.TestEntry("lower", "run-2"));

        Assert.Equal("upper", store.Get("A")!.Value);
        Assert.Equal("lower", store.Get("a")!.Value);
    }
}
