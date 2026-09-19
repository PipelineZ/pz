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

        CreateLegacyV1Database(connection);

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

    /// <summary>Two processes racing `EnsureCurrent` against the same schema must not both decide to
    /// migrate: a caller that finds the store behind takes the exclusive `sp_getapplock` inside its
    /// transaction and reads the version again under it, instead of migrating on the version it read
    /// before it held the lock.
    ///
    /// Deterministic by ORDERING, not timing (same technique as
    /// <c>SqlKeyedStateStoreConcurrencyTests.An_RCSI_snapshot_read_that_misses_an_uncommitted_insert_still_conflicts_via_PZ0520</c>):
    /// a raw session holds the SAME `sp_getapplock` resource `EnsureCurrent` itself would take, and the
    /// poll loop blocks the TEST (not the two SQL sessions) until it has observed -- via
    /// `sys.dm_exec_requests`, not a guess -- that the background `EnsureCurrent` call is already waiting
    /// on that lock. Only then does the raw session release it.</summary>
    [SkippableFact]
    public void EnsureCurrent_waits_for_a_concurrent_migrator_before_deciding_to_migrate()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        CreateLegacyV1Database(connection); // behind CurrentVersion, so EnsureCurrent has to take the lock

        // A second, raw session takes the exact resource EnsureCurrent's own sp_getapplock call would --
        // same "pz_state_schema:<schema>" key -- and holds it, uncommitted, standing in for a concurrent
        // migrator.
        using var raw = connection.Open();
        var rawSpid = Convert.ToInt32(new SqlCommand("SELECT @@SPID", raw).ExecuteScalar());
        using var rawTransaction = raw.BeginTransaction();
        using (var acquire = new SqlCommand(
            "DECLARE @result INT; " +
            "EXEC @result = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', " +
            "@LockOwner = 'Transaction', @LockTimeout = -1; SELECT @result;",
            raw, rawTransaction))
        {
            acquire.Parameters.AddWithValue("@resource", "pz_state_schema:" + connection.Schema);
            Assert.Equal(0, Convert.ToInt32(acquire.ExecuteScalar())); // acquired immediately, nothing else held it yet
        }

        var ensureTask = Task.Run(() => SqlStateSchema.EnsureCurrent(connection));

        using (var poll = connection.Open())
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            var blocked = false;
            while (DateTime.UtcNow < deadline)
            {
                using var check = new SqlCommand(
                    "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = @spid", poll);
                check.Parameters.AddWithValue("@spid", rawSpid);
                if ((int)check.ExecuteScalar()! > 0)
                {
                    blocked = true;
                    break;
                }

                Thread.Sleep(20);
            }

            Assert.True(blocked, "EnsureCurrent never blocked on the concurrent migrator's applock");
        }

        rawTransaction.Commit(); // releases the transaction-owned applock

        Assert.True(ensureTask.Wait(TimeSpan.FromSeconds(10)),
            "EnsureCurrent did not complete after the concurrent migrator released the lock");

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(connection));
        Assert.Equal(1, CountSchemaVersionRows(connection));
    }

    /// <summary>A migration lock that cannot be acquired within its timeout is PZ0528 with a "retry"
    /// next step, naming server/database but never the connection string (same secret-hygiene contract
    /// as PZ0518/PZ0519). Uses the internal <c>lockTimeoutMs</c> test seam to bound a REAL wait to a few
    /// hundred milliseconds instead of <see cref="SqlStateSchema.LockTimeoutMs"/>'s production
    /// value.</summary>
    [SkippableFact]
    public void EnsureCurrent_reports_PZ0528_when_the_migration_lock_times_out()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        CreateLegacyV1Database(connection); // behind CurrentVersion, so EnsureCurrent has to take the lock

        using var raw = connection.Open();
        using var rawTransaction = raw.BeginTransaction();
        using (var acquire = new SqlCommand(
            "DECLARE @result INT; " +
            "EXEC @result = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', " +
            "@LockOwner = 'Transaction', @LockTimeout = -1; SELECT @result;",
            raw, rawTransaction))
        {
            acquire.Parameters.AddWithValue("@resource", "pz_state_schema:" + connection.Schema);
            Assert.Equal(0, Convert.ToInt32(acquire.ExecuteScalar()));
        }

        try
        {
            var ex = Assert.Throws<PzConfigException>(() => SqlStateSchema.EnsureCurrent(connection, lockTimeoutMs: 500));

            Assert.Equal(PzErrorCode.StateSchemaMigrationLockTimedOut, ex.Error.Code);
            Assert.Contains("retry", ex.Error.Hint, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            rawTransaction.Rollback();
        }
    }

    /// <summary>The scenario the issue actually named: two processes racing `EnsureCurrent` against an
    /// EMPTY database, both deciding whether to run the inaugural migration. Real concurrency (two
    /// `Task.Run`s against two independent <see cref="SqlStateConnection"/>s sharing one fresh,
    /// never-migrated database), not the ordering-only proof above -- both callers must complete without
    /// throwing, and the database must end up with exactly one `schema_version` row at
    /// <see cref="SqlStateSchema.CurrentVersion"/> rather than two (the pre-applock defect: `2714` /
    /// PZ0519 with the wrong advice, or a duplicate row `schema_version` had no key to prevent).</summary>
    [SkippableFact]
    public async Task EnsureCurrent_does_not_corrupt_schema_version_when_two_processes_race_on_an_empty_database()
    {
        DockerFacts.SkipUnlessDocker();
        var connectionString = fixture.NewRawConnectionString(); // one fresh, never-migrated database
        var first = new SqlStateConnection(connectionString, "pz");
        var second = new SqlStateConnection(connectionString, "pz");

        await Task.WhenAll(
            Task.Run(() => SqlStateSchema.EnsureCurrent(first)),
            Task.Run(() => SqlStateSchema.EnsureCurrent(second)));

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(first));
        Assert.Equal(1, CountSchemaVersionRows(first));
    }

    /// <summary>An ordinary run against a store that is already current must not queue behind
    /// somebody else's migration lock -- nothing a migrator does can take the store back below the
    /// version this caller needs.</summary>
    [SkippableFact]
    public void EnsureCurrent_on_a_current_store_does_not_wait_for_the_migration_lock()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        using var raw = connection.Open();
        using var rawTransaction = raw.BeginTransaction();
        using (var acquire = new SqlCommand(
            "DECLARE @result INT; " +
            "EXEC @result = sp_getapplock @Resource = @resource, @LockMode = 'Exclusive', " +
            "@LockOwner = 'Transaction', @LockTimeout = -1; SELECT @result;",
            raw, rawTransaction))
        {
            acquire.Parameters.AddWithValue("@resource", "pz_state_schema:" + connection.Schema);
            Assert.Equal(0, Convert.ToInt32(acquire.ExecuteScalar()));
        }

        try
        {
            SqlStateSchema.EnsureCurrent(connection, lockTimeoutMs: 500); // PZ0528 if it had asked for the lock
        }
        finally
        {
            rawTransaction.Rollback();
        }
    }

    /// <summary>The race this migration heals leaves two `schema_version` rows carrying the SAME
    /// version (both racers stamped the version they migrated to), so de-duplicating by "older than the
    /// newest" would delete neither and the PRIMARY KEY could not be added.</summary>
    [SkippableFact]
    public void EnsureCurrent_heals_a_store_whose_schema_version_holds_two_rows_at_the_same_version()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        CreateLegacyV1Database(connection);
        using (var sqlConnection = connection.Open())
        {
            using var duplicate = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = " +
                "'INSERT INTO ' + QUOTENAME(@schema) + '.schema_version (version) VALUES (1);'; EXEC(@sql);",
                sqlConnection);
            duplicate.Parameters.AddWithValue("@schema", connection.Schema);
            duplicate.ExecuteNonQuery();
        }

        Assert.Equal(2, CountSchemaVersionRows(connection));

        SqlStateSchema.EnsureCurrent(connection);

        Assert.Equal(SqlStateSchema.CurrentVersion, SqlStateSchema.ReadVersion(connection));
        Assert.Equal(1, CountSchemaVersionRows(connection));
    }

    /// <summary>A version-1 store built the way the first release built it, bypassing SqlStateSchema:
    /// default (case-insensitive) collation on `state`, and a `schema_version` table with no key.</summary>
    private static void CreateLegacyV1Database(SqlStateConnection connection)
    {
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
    }

    private static int CountSchemaVersionRows(SqlStateConnection connection)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT COUNT(*) FROM ' + QUOTENAME(@schema) + N'.schema_version'; " +
            "EXEC sp_executesql @sql;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        return (int)command.ExecuteScalar()!;
    }
}
