using Microsoft.Data.SqlClient;
using Pz.Core.Validation;

namespace Pz.State.SqlServer;

/// <summary>Creates and forward-migrates the SQL Server state schema (`schema_version`, `state`,
/// `runs`, `run_nodes`, `run_events`).
///
/// `pz` owns this schema outright: on first use against a database it issues the DDL and stamps
/// `schema_version`; on every later open it compares versions. An older store is migrated forward
/// inside one transaction; a NEWER store refuses with PZ0519 and writes nothing, because a newer `pz`
/// elsewhere may already depend on columns this build does not know about — guessing would be worse
/// than failing loud.
///
/// The schema name (`state.schema`) is operator-supplied, so every statement that names it goes through
/// SQL Server's own `QUOTENAME` (as a bound parameter fed to dynamic SQL) rather than C#-side string
/// interpolation — that keeps escaping in the one place that already knows the identifier-quoting rules
/// for this dialect. Table and column names below are our own fixed literals, never operator input, so
/// interpolating those directly is safe.</summary>
public static class SqlStateSchema
{
    public const int CurrentVersion = 2;

    public static int ReadVersion(SqlStateConnection connection)
    {
        using var sqlConnection = connection.Open();
        try
        {
            return ReadVersionCore(sqlConnection, null, connection.Schema);
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.MigrationFailed(ex);
        }
    }

    public static void EnsureCurrent(SqlStateConnection connection)
    {
        // PZ0518 is scoped to Open() alone (below) -- a failure past this point means the connection
        // was fine, so it is never "cannot reach the store" and must not tell the operator to check
        // connectivity (that failure mode is PZ0519, "a migration failed partway").
        using var sqlConnection = connection.Open();

        int version;
        try
        {
            version = ReadVersionCore(sqlConnection, null, connection.Schema);
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.MigrationFailed(ex);
        }

        if (version > CurrentVersion)
        {
            throw new PzConfigException(new PzError(PzErrorCode.StateSchemaVersionMismatch,
                $"the state store's schema is at version {version}, newer than this build of pz " +
                $"understands (version {CurrentVersion}).",
                "project.yml", null,
                "upgrade pz, or point state.connection at a store this build created"));
        }

        if (version == CurrentVersion)
        {
            return;
        }

        try
        {
            using var transaction = sqlConnection.BeginTransaction();
            try
            {
                for (var target = version + 1; target <= CurrentVersion; target++)
                {
                    Migrate(sqlConnection, transaction, connection.Schema, target);
                }

                StampVersion(sqlConnection, transaction, connection.Schema, CurrentVersion, insert: version == 0);
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.MigrationFailed(ex);
        }
    }

    /// <summary>Runs the DDL that takes the schema from <paramref name="target"/> - 1 to
    /// <paramref name="target"/>. Version 1 is the schema's inaugural shape; version
    /// 2 heals `scope`/`state_key`'s collation on databases created
    /// before that column definition carried `COLLATE ... BIN2` -- see <see cref="MigrateToBinaryCollation"/>.
    /// Each version gets its own case here; earlier cases are never rewritten, only added to.</summary>
    private static void Migrate(SqlConnection sqlConnection, SqlTransaction transaction, string schema, int target)
    {
        switch (target)
        {
            case 1:
                MigrateToV1(sqlConnection, transaction, schema);
                return;
            case 2:
                MigrateToBinaryCollation(sqlConnection, transaction, schema);
                return;
            default:
                throw new InvalidOperationException($"no migration defined for state schema version {target}.");
        }
    }

    private static void MigrateToV1(SqlConnection sqlConnection, SqlTransaction transaction, string schema)
    {
        ExecuteSchemaCreate(sqlConnection, transaction, schema);

        ExecuteTableCreate(sqlConnection, transaction, schema, "schema_version", "version INT NOT NULL");

        // "state_key" and "rows_moved" (below) are deliberate: KEY and ROWS are reserved words in T-SQL.
        //
        // scope/state_key are COLLATE ... BIN2: SQL Server's default collation is
        // case-insensitive/accent-insensitive, which would silently fold distinct keys like "A" and
        // "a" together -- breaking both the ordinal key semantics the local (JSON) backend guarantees
        // (StringComparer.Ordinal) and SqlKeyedStateStore's insert-if-absent guard, which relies on an
        // exact, case-sensitive match to tell "already there" apart from "free to insert".
        ExecuteTableCreate(sqlConnection, transaction, schema, "state", """
            scope NVARCHAR(32) COLLATE Latin1_General_100_BIN2 NOT NULL,
            state_key NVARCHAR(512) COLLATE Latin1_General_100_BIN2 NOT NULL,
            payload NVARCHAR(MAX) NOT NULL,
            version INT NOT NULL,
            updated_at DATETIME2 NOT NULL,
            PRIMARY KEY (scope, state_key)
            """);

        ExecuteTableCreate(sqlConnection, transaction, schema, "runs", """
            run_id NVARCHAR(64) NOT NULL PRIMARY KEY,
            project NVARCHAR(256) NOT NULL,
            status NVARCHAR(32) NOT NULL,
            started_at DATETIME2 NOT NULL,
            finished_at DATETIME2 NULL,
            events_dropped INT NOT NULL
            """);

        ExecuteTableCreate(sqlConnection, transaction, schema, "run_nodes", """
            run_id NVARCHAR(64) NOT NULL,
            node_id NVARCHAR(128) NOT NULL,
            name NVARCHAR(512) NOT NULL,
            kind NVARCHAR(32) NOT NULL,
            status NVARCHAR(32) NOT NULL,
            rows_moved BIGINT NOT NULL,
            duration_ms BIGINT NOT NULL,
            error_code NVARCHAR(16) NULL,
            error_message NVARCHAR(MAX) NULL,
            provenance NVARCHAR(32) NULL,
            watermark_cursor NVARCHAR(256) NULL,
            watermark_type NVARCHAR(64) NULL,
            watermark_value NVARCHAR(256) NULL,
            payload NVARCHAR(MAX) NULL,
            PRIMARY KEY (run_id, node_id)
            """);

        ExecuteTableCreate(sqlConnection, transaction, schema, "run_events", """
            run_id NVARCHAR(64) NOT NULL,
            seq BIGINT NOT NULL,
            at DATETIME2 NOT NULL,
            event NVARCHAR(64) NOT NULL,
            payload NVARCHAR(MAX) NOT NULL,
            PRIMARY KEY (run_id, seq)
            """);
    }

    /// <summary>Heals `scope`/`state_key` on a database whose `state` table was created before those
    /// columns carried `COLLATE Latin1_General_100_BIN2`. `ExecuteTableCreate` is guarded by `IF OBJECT_ID(...) IS
    /// NULL`, so such a database's `state` table is never touched by <see cref="MigrateToV1"/> again,
    /// and `EnsureCurrent` never re-diffs column definitions on its own -- only bumping
    /// <see cref="CurrentVersion"/> and adding this case actually reaches it.
    ///
    /// Safe to run against existing data: moving from a case-insensitive collation to a stricter
    /// binary one can only SPLIT what the old collation treated as equal, never merge rows that were
    /// previously distinct (the old, coarser `PRIMARY KEY (scope, state_key)` already forced at most one
    /// row per case-insensitive-equivalent key), so re-adding the primary key below can never hit a
    /// uniqueness violation. Also safe to re-run: `ALTER COLUMN` to a column already at the target type
    /// and collation is a no-op, and the constraint name is re-discovered each time rather than assumed.
    ///
    /// A column that is part of a `PRIMARY KEY` cannot have its collation changed in place -- SQL
    /// Server refuses `ALTER COLUMN` on a column backing an index/constraint -- so the constraint is
    /// dropped and recreated around the two `ALTER COLUMN`s, inside the same migration transaction
    /// <see cref="EnsureCurrent"/> already wraps every version's DDL in.</summary>
    private static void MigrateToBinaryCollation(SqlConnection sqlConnection, SqlTransaction transaction, string schema)
    {
        // The constraint name is server-generated (e.g. "PK__state__..."), not a literal we control,
        // so it is looked up rather than assumed. schema/table are compared here as ordinary VALUES
        // (s.name = @schema, t.name = 'state'), not as identifiers, so this query needs no
        // QUOTENAME/dynamic SQL at all.
        string primaryKeyName;
        using (var lookup = new SqlCommand(
            "SELECT kc.name FROM sys.key_constraints kc " +
            "JOIN sys.tables t ON kc.parent_object_id = t.object_id " +
            "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
            "WHERE kc.type = 'PK' AND t.name = 'state' AND s.name = @schema",
            sqlConnection, transaction))
        {
            lookup.Parameters.AddWithValue("@schema", schema);
            var result = lookup.ExecuteScalar();
            if (result is not string name)
            {
                // The "state" table always exists by the time target 2 runs -- MigrateToV1 either ran
                // moments ago in this same transaction (fresh database) or ran in a prior EnsureCurrent
                // call (existing v1 database) -- so a missing PK here means the schema is not the shape
                // this migration expects, and guessing at DDL from here would be worse than failing loud.
                throw new InvalidOperationException(
                    $"expected a PRIMARY KEY on {schema}.state before migrating to schema version 2, found none.");
            }

            primaryKeyName = name;
        }

        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = " +
            "'ALTER TABLE ' + QUOTENAME(@schema) + '.state DROP CONSTRAINT ' + QUOTENAME(@pk) + '; ' + " +
            "'ALTER TABLE ' + QUOTENAME(@schema) + '.state ALTER COLUMN scope NVARCHAR(32) " +
            "COLLATE Latin1_General_100_BIN2 NOT NULL; ' + " +
            "'ALTER TABLE ' + QUOTENAME(@schema) + '.state ALTER COLUMN state_key NVARCHAR(512) " +
            "COLLATE Latin1_General_100_BIN2 NOT NULL; ' + " +
            "'ALTER TABLE ' + QUOTENAME(@schema) + '.state ADD CONSTRAINT ' + QUOTENAME(@pk) + " +
            "' PRIMARY KEY (scope, state_key);'; " +
            "EXEC(@sql);",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.Add("@pk", System.Data.SqlDbType.NVarChar, 128).Value = primaryKeyName;
        command.ExecuteNonQuery();
    }

    /// <summary>`CREATE SCHEMA` must be the only statement in its batch, so it always runs through
    /// `EXEC` of a dynamically built string. `QUOTENAME(@schema)` does the escaping, since `@schema` is
    /// operator-supplied -- built into a local variable first, because `EXEC(expr)`'s inline-expression
    /// grammar accepts string literals and variables only, not function calls (an undocumented T-SQL
    /// parser restriction: `EXEC('...' + QUOTENAME(@p) + '...')` fails to parse,
    /// `DECLARE @sql = '...' + QUOTENAME(@p); EXEC(@sql)` does not).</summary>
    private static void ExecuteSchemaCreate(SqlConnection sqlConnection, SqlTransaction transaction, string schema)
    {
        using var command = new SqlCommand(
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema) " +
            "BEGIN " +
            "DECLARE @sql NVARCHAR(MAX) = 'CREATE SCHEMA ' + QUOTENAME(@schema); " +
            "EXEC(@sql); " +
            "END",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", schema);
        command.ExecuteNonQuery();
    }

    private static void ExecuteTableCreate(
        SqlConnection sqlConnection, SqlTransaction transaction, string schema, string table, string columns)
    {
        using var command = new SqlCommand(
            "IF OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table)) IS NULL " +
            "BEGIN " +
            "DECLARE @sql NVARCHAR(MAX) = " +
            "'CREATE TABLE ' + QUOTENAME(@schema) + '.' + QUOTENAME(@table) + ' (' + @columns + ')'; " +
            "EXEC(@sql); " +
            "END",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.Add("@columns", System.Data.SqlDbType.NVarChar, -1).Value = columns;
        command.ExecuteNonQuery();
    }

    private static void StampVersion(
        SqlConnection sqlConnection, SqlTransaction transaction, string schema, int version, bool insert)
    {
        var verb = insert
            ? "'INSERT INTO ' + QUOTENAME(@schema) + '.schema_version (version) VALUES (' + " +
              "CONVERT(nvarchar(10), @version) + ')'"
            : "'UPDATE ' + QUOTENAME(@schema) + '.schema_version SET version = ' + " +
              "CONVERT(nvarchar(10), @version)";

        using var command = new SqlCommand(
            $"DECLARE @sql NVARCHAR(MAX) = {verb}; EXEC(@sql);", sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@version", version);
        command.ExecuteNonQuery();
    }

    private static int ReadVersionCore(SqlConnection sqlConnection, SqlTransaction? transaction, string schema)
    {
        using var command = new SqlCommand(
            "IF OBJECT_ID(QUOTENAME(@schema) + N'.schema_version') IS NULL " +
            "SELECT 0 " +
            "ELSE " +
            "BEGIN " +
            "DECLARE @sql NVARCHAR(MAX) = 'SELECT version FROM ' + QUOTENAME(@schema) + '.schema_version'; " +
            "EXEC(@sql); " +
            "END",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", schema);
        var result = command.ExecuteScalar();
        return result is null or DBNull ? 0 : Convert.ToInt32(result);
    }
}
