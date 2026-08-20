using System.Data;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>Pure helpers + admin queries shared by the SQL Server cdc source
/// (<see cref="SqlServerCdcPartition"/>) and `pz cdc status`/`drop`. Holds the
/// canonical vocabulary the engine depends on EXACTLY: the <c>_pz_lsn</c> string form
/// (<c>{start_lsn 20 uppercase hex}-{seqval 20 uppercase hex}</c>, no <c>0x</c> prefix), the default
/// capture-instance name, and the server-side prerequisite checks. No connection config or SQL text
/// ever appears in the messages this class raises; prerequisite messages carry the exact remediation
/// statement as their payload so the operator can copy-paste the fix.</summary>
internal static class SqlServerCdc
{
    private const string ZeroLsnHex = "00000000000000000000"; // 20 zero hex digits = binary(10) of zeros

    /// <summary>The all-zeros <c>_pz_lsn</c> form the first-run snapshot stamps on every row, mirroring
    /// the postgres snapshot: 41 chars, <c>{20 zeros}-{20 zeros}</c>.</summary>
    public const string SnapshotLsn = ZeroLsnHex + "-" + ZeroLsnHex;

    /// <summary>The cdc entity's schema and table, from the dataset NAME -- schema defaults to
    /// <c>dbo</c> for an unqualified name. A cdc dataset cannot fail to name a table, so there is no
    /// "change capture requires 'table'" guard.</summary>
    public static (string Schema, string Table) SchemaAndTable(DatasetSpec spec) =>
        MsDdl.SplitEntity(spec.Dataset);

    /// <summary>The capture instance name: the dataset's <c>capture_instance:</c> option if set, else
    /// <c>{schema}_{table}</c> -- SQL Server's own default naming convention for <c>sp_cdc_enable_table</c>
    /// when <c>@capture_instance</c> is not passed.</summary>
    public static string CaptureInstance(DatasetSpec spec)
    {
        if (spec.Options.TryGetValue("capture_instance", out var ci) && ci?.ToString() is { Length: > 0 } explicitInstance)
        {
            return explicitInstance;
        }

        var (schema, table) = SchemaAndTable(spec);
        return $"{schema}_{table}";
    }

    /// <summary>Canonical LSN string form: 20 uppercase hex digits of the 10-byte LSN (no <c>0x</c>
    /// prefix) -- the sync-token form the engine stores and replays.</summary>
    public static string FormatLsn(byte[] lsn) => Convert.ToHexString(lsn);

    /// <summary>Inverse of <see cref="FormatLsn"/>.</summary>
    public static byte[] ParseLsn(string hex20) => Convert.FromHexString(hex20);

    /// <summary>Byte-wise (== numeric, LSNs are stored big-endian-ordered) comparison of two 10-byte LSNs.</summary>
    public static int CompareLsn(byte[] a, byte[] b)
    {
        for (var i = 0; i < a.Length; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0)
            {
                return c;
            }
        }

        return 0;
    }

    /// <summary>The plain-SQL snapshot projection: the three change-row header columns
    /// (<c>_pz_op='insert'</c>, the all-zeros <c>_pz_lsn</c>, null <c>_pz_changed_at</c>) prepended to
    /// the table's own columns, read through the regular <see cref="SqlServerArrowReader"/> path.</summary>
    public static string SnapshotSelect(string schema, string table) =>
        "select cast('insert' as varchar(6)) as _pz_op, " +
        $"cast('{SnapshotLsn}' as varchar(41)) as _pz_lsn, " +
        "cast(null as datetime2) as _pz_changed_at, " +
        $"t.* from {MsDdl.Quote(schema)}.{MsDdl.Quote(table)} t";

    /// <summary>The bounded change-window SELECT (spec's window SQL): maps <c>__$operation</c>
    /// (1=delete, 2=insert, else=update per <c>N'all'</c> mode), concatenates start_lsn/seqval into the
    /// canonical <c>_pz_lsn</c> form, resolves <c>_pz_changed_at</c> via <c>fn_cdc_map_lsn_to_time</c>,
    /// then projects the data columns explicitly (never <c>t.*</c> -- that would also pull the
    /// <c>__$</c>-prefixed metadata columns). <paramref name="dataColumns"/> comes from a probe of the
    /// BASE table (see <see cref="ProbeBaseColumnsAsync"/>): the window projects exactly the base
    /// table's columns. <c>@from</c>/<c>@to</c> are bound as <see cref="SqlDbType.Binary"/>(10)
    /// parameters by the caller -- never string-spliced. The capture-instance identifier embedded in
    /// the function name is bracket-quoted via <see cref="MsDdl.Quote"/>.</summary>
    public static string BuildWindowSelect(string captureInstance, IReadOnlyList<string> dataColumns)
    {
        var fn = $"cdc.{MsDdl.Quote($"fn_cdc_get_all_changes_{captureInstance}")}";
        var columns = string.Join(", ", dataColumns.Select(c => $"t.{MsDdl.Quote(c)}"));
        return
            "select case [__$operation] when 1 then 'delete' when 2 then 'insert' else 'update' end as _pz_op, " +
            "upper(convert(varchar(40), [__$start_lsn], 2)) + '-' + upper(convert(varchar(40), [__$seqval], 2)) as _pz_lsn, " +
            "cast(sys.fn_cdc_map_lsn_to_time([__$start_lsn]) as datetime2) as _pz_changed_at, " +
            $"{columns} " +
            $"from {fn}(@from, @to, N'all') t " +
            "order by [__$start_lsn], [__$seqval]";
    }

    /// <summary>Probes the BASE table's own column names, in table order (<c>select top 0 *</c>) --
    /// used both to build the window SELECT's explicit column list and (by <see
    /// cref="SqlServerSource.GetSchemaAsync"/>, via <see cref="SnapshotSelect"/>'s <c>t.*</c>) to
    /// declare the dataset's Arrow schema. The window read projects exactly these columns off the
    /// change table, so a captured-column set that differs from the base table's current columns still
    /// yields the schema-declared shape (a divergence SQL Server itself would reject at read time).</summary>
    public static async Task<IReadOnlyList<string>> ProbeBaseColumnsAsync(
        SqlConnection conn, string schema, string table, CancellationToken ct)
    {
        await using var cmd = new SqlCommand($"select top 0 * from {MsDdl.Quote(schema)}.{MsDdl.Quote(table)}", conn);
        await using var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly, ct).ConfigureAwait(false);
        var columns = new List<string>(reader.FieldCount);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns.Add(reader.GetName(i));
        }

        return columns;
    }

    /// <summary><c>sys.fn_cdc_get_max_lsn()</c>: the highest LSN cdc has captured for the database, or
    /// null if the capture job has never primed.</summary>
    public static async Task<byte[]?> GetMaxLsnAsync(SqlConnection conn, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("select sys.fn_cdc_get_max_lsn()", conn);
        return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as byte[];
    }

    /// <summary>db-level cdc gate (<c>sys.databases.is_cdc_enabled</c>): until <c>sp_cdc_enable_db</c>
    /// has run, the <c>cdc</c> schema itself does not exist, so <see cref="GetMinLsnAsync"/> (which
    /// calls a function living in that schema) would raise "invalid object name" rather than returning
    /// null. Callers must check this FIRST and skip <see cref="GetMinLsnAsync"/> when false.</summary>
    public static async Task<bool> IsDbCdcEnabledAsync(SqlConnection conn, CancellationToken ct) =>
        await ScalarAsync<bool?>(conn, "select is_cdc_enabled from sys.databases where name = db_name()", ct)
            .ConfigureAwait(false) == true;

    /// <summary><c>sys.fn_cdc_get_min_lsn(@instance)</c>: the lowest LSN still retained for the capture
    /// instance, or null if the instance does not exist. Requires db-level cdc to already be enabled
    /// (see <see cref="IsDbCdcEnabledAsync"/>) -- otherwise the <c>cdc</c> schema doesn't exist and this
    /// throws "invalid object name" instead.</summary>
    public static async Task<byte[]?> GetMinLsnAsync(SqlConnection conn, string captureInstance, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("select sys.fn_cdc_get_min_lsn(@instance)", conn);
        cmd.Parameters.Add(new SqlParameter("@instance", SqlDbType.NVarChar, 386) { Value = captureInstance });
        return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as byte[];
    }

    /// <summary><c>sys.fn_cdc_increment_lsn(@lsn)</c>: the next LSN after <paramref name="lsn"/>. The
    /// change-window functions treat their <c>@from</c> bound as INCLUSIVE, so the poll path increments
    /// the prior token before using it as <c>@from</c> -- otherwise the boundary change (already
    /// consumed last run) would be re-read.</summary>
    public static async Task<byte[]> IncrementLsnAsync(SqlConnection conn, byte[] lsn, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("select sys.fn_cdc_increment_lsn(@lsn)", conn);
        cmd.Parameters.Add(new SqlParameter("@lsn", SqlDbType.Binary, 10) { Value = lsn });
        return (byte[])(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;
    }

    /// <summary>Runs the three online prerequisite checks for a cdc dataset and returns one remediation
    /// statement per UNMET prerequisite (empty = all met). Aggregates every check it CAN run into one
    /// pass: the table/job checks are independent of each other, but the table-captured check needs the
    /// <c>cdc</c> schema to exist (created by <c>sp_cdc_enable_db</c>), so it is skipped -- not
    /// errored -- while the db-level prerequisite is unmet (re-running validation after fixing the db
    /// level surfaces it on the next pass).</summary>
    public static async Task<IReadOnlyList<string>> ValidatePrerequisitesAsync(
        SqlConnection conn, DatasetSpec spec, CancellationToken ct)
    {
        var (schema, table) = SchemaAndTable(spec);
        var instance = CaptureInstance(spec);
        var unmet = new List<string>();

        var dbEnabled = await IsDbCdcEnabledAsync(conn, ct).ConfigureAwait(false);
        if (!dbEnabled)
        {
            unmet.Add("EXEC sys.sp_cdc_enable_db");
        }
        else
        {
            var captured = await ScalarAsync<int?>(
                conn, "select 1 from cdc.change_tables where capture_instance = @instance", ct,
                ("@instance", instance)).ConfigureAwait(false) is not null;
            if (!captured)
            {
                unmet.Add(
                    $"EXEC sys.sp_cdc_enable_table @source_schema = N'{schema}', @source_name = N'{table}', " +
                    "@role_name = NULL");
            }
        }

        var jobPresent = await ScalarAsync<int?>(
            conn, "select 1 from msdb.dbo.cdc_jobs where job_type = N'capture' and database_id = db_id()", ct)
            .ConfigureAwait(false) is not null;
        if (!jobPresent)
        {
            unmet.Add(
                "capture job not found -- ensure SQL Server Agent is running (MSSQL_AGENT_ENABLED=true in containers)");
        }

        return unmet;
    }

    /// <summary>Discovers the change-key columns for a capture instance: the cdc index columns
    /// (<c>cdc.index_columns</c>, the unique index cdc itself picked to identify a row -- usually the
    /// PK) in ordinal order, falling back to the base table's primary key when the instance has none
    /// (e.g. <c>@supports_net_changes = 0</c> with no PK at capture time).</summary>
    public static async Task<IReadOnlyList<string>> DiscoverKeyColumnsAsync(
        SqlConnection conn, string schema, string table, string captureInstance, CancellationToken ct)
    {
        var indexColumns = await ChangeIndexColumnsAsync(conn, captureInstance, ct).ConfigureAwait(false);
        return indexColumns.Count > 0
            ? indexColumns
            : await PrimaryKeyColumnsAsync(conn, schema, table, ct).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<string>> ChangeIndexColumnsAsync(
        SqlConnection conn, string captureInstance, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(
            """
            select ic.column_name
            from cdc.index_columns ic
            join cdc.change_tables ct on ic.object_id = ct.object_id
            where ct.capture_instance = @instance
            order by ic.index_ordinal
            """,
            conn);
        cmd.Parameters.Add(new SqlParameter("@instance", SqlDbType.NVarChar, 386) { Value = captureInstance });
        return await ReadColumnAsync(cmd, ct).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<string>> PrimaryKeyColumnsAsync(
        SqlConnection conn, string schema, string table, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(
            """
            select c.name
            from sys.indexes i
            join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
            join sys.columns c on ic.object_id = c.object_id and ic.column_id = c.column_id
            join sys.tables t on i.object_id = t.object_id
            join sys.schemas s on t.schema_id = s.schema_id
            where s.name = @schema and t.name = @table and i.is_primary_key = 1
            order by ic.key_ordinal
            """,
            conn);
        cmd.Parameters.Add(new SqlParameter("@schema", SqlDbType.NVarChar, 128) { Value = schema });
        cmd.Parameters.Add(new SqlParameter("@table", SqlDbType.NVarChar, 128) { Value = table });
        return await ReadColumnAsync(cmd, ct).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<string>> ReadColumnAsync(SqlCommand cmd, CancellationToken ct)
    {
        var values = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }

    private static async Task<T?> ScalarAsync<T>(
        SqlConnection conn, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var cmd = new SqlCommand(sql, conn);
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? default : (T)result;
    }
}
