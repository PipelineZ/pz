using System.Globalization;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres;

/// <summary>Pure helpers + admin queries shared by the Postgres cdc source
/// (<see cref="PostgresCdcPartition"/>) and `pz cdc status`/`drop`. Holds the
/// canonical vocabulary the poll path and the engine depend on EXACTLY: the LSN string form
/// (<c>{(ulong)lsn:X16}</c>, 16 uppercase hex), the default slot/publication names, and the
/// server-side prerequisite checks. No connection config or SQL text ever appears in the messages this
/// class raises; prerequisite/conflict messages carry the exact remediation statement as their payload
/// so the operator can copy-paste the fix.</summary>
internal static class PostgresCdc
{
    private const int MaxIdentifier = 63; // postgres identifier limit (NAMEDATALEN - 1)

    /// <summary>The default replication slot name for a dataset: <c>pz_{source}_{dataset}</c> sanitized,
    /// unless the dataset declares an explicit <see cref="DatasetSpec.ChangeCaptureSlot"/> (used verbatim).</summary>
    public static string SlotName(DatasetSpec spec) =>
        spec.ChangeCaptureSlot is { Length: > 0 } explicitSlot
            ? explicitSlot
            : Sanitize($"pz_{spec.Source}_{spec.Dataset}");

    /// <summary>The publication the prerequisite remediation names: the dataset's <c>publication:</c>
    /// option if set, else <c>pz_{source}</c> sanitized.</summary>
    public static string PublicationName(DatasetSpec spec) =>
        spec.Options.TryGetValue("publication", out var p) && p?.ToString() is { Length: > 0 } explicitPub
            ? explicitPub
            : Sanitize($"pz_{spec.Source}");

    // Lowercase, fold every non-[a-z0-9_] to '_', cap at the postgres identifier limit -- the shared
    // canonical derivation the admin path and the engine both reproduce.
    private static string Sanitize(string raw)
    {
        var lower = raw.ToLowerInvariant();
        var sb = new StringBuilder(lower.Length);
        foreach (var c in lower)
        {
            sb.Append(c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' ? c : '_');
        }

        var s = sb.ToString();
        return s.Length <= MaxIdentifier ? s : s[..MaxIdentifier];
    }

    /// <summary>Canonical LSN string form: 16 uppercase hex digits of the raw 64-bit WAL position. This
    /// is the sync token form (a bare 16-hex commit LSN) that the engine stores and replays.</summary>
    public static string FormatLsn(NpgsqlLogSequenceNumber lsn) =>
        ((ulong)lsn).ToString("X16", CultureInfo.InvariantCulture);

    /// <summary>Inverse of <see cref="FormatLsn"/>: parses the 16-hex form back to an LSN. (Not
    /// <see cref="NpgsqlLogSequenceNumber.Parse"/>, which expects postgres's <c>X/X</c> text form, not
    /// the bare 16-hex canonical form pz stores.)</summary>
    public static NpgsqlLogSequenceNumber ParseLsn(string x16) =>
        (NpgsqlLogSequenceNumber)ulong.Parse(x16, NumberStyles.HexNumber, CultureInfo.InvariantCulture);

    /// <summary>The cdc entity's schema and table, from the dataset NAME -- schema defaults to
    /// <c>public</c> for an unqualified name.</summary>
    public static (string Schema, string Table) SchemaAndTable(DatasetSpec spec) =>
        PgDdl.SplitEntity(spec.Dataset);

    /// <summary>The plain-SQL snapshot projection: the three change-row header columns
    /// (<c>_pz_op='insert'</c>, the all-zeros <c>_pz_lsn</c>, null <c>_pz_changed_at</c>) prepended to
    /// the table's own columns, read through the regular <see cref="Batches.DataReaderSource"/> path.</summary>
    public static string SnapshotSelect(string schema, string table) =>
        "select cast('insert' as text) as _pz_op, " +
        "cast('0000000000000000-000000000' as text) as _pz_lsn, " +
        "cast(null as timestamptz) as _pz_changed_at, " +
        $"t.* from {PgDdl.Quote(schema)}.{PgDdl.Quote(table)} t";

    /// <summary>Runs the online prerequisite checks (regular connection) for a cdc dataset and returns
    /// one remediation statement per UNMET prerequisite (empty = all met). Aggregates every check so the
    /// caller can raise a single actionable error naming all of them at once.</summary>
    public static async Task<IReadOnlyList<string>> ValidatePrerequisitesAsync(
        NpgsqlConnection conn, DatasetSpec spec, CancellationToken ct)
    {
        var (schema, table) = SchemaAndTable(spec);
        var unmet = new List<string>();

        // pgoutput binary mode (PostgresCdcPartition's StartReplication binary: true) is a PostgreSQL 14+
        // feature. On 12/13 the first-run snapshot succeeds regardless (it never touches replication), so
        // without this check the failure only surfaces later, at the poll, with a misleading
        // publication-remediation message.
        var versionNum = Convert.ToInt32(
            (await ScalarAsync(conn, "select current_setting('server_version_num')::int", ct).ConfigureAwait(false))!,
            CultureInfo.InvariantCulture);
        if (versionNum < 140000)
        {
            unmet.Add(
                "PostgreSQL 14 or newer is required for cdc datasets (pgoutput binary mode); " +
                $"server reports {versionNum}");
        }

        var walLevel = (string?)await ScalarAsync(conn, "show wal_level", ct).ConfigureAwait(false);
        if (!string.Equals(walLevel, "logical", StringComparison.Ordinal))
        {
            unmet.Add("ALTER SYSTEM SET wal_level = logical; -- then restart postgres");
        }

        var hasReplication = await ScalarAsync(
            conn, "select rolreplication or rolsuper from pg_roles where rolname = current_user", ct)
            .ConfigureAwait(false) is bool rep && rep;
        if (!hasReplication)
        {
            var currentUser = (string)(await ScalarAsync(conn, "select current_user", ct).ConfigureAwait(false))!;
            unmet.Add($"ALTER ROLE {currentUser} REPLICATION;");
        }

        // The SPECIFIC publication pz will stream off (PublicationName(spec)) must exist AND cover the
        // table -- either as a FOR ALL TABLES publication (puballtables) or by explicit membership. Any
        // OTHER publication covering the table is deliberately NOT sufficient: the poll path names this one
        // publication at StartReplication, so accepting a different one at prereq time would pass the first
        // run and then fail the poll.
        await using (var cmd = new NpgsqlCommand(
            """
            select exists (
                select 1 from pg_publication p
                where p.pubname = @pub
                  and (p.puballtables
                       or exists (
                           select 1 from pg_publication_tables t
                           where t.pubname = p.pubname and t.schemaname = @schema and t.tablename = @table))
            )
            """,
            conn))
        {
            cmd.Parameters.AddWithValue("pub", PublicationName(spec));
            cmd.Parameters.AddWithValue("schema", schema);
            cmd.Parameters.AddWithValue("table", table);
            var covered = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) is bool cov && cov;
            if (!covered)
            {
                unmet.Add(
                    $"CREATE PUBLICATION {PublicationName(spec)} FOR TABLE " +
                    $"{PgDdl.Quote(schema)}.{PgDdl.Quote(table)};");
            }
            else if (versionNum >= 150000)
            {
                // pg_publication_tables.attnames (PG15+ only, hence the version gate) is non-null exactly
                // when the publication was created/altered with a column list (FOR TABLE t (id, name)).
                // Columns it omits never appear in the pgoutput tuple at all -- PostgresCdcPartition decodes
                // them as null -- and a merge would then overwrite real data with those nulls. A column list
                // that happens to name every column still trips this: it is not the "no column list" state
                // the covered-columns comparison below intentionally treats as equivalent to it.
                var attnames = await ScalarAsync(
                    conn,
                    """
                    select t.attnames from pg_publication_tables t
                    where t.pubname = @pub and t.schemaname = @schema and t.tablename = @table
                    """,
                    ct, ("pub", PublicationName(spec)), ("schema", schema), ("table", table)) as string[];

                if (attnames is not null)
                {
                    var allColumns = await ScalarArrayAsync(
                        conn,
                        "select array_agg(column_name::text) from information_schema.columns " +
                        "where table_schema = @schema and table_name = @table",
                        ct, ("schema", schema), ("table", table)).ConfigureAwait(false);
                    if (!allColumns.All(c => attnames.Contains(c, StringComparer.Ordinal)))
                    {
                        unmet.Add(
                            $"publication {PublicationName(spec)} declares a column list for " +
                            $"{PgDdl.Quote(schema)}.{PgDdl.Quote(table)}; cdc requires all columns -- " +
                            "recreate it without a column list");
                    }
                }
            }
        }

        return unmet;
    }

    /// <summary>Discovers the change-key columns for a table: the replica-identity index's columns
    /// (<c>indisreplident</c>) in index order, falling back to the primary key (<c>indisprimary</c>).
    /// Empty when neither exists (the caller then reports no keys).</summary>
    public static async Task<IReadOnlyList<string>> DiscoverKeyColumnsAsync(
        NpgsqlConnection conn, string schema, string table, CancellationToken ct)
    {
        var replident = await KeyColumnsForAsync(conn, schema, table, "indisreplident", ct).ConfigureAwait(false);
        return replident.Count > 0
            ? replident
            : await KeyColumnsForAsync(conn, schema, table, "indisprimary", ct).ConfigureAwait(false);
    }

    // `flag` is a fixed internal column name (indisreplident|indisprimary), never user input.
    // indkey is an int2vector; its text form is space-separated, so string_to_array + unnest WITH
    // ORDINALITY recovers the column positions in index-key order robustly.
    private static async Task<IReadOnlyList<string>> KeyColumnsForAsync(
        NpgsqlConnection conn, string schema, string table, string flag, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"""
            select a.attname
            from pg_index i
            join pg_class c on c.oid = i.indrelid
            join pg_namespace n on n.oid = c.relnamespace
            cross join lateral unnest(string_to_array(i.indkey::text, ' ')::int[]) with ordinality as k(attnum, ord)
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
            where n.nspname = @schema and c.relname = @table and i.{flag}
            order by k.ord
            """,
            conn);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("table", table);

        var columns = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    /// <summary>First-run slot-lifecycle policy on a regular connection: no existing slot is a no-op; an
    /// existing pgoutput slot is dropped so the full-refresh re-snapshot recreates it fresh; an existing
    /// slot with a DIFFERENT plugin is a conflict that pz refuses to touch.</summary>
    public static async Task ResolveSlotConflictAsync(NpgsqlConnection conn, string slotName, CancellationToken ct)
    {
        var plugin = (string?)await ScalarAsync(
            conn, "select plugin from pg_replication_slots where slot_name = @name", ct,
            ("name", slotName)).ConfigureAwait(false);

        if (plugin is null)
        {
            return; // fresh -- CreatePgOutputReplicationSlot will make it
        }

        if (!string.Equals(plugin, "pgoutput", StringComparison.Ordinal))
        {
            throw new PzConnectorException(
                $"postgres cdc: slot conflict -- replication slot '{slotName}' already exists with output " +
                $"plugin '{plugin}', not pgoutput; pz will not reuse a foreign slot -- run `pz cdc drop` to " +
                "remove it (or set sync.slot to a name pz owns)",
                isTransient: false);
        }

        // Existing pz pgoutput slot on a first run / --full-refresh: drop it so the snapshot is taken
        // against a fresh consistent point.
        await using var drop = new NpgsqlCommand("select pg_drop_replication_slot(@name)", conn);
        drop.Parameters.AddWithValue("name", slotName);
        await drop.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>`pz cdc drop`: drops <paramref name="slotName"/> if it exists; a missing slot
    /// is a no-op (idempotent -- dropping an already-dropped dataset must never error).</summary>
    public static async Task DropSlotIfExistsAsync(NpgsqlConnection conn, string slotName, CancellationToken ct)
    {
        var exists = await ScalarAsync(
            conn, "select 1 from pg_replication_slots where slot_name = @name", ct, ("name", slotName))
            .ConfigureAwait(false) is not null;
        if (!exists)
        {
            return;
        }

        await using var drop = new NpgsqlCommand("select pg_drop_replication_slot(@name)", conn);
        drop.Parameters.AddWithValue("name", slotName);
        await drop.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task<object?> ScalarAsync(
        NpgsqlConnection conn, string sql, CancellationToken ct, params (string Name, object Value)[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in parameters)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is DBNull ? null : result;
    }

    // array_agg over an empty/absent table yields a single null row rather than an empty array -- treat
    // that the same as "no columns" instead of a null-reference on the caller's .All().
    private static async Task<string[]> ScalarArrayAsync(
        NpgsqlConnection conn, string sql, CancellationToken ct, params (string Name, object Value)[] parameters) =>
        await ScalarAsync(conn, sql, ct, parameters).ConfigureAwait(false) as string[] ?? [];
}
