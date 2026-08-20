using Apache.Arrow;
using Apache.Arrow.Types;
using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres;

/// <summary>Postgres sink: the Arrow -> postgres
/// DDL/NpgsqlDbType reverse map, <c>CREATE TABLE</c> generation for a missing target, and the
/// <c>information_schema.columns</c> comparison that backs <c>schema_policy: fail_on_change</c> (the
/// default). The matrix is exactly the mirror of <see cref="PgTypeMap"/> (source direction):
/// int32/int64/double/decimal128(38,9)/utf8/bool/date32/timestamp(us,UTC) map to integer/bigint/double
/// precision/numeric(38,9)/text/boolean/date/timestamptz respectively. A merge target that is
/// auto-created here gets <c>unique (&lt;keys&gt;)</c> added to the CREATE TABLE (ON CONFLICT requires a
/// unique constraint/index covering exactly the conflict columns); a PRE-EXISTING merge target is instead
/// verified (via <see cref="EnsureUniqueConstraintOnKeysAsync"/>) to already have one, BEFORE the
/// COPY/finalize steps run, so a missing constraint fails fast with a clean, actionable error.</summary>
internal static class PgDdl
{
    /// <summary>The nullable soft-delete marker column, appended to the
    /// desired schema (create-table and drift-check paths alike) only when <c>on_delete: soft</c> --
    /// see <see cref="EnsureTargetAsync"/> and <see cref="PostgresSinkWriteSession"/>.</summary>
    public const string SoftDeleteColumn = "_pz_deleted_at";

    public static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    /// <summary>The entity name IS the object name. Splits
    /// <c>schema.table</c> on its single dot; an unqualified name takes <paramref name="defaultSchema"/>.
    /// Three or more parts are refused
    /// rather than quoted as one identifier -- <c>Quote("db.raw")</c> is the single identifier literally
    /// named <c>db.raw</c>, so a cross-database name would silently read nothing.</summary>
    public static (string Schema, string Table) SplitEntity(string entity, string defaultSchema = "public")
    {
        var parts = entity.Split('.');
        return parts.Length switch
        {
            1 => (defaultSchema, parts[0]),
            2 => (parts[0], parts[1]),
            _ => throw new PzConnectorException(
                $"entity '{entity}': expected '<table>' or '<schema>.<table>'", isTransient: false),
        };
    }

    /// <summary>The DDL type keyword used both to CREATE the target when missing and as the expected
    /// value compared against <c>information_schema.columns</c> for an existing target.</summary>
    public static string PgTypeFor(Field field) => field.DataType.TypeId switch
    {
        ArrowTypeId.Int32 => "integer",
        ArrowTypeId.Int64 => "bigint",
        ArrowTypeId.Double => "double precision",
        ArrowTypeId.Decimal128 => "numeric(38,9)",
        ArrowTypeId.String => "text",
        ArrowTypeId.Boolean => "boolean",
        ArrowTypeId.Date32 => "date",
        ArrowTypeId.Timestamp => "timestamptz",
        _ => throw new PzConnectorException(
            $"column '{field.Name}': postgres sink v0 does not support Arrow type '{field.DataType}'",
            isTransient: false),
    };

    /// <summary>The <see cref="NpgsqlDbType"/> used to write each column's cells via
    /// <c>NpgsqlBinaryImporter</c> -- consistent with <see cref="PgTypeFor"/>'s DDL choice for the same
    /// Arrow type.</summary>
    public static NpgsqlDbType NpgsqlTypeFor(Field field) => field.DataType.TypeId switch
    {
        ArrowTypeId.Int32 => NpgsqlDbType.Integer,
        ArrowTypeId.Int64 => NpgsqlDbType.Bigint,
        ArrowTypeId.Double => NpgsqlDbType.Double,
        ArrowTypeId.Decimal128 => NpgsqlDbType.Numeric,
        ArrowTypeId.String => NpgsqlDbType.Text,
        ArrowTypeId.Boolean => NpgsqlDbType.Boolean,
        ArrowTypeId.Date32 => NpgsqlDbType.Date,
        ArrowTypeId.Timestamp => NpgsqlDbType.TimestampTz,
        _ => throw new PzConnectorException(
            $"column '{field.Name}': postgres sink v0 does not support Arrow type '{field.DataType}'",
            isTransient: false),
    };

    /// <summary>Builds the CREATE TABLE for a missing target. For <c>mode: merge</c> with a non-empty
    /// <paramref name="keys"/>, appends a table-level <c>unique (&lt;keys&gt;)</c> constraint -- ON
    /// CONFLICT requires a unique constraint/index covering exactly the conflict columns, and this is the
    /// only place a genuinely-missing target is created, so it is the only place that constraint can be
    /// added for free. Every other mode (and merge with no keys, which DagCompiler's PZ0209 already
    /// forbids upstream) leaves the table unchanged.</summary>
    public static string BuildCreateTableSql(
        string pgSchema, string table, Schema arrowSchema, string mode, IReadOnlyList<string> keys,
        string? onDelete = null)
    {
        var columns = arrowSchema.FieldsList.Select(f => $"{Quote(f.Name)} {PgTypeFor(f)}").ToList();
        if (string.Equals(mode, "merge", StringComparison.Ordinal) && keys.Count > 0)
        {
            columns.Add($"unique ({string.Join(", ", keys.Select(Quote))})");
        }

        if (string.Equals(onDelete, "soft", StringComparison.Ordinal))
        {
            columns.Add($"{Quote(SoftDeleteColumn)} timestamptz");
        }

        return $"create table {Quote(pgSchema)}.{Quote(table)} ({string.Join(", ", columns)})";
    }

    /// <summary>Ensures the target exists for a write session: creates it (via <see
    /// cref="BuildCreateTableSql"/>) if missing, otherwise enforces <c>schema_policy</c>.
    /// <c>fail_on_change</c> (the default) compares every schema-declared column's name and type against
    /// <c>information_schema.columns</c>, throwing on the FIRST mismatch (missing column or type drift).
    /// <c>evolve</c> is a clean not-supported error (descope valve -- schema evolution is not implemented
    /// in v0). Runs inside the caller's transaction so a missing-target CREATE TABLE is rolled back along
    /// with everything else if the session is later aborted.</summary>
    public static async Task EnsureTargetAsync(
        NpgsqlConnection connection, NpgsqlTransaction tx, string schemaPolicy,
        string pgSchema, string table, Schema arrowSchema, string outputName,
        string mode, IReadOnlyList<string> keys, string? onDelete, CancellationToken ct)
    {
        if (string.Equals(schemaPolicy, "evolve", StringComparison.OrdinalIgnoreCase))
        {
            throw new PzConnectorException(
                $"output '{outputName}': schema_policy 'evolve' is not supported by the postgres sink in " +
                "v0 -- hint: use 'fail_on_change' (the default) and align the target schema by hand",
                isTransient: false);
        }

        var exists = await TableExistsAsync(connection, tx, pgSchema, table, ct).ConfigureAwait(false);
        if (!exists)
        {
            await using var create = new NpgsqlCommand(
                BuildCreateTableSql(pgSchema, table, arrowSchema, mode, keys, onDelete), connection, tx);
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return;
        }

        // fail_on_change (default, and the only other value v0 recognizes): existing target's columns
        // must match name-for-name and type-for-type (see NormalizeTypeName for the aliases postgres's
        // information_schema actually reports, e.g. "timestamp with time zone" for timestamptz).
        var existingColumns = await LoadExistingColumnsAsync(connection, tx, pgSchema, table, ct).ConfigureAwait(false);
        foreach (var field in arrowSchema.FieldsList)
        {
            var expected = ExpectedColumnType(field);
            if (!existingColumns.TryGetValue(field.Name, out var actual))
            {
                throw new PzConnectorException(
                    $"output '{outputName}': target column '{field.Name}' is missing from " +
                    $"{pgSchema}.{table} (expected '{expected.Display}') -- hint: schema_policy is " +
                    "'fail_on_change' (the default); align the target table's columns by hand, or drop " +
                    "and let the sink recreate it",
                    isTransient: false);
            }

            if (!expected.Equals(actual))
            {
                throw new PzConnectorException(
                    $"output '{outputName}': target column '{field.Name}' in {pgSchema}.{table} has type " +
                    $"'{actual.Display}', expected '{expected.Display}' -- hint: schema_policy is " +
                    "'fail_on_change' (the default); align the target column's type by hand, or drop and " +
                    "let the sink recreate the table",
                    isTransient: false);
            }
        }

        // on_delete: soft requires the nullable _pz_deleted_at marker column.
        // fail_on_change (default) treats a missing column as drift, naming it in the error, same as
        // every other declared column; schema_policy: additive instead ALTERs it in -- scoped to this
        // one soft-delete column, not general schema evolution (out of scope here).
        if (string.Equals(onDelete, "soft", StringComparison.Ordinal))
        {
            var expectedSoft = new PgColumnType("timestamptz", null, null);
            if (!existingColumns.TryGetValue(SoftDeleteColumn, out var actualSoft))
            {
                if (string.Equals(schemaPolicy, "additive", StringComparison.OrdinalIgnoreCase))
                {
                    await using var alter = new NpgsqlCommand(
                        $"alter table {Quote(pgSchema)}.{Quote(table)} add column {Quote(SoftDeleteColumn)} timestamptz",
                        connection, tx);
                    await alter.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    throw new PzConnectorException(
                        $"output '{outputName}': target column '{SoftDeleteColumn}' is missing from " +
                        $"{pgSchema}.{table} (expected 'timestamptz') -- hint: schema_policy is " +
                        "'fail_on_change' (the default); add the column by hand (or set schema_policy: " +
                        "additive), or drop and let the sink recreate it",
                        isTransient: false);
                }
            }
            else if (!expectedSoft.Equals(actualSoft))
            {
                throw new PzConnectorException(
                    $"output '{outputName}': target column '{SoftDeleteColumn}' in {pgSchema}.{table} has " +
                    $"type '{actualSoft.Display}', expected 'timestamptz' -- hint: schema_policy is " +
                    "'fail_on_change' (the default); align the target column's type by hand, or drop and " +
                    "let the sink recreate the table",
                    isTransient: false);
            }
        }

        // A PRE-EXISTING merge target must already carry a unique constraint/index covering
        // EXACTLY the merge keys -- ON CONFLICT (<keys>) fails at finalize time otherwise, but checking
        // here (before the temp table/COPY are ever set up) fails fast with a clean, actionable error.
        if (string.Equals(mode, "merge", StringComparison.Ordinal) && keys.Count > 0)
        {
            await EnsureUniqueConstraintOnKeysAsync(connection, tx, pgSchema, table, keys, outputName, ct)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Verifies at least one unique index (a plain <c>unique</c> constraint/index, or the index
    /// backing a primary key -- both surface identically in <c>pg_index</c> with <c>indisunique</c> true)
    /// on <paramref name="table"/> covers EXACTLY the column set in <paramref name="keys"/> (order-
    /// insensitive, but not a superset/subset). Excludes PARTIAL indexes (<c>indpred is not null</c>, e.g.
    /// <c>create unique index on t(id) where active</c>) and EXPRESSION indexes (<c>indexprs is not
    /// null</c>): both can cover the key column set yet neither can back an <c>ON CONFLICT (&lt;keys&gt;)</c>
    /// arbiter, so admitting them here would pass this pre-flight check only to fail later at finalize with
    /// a raw postgres error ("no unique or exclusion constraint matching ON CONFLICT specification") instead
    /// of this method's clean, actionable one. Throws a clean, actionable <see cref="PzConnectorException"/>
    /// naming the missing constraint if none matches.</summary>
    private static async Task EnsureUniqueConstraintOnKeysAsync(
        NpgsqlConnection connection, NpgsqlTransaction tx, string pgSchema, string table,
        IReadOnlyList<string> keys, string outputName, CancellationToken ct)
    {
        var keySet = new HashSet<string>(keys, StringComparer.Ordinal);

        await using var command = new NpgsqlCommand(
            """
            select i.indexrelid, a.attname
            from pg_index i
            join pg_class c on c.oid = i.indrelid
            join pg_namespace n on n.oid = c.relnamespace
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
            where n.nspname = @schema and c.relname = @table and i.indisunique
                and i.indpred is null and i.indexprs is null
            """,
            connection, tx);
        command.Parameters.AddWithValue("schema", pgSchema);
        command.Parameters.AddWithValue("table", table);

        var byIndex = new Dictionary<uint, HashSet<string>>();
        await using (var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var indexOid = reader.GetFieldValue<uint>(0);
                var column = reader.GetString(1);
                if (!byIndex.TryGetValue(indexOid, out var columns))
                {
                    columns = new HashSet<string>(StringComparer.Ordinal);
                    byIndex[indexOid] = columns;
                }

                columns.Add(column);
            }
        }

        var satisfied = byIndex.Values.Any(columns => columns.SetEquals(keySet));
        if (!satisfied)
        {
            var keyList = string.Join(", ", keys);
            throw new PzConnectorException(
                $"output '{outputName}': merge target {pgSchema}.{table} needs a unique constraint on " +
                $"({keyList}) -- hint: add `unique ({keyList})` (or an equivalent unique index) to the " +
                "existing target table so ON CONFLICT can resolve merges",
                isTransient: false);
        }
    }

    private static async Task<bool> TableExistsAsync(
        NpgsqlConnection connection, NpgsqlTransaction tx, string pgSchema, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "select 1 from information_schema.tables where table_schema = @schema and table_name = @table",
            connection, tx);
        command.Parameters.AddWithValue("schema", pgSchema);
        command.Parameters.AddWithValue("table", table);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    private static async Task<Dictionary<string, PgColumnType>> LoadExistingColumnsAsync(
        NpgsqlConnection connection, NpgsqlTransaction tx, string pgSchema, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            """
            select column_name, data_type, numeric_precision, numeric_scale
            from information_schema.columns
            where table_schema = @schema and table_name = @table
            """,
            connection, tx);
        command.Parameters.AddWithValue("schema", pgSchema);
        command.Parameters.AddWithValue("table", table);

        var columns = new Dictionary<string, PgColumnType>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            var dataType = NormalizeTypeName(reader.GetString(1));
            int? precision = reader.IsDBNull(2) ? null : reader.GetInt32(2);
            int? scale = reader.IsDBNull(3) ? null : reader.GetInt32(3);
            columns[name] = new PgColumnType(dataType, precision, scale);
        }

        return columns;
    }

    private static PgColumnType ExpectedColumnType(Field field) => field.DataType.TypeId switch
    {
        ArrowTypeId.Decimal128 => new PgColumnType("numeric", 38, 9),
        _ => new PgColumnType(NormalizeTypeName(PgTypeFor(field)), null, null),
    };

    /// <summary>Maps the handful of alternate spellings postgres's <c>information_schema.columns</c> (or a
    /// hand-authored target table declared with a pg_catalog shorthand) can surface to the single
    /// canonical name this comparison uses on both sides -- e.g. <c>timestamp with time zone</c> (what
    /// <c>information_schema</c> always reports for a <c>timestamptz</c> column) is normalized the same as
    /// the literal word <c>timestamptz</c>.</summary>
    private static string NormalizeTypeName(string raw) => raw.Trim().ToLowerInvariant() switch
    {
        "timestamp with time zone" or "timestamptz" => "timestamptz",
        "timestamp without time zone" => "timestamp",
        "character varying" or "varchar" => "varchar",
        "int4" => "integer",
        "int8" => "bigint",
        "float8" => "double precision",
        "bool" => "boolean",
        "decimal" => "numeric",
        var other => other,
    };

    private readonly record struct PgColumnType(string DataType, int? Precision, int? Scale)
    {
        public string Display => DataType == "numeric" && Precision is not null
            ? $"numeric({Precision},{Scale})"
            : DataType;

        public bool Equals(PgColumnType other) => DataType == other.DataType &&
            (DataType != "numeric" || (Precision == other.Precision && Scale == other.Scale));

        public override int GetHashCode() => HashCode.Combine(DataType, Precision, Scale);
    }
}
