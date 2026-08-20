using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>T-SQL identifier/DDL helpers.</summary>
internal static class MsDdl
{
    /// <summary>The nullable soft-delete marker column, appended to the
    /// desired schema (create-table and drift-check paths alike) only when <c>on_delete: soft</c> --
    /// see <see cref="EnsureTargetAsync"/> and <see cref="SqlServerSinkWriteSession"/>.</summary>
    public const string SoftDeleteColumn = "_pz_deleted_at";

    /// <summary>Canonical DDL spelling for <see cref="SoftDeleteColumn"/>, matching <see
    /// cref="DdlType"/>'s timestamp precision convention.</summary>
    private const string SoftDeleteColumnType = "datetime2(6)";

    /// <summary>Bracket-quotes an identifier; only ']' needs doubling inside brackets.</summary>
    public static string Quote(string identifier) => $"[{identifier.Replace("]", "]]")}]";

    /// <summary>The entity name IS the object name. Splits
    /// <c>schema.table</c> on its single dot; an unqualified name takes <paramref name="defaultSchema"/>.
    /// Three or more parts are refused
    /// rather than quoted as one identifier -- <c>Quote("db.dbo")</c> is the single identifier literally
    /// named <c>db.dbo</c>, so a cross-database name would silently read nothing. Deliberately a twin of
    /// <c>PgDdl.SplitEntity</c> rather than a shared helper: connectors load into separate ALCs and share
    /// only the Abstractions ABI, which stays closed.</summary>
    public static (string Schema, string Table) SplitEntity(string entity, string defaultSchema = "dbo")
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

    internal static string DdlType(Field field) => field.DataType.TypeId switch
    {
        ArrowTypeId.Int32 => "int",
        ArrowTypeId.Int64 => "bigint",
        ArrowTypeId.Double => "float",
        ArrowTypeId.Decimal128 => "decimal(38,9)",
        ArrowTypeId.String => "nvarchar(max)",
        ArrowTypeId.Boolean => "bit",
        ArrowTypeId.Date32 => "date",
        ArrowTypeId.Timestamp => "datetime2(6)",
        _ => throw new PzConnectorException(
            $"column '{field.Name}': Arrow type {field.DataType} has no SQL Server DDL mapping", isTransient: false),
    };

    public static string BuildColumnListSql(Schema arrowSchema, IReadOnlyDictionary<string, string> types) =>
        string.Join(", ", arrowSchema.FieldsList.Select(f => $"{Quote(f.Name)} {types[f.Name]}"));

    public static string BuildCreateTableSql(
        string msSchema, string table, Schema arrowSchema, string mode, IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, string> types, string? onDelete = null)
    {
        var isMerge = string.Equals(mode, "merge", StringComparison.Ordinal);
        var keySet = new HashSet<string>(keys, StringComparer.Ordinal);
        var columns = arrowSchema.FieldsList.Select(f =>
            $"{Quote(f.Name)} {types[f.Name]}{(isMerge && keySet.Contains(f.Name) ? " not null" : "")}").ToList();
        if (string.Equals(onDelete, "soft", StringComparison.Ordinal))
        {
            columns.Add($"{Quote(SoftDeleteColumn)} {SoftDeleteColumnType}");
        }

        var pk = isMerge && keys.Count > 0
            ? $", constraint {Quote($"pk_{table}")} primary key clustered ({string.Join(", ", keys.Select(Quote))})"
            : "";
        return $"create table {Quote(msSchema)}.{Quote(table)} ({string.Join(", ", columns)}{pk})";
    }

    /// <summary>Appended (as an identity) to the merge staging #temp so each staged row carries its
    /// arrival ordinal — the tiebreaker that makes same-session duplicate keys resolve last-writer-wins
    /// (the TestKit merge contract, MergeRows.Absorb; postgres uses ctid the same way).
    /// A raw MERGE over duplicated staged keys would instead fail loudly (PK violation for two
    /// not-matched rows, error 8672 for two matched ones).</summary>
    public const string StagingSequenceColumn = "__pz_seq";

    public static string BuildMergeSql(
        string quotedTarget, string quotedStaging, Schema arrowSchema, IReadOnlyList<string> keys,
        string? onDelete = null)
    {
        var keySet = new HashSet<string>(keys, StringComparer.Ordinal);
        var all = arrowSchema.FieldsList.Select(f => f.Name).ToArray();
        var nonKeys = all.Where(c => !keySet.Contains(c)).ToArray();
        var columnList = string.Join(", ", all.Select(Quote));
        var partitionKeys = string.Join(", ", keys.Select(Quote));
        // Key-dedup the staging rows (last __pz_seq wins) BEFORE the merge: see StagingSequenceColumn.
        var dedupedSource =
            $"(select {columnList} from (select {columnList}, row_number() over " +
            $"(partition by {partitionKeys} order by {Quote(StagingSequenceColumn)} desc) as [__pz_rn] " +
            $"from {quotedStaging}) as d where [__pz_rn] = 1)";
        var on = string.Join(" and ", keys.Select(k => $"t.{Quote(k)} = s.{Quote(k)}"));

        var updateSets = nonKeys.Select(c => $"t.{Quote(c)} = s.{Quote(c)}").ToList();
        if (string.Equals(onDelete, "soft", StringComparison.Ordinal))
        {
            // A key re-upserted after being soft-deleted (this session or a prior one) is live again --
            // clear the marker.
            updateSets.Add($"t.{Quote(SoftDeleteColumn)} = null");
        }

        var update = updateSets.Count > 0 ? $"when matched then update set {string.Join(", ", updateSets)} " : "";
        var insertVals = string.Join(", ", all.Select(c => $"s.{Quote(c)}"));
        return $"merge {quotedTarget} with (holdlock) as t using {dedupedSource} as s on {on} " +
               $"{update}when not matched then insert ({columnList}) values ({insertVals});";
    }

    public static async Task<Dictionary<string, string>?> EnsureTargetAsync(
        SqlConnection connection, SqlTransaction tx, string schemaPolicy,
        string msSchema, string table, Schema arrowSchema, string outputName,
        string mode, IReadOnlyList<string> keys, string? onDelete, MsResolvedTypes resolved, CancellationToken ct)
    {
        if (string.Equals(schemaPolicy, "evolve", StringComparison.OrdinalIgnoreCase))
        {
            throw new PzConnectorException(
                $"output '{outputName}': schema_policy 'evolve' is not supported by the sqlserver sink -- " +
                "hint: use 'fail_on_change' (the default) and align the target schema by hand",
                isTransient: false);
        }

        var exists = await ScalarAsync<int?>(connection, tx,
            "select 1 from sys.tables t join sys.schemas s on t.schema_id = s.schema_id " +
            "where s.name = @schema and t.name = @table",
            ct, ("@schema", msSchema), ("@table", table)).ConfigureAwait(false) is not null;
        if (!exists)
        {
            await using var create = new SqlCommand(
                BuildCreateTableSql(msSchema, table, arrowSchema, mode, keys, resolved.Types, onDelete), connection, tx);
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return null;
        }

        // fail_on_change: exact-name + exact-canonical-type column check via sys.columns/sys.types
        // (precision/scale/max_length included, so decimal(38,9) vs decimal(18,2) is a mismatch) --
        // except an undeclared STRING column, which accepts any nvarchar/varchar width:
        // pz's own expected size for it is derived from data, not deterministic, so exactness there
        // would reject both old pz-created nvarchar(max) tables and hand-sized ones.
        var existing = await LoadExistingColumnsAsync(connection, tx, msSchema, table, ct).ConfigureAwait(false);
        var errors = new List<string>();
        foreach (var field in arrowSchema.FieldsList)
        {
            var expected = resolved.Types[field.Name];
            if (!existing.TryGetValue(field.Name, out var actual))
            {
                errors.Add($"target column '{field.Name}' is missing from {msSchema}.{table} (expected '{expected}')");
                continue;
            }

            var isUndeclaredString = !resolved.Declared.Contains(field.Name)
                && field.DataType.TypeId == ArrowTypeId.String;
            var ok = isUndeclaredString
                ? actual.StartsWith("nvarchar(", StringComparison.OrdinalIgnoreCase)
                    || actual.StartsWith("varchar(", StringComparison.OrdinalIgnoreCase)
                : string.Equals(actual, expected, StringComparison.OrdinalIgnoreCase);
            if (!ok)
            {
                errors.Add($"target column '{field.Name}' in {msSchema}.{table} has type '{actual}', expected '{expected}'");
            }
        }

        // on_delete: soft requires the nullable _pz_deleted_at marker column.
        // fail_on_change (default) reports a missing/mismatched column the same way as every other
        // declared column; schema_policy: additive instead ALTERs it in -- scoped to this one
        // soft-delete column, not general schema evolution (out of scope here).
        if (string.Equals(onDelete, "soft", StringComparison.Ordinal))
        {
            if (!existing.TryGetValue(SoftDeleteColumn, out var actualSoft))
            {
                if (string.Equals(schemaPolicy, "additive", StringComparison.OrdinalIgnoreCase))
                {
                    await using var alter = new SqlCommand(
                        $"alter table {Quote(msSchema)}.{Quote(table)} add {Quote(SoftDeleteColumn)} {SoftDeleteColumnType}",
                        connection, tx);
                    await alter.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                else
                {
                    errors.Add(
                        $"target column '{SoftDeleteColumn}' is missing from {msSchema}.{table} " +
                        $"(expected '{SoftDeleteColumnType}')");
                }
            }
            else if (!string.Equals(actualSoft, SoftDeleteColumnType, StringComparison.OrdinalIgnoreCase))
            {
                errors.Add(
                    $"target column '{SoftDeleteColumn}' in {msSchema}.{table} has type '{actualSoft}', " +
                    $"expected '{SoftDeleteColumnType}'");
            }
        }

        if (errors.Count > 0)
        {
            throw new PzConnectorException(
                $"output '{outputName}': {string.Join("; ", errors)} -- hint: schema_policy is " +
                "'fail_on_change' (the default); align the target table by hand, or drop it and let the " +
                "sink recreate it",
                isTransient: false);
        }

        if (string.Equals(mode, "merge", StringComparison.Ordinal) && keys.Count > 0)
        {
            await EnsureUniqueIndexOnKeysAsync(connection, tx, msSchema, table, keys, outputName, ct).ConfigureAwait(false);
        }

        return existing;
    }

    /// <summary>Reads existing columns as canonical DDL spellings: "decimal(38,9)", "datetime2(6)",
    /// "nvarchar(max)"/"varchar(200)"/"char(10)"/"nchar(10)" (max_length -1 => "max"), plain names
    /// otherwise — directly comparable to the effective types resolved by <see
    /// cref="MsEffectiveTypes.Resolve"/> (or, for the relaxed undeclared-string comparison in <see
    /// cref="EnsureTargetAsync"/>, matched by prefix instead).</summary>
    private static async Task<Dictionary<string, string>> LoadExistingColumnsAsync(
        SqlConnection connection, SqlTransaction tx, string msSchema, string table, CancellationToken ct)
    {
        const string sql =
            "select c.name, tp.name as type_name, c.max_length, c.precision, c.scale " +
            "from sys.columns c " +
            "join sys.types tp on c.user_type_id = tp.user_type_id " +
            "join sys.tables t on c.object_id = t.object_id " +
            "join sys.schemas s on t.schema_id = s.schema_id " +
            "where s.name = @schema and t.name = @table";
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = new SqlCommand(sql, connection, tx);
        command.Parameters.AddWithValue("@schema", msSchema);
        command.Parameters.AddWithValue("@table", table);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            var type = reader.GetString(1);
            var display = type switch
            {
                "decimal" or "numeric" => $"decimal({reader.GetByte(3)},{reader.GetByte(4)})",
                "datetime2" => $"datetime2({reader.GetByte(4)})",
                "nvarchar" => reader.GetInt16(2) == -1 ? "nvarchar(max)" : $"nvarchar({reader.GetInt16(2) / 2})",
                "varchar" => reader.GetInt16(2) == -1 ? "varchar(max)" : $"varchar({reader.GetInt16(2)})",
                "char" => $"char({reader.GetInt16(2)})",
                "nchar" => $"nchar({reader.GetInt16(2) / 2})",
                _ => type,
            };
            result[name] = display;
        }

        return result;
    }

    /// <summary>Merge determinism guard: at least one unique index / unique constraint /
    /// PK whose key column set equals EXACTLY the merge keys (unfiltered, no included-key mismatch).</summary>
    private static async Task EnsureUniqueIndexOnKeysAsync(
        SqlConnection connection, SqlTransaction tx, string msSchema, string table,
        IReadOnlyList<string> keys, string outputName, CancellationToken ct)
    {
        const string sql =
            "select i.index_id, c.name " +
            "from sys.indexes i " +
            "join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id and ic.is_included_column = 0 " +
            "join sys.columns c on ic.object_id = c.object_id and ic.column_id = c.column_id " +
            "join sys.tables t on i.object_id = t.object_id " +
            "join sys.schemas s on t.schema_id = s.schema_id " +
            "where s.name = @schema and t.name = @table and i.is_unique = 1 and i.has_filter = 0";
        var byIndex = new Dictionary<int, HashSet<string>>();
        await using (var command = new SqlCommand(sql, connection, tx))
        {
            command.Parameters.AddWithValue("@schema", msSchema);
            command.Parameters.AddWithValue("@table", table);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var id = reader.GetInt32(0);
                if (!byIndex.TryGetValue(id, out var set))
                {
                    byIndex[id] = set = new HashSet<string>(StringComparer.Ordinal);
                }

                set.Add(reader.GetString(1));
            }
        }

        var wanted = new HashSet<string>(keys, StringComparer.Ordinal);
        if (!byIndex.Values.Any(wanted.SetEquals))
        {
            throw new PzConnectorException(
                $"output '{outputName}': merge requires a unique index or primary key on exactly " +
                $"[{string.Join(", ", keys)}] on {msSchema}.{table} -- hint: create one (e.g. " +
                $"'create unique index ux_{table}_keys on {msSchema}.{table} ({string.Join(", ", keys)})') " +
                "or drop the table and let the sink recreate it",
                isTransient: false);
        }
    }

    private static async Task<T?> ScalarAsync<T>(
        SqlConnection connection, SqlTransaction tx, string sql, CancellationToken ct,
        params (string Name, object Value)[] parameters)
    {
        await using var command = new SqlCommand(sql, connection, tx);
        foreach (var (name, value) in parameters)
        {
            command.Parameters.AddWithValue(name, value);
        }

        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is null or DBNull ? default : (T)result;
    }
}
