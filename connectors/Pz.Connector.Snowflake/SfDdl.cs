using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake identifier quoting and generated-SQL builders. Deliberately duplicated
/// per-connector (see MsDdl's note): connectors share no helper assembly.</summary>
internal static class SfDdl
{
    /// <summary>Reserved by the sink's merge staging for last-writer-wins ordering.</summary>
    public const string StagingSequenceColumn = "_pz_seq";

    public static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    public static (string Schema, string Table) SplitEntity(string entity, string defaultSchema = "PUBLIC")
    {
        var dot = entity.IndexOf('.');
        var (schema, table) = dot < 0 ? (defaultSchema, entity) : (entity[..dot], entity[(dot + 1)..]);
        if (schema.Length == 0 || table.Length == 0)
        {
            throw new PzConnectorException(
                $"entity '{entity}' is not a valid snowflake object name -- expected 'SCHEMA.TABLE' or 'TABLE'",
                isTransient: false);
        }

        return (schema, table);
    }

    public static string BuildCreateTableSql(string schema, string table, Schema arrowSchema)
    {
        var columns = arrowSchema.FieldsList
            .Select(f => $"{Quote(f.Name)} {SfTypeMap.ToSnowflakeDdl(f.DataType)}");
        return $"create table if not exists {Quote(schema)}.{Quote(table)} ({string.Join(", ", columns)})";
    }

    public static string BuildInsertSql(string schema, string table, string tempTable, Schema arrowSchema) =>
        BuildInsertCore("insert into", schema, table, tempTable, arrowSchema);

    public static string BuildInsertOverwriteSql(string schema, string table, string tempTable, Schema arrowSchema) =>
        BuildInsertCore("insert overwrite into", schema, table, tempTable, arrowSchema);

    private static string BuildInsertCore(string verb, string schema, string table, string tempTable, Schema arrowSchema)
    {
        var columns = string.Join(", ", arrowSchema.FieldsList.Select(f => Quote(f.Name)));
        return $"{verb} {Quote(schema)}.{Quote(table)} ({columns}) select {columns} from {tempTable}";
    }

    public static string BuildMergeSql(string schema, string table, string tempTable,
        Schema arrowSchema, IReadOnlyList<string> keys)
    {
        var all = arrowSchema.FieldsList.Select(f => f.Name).ToArray();
        var nonKeys = all.Where(c => !keys.Contains(c, StringComparer.Ordinal)).ToArray();
        var keyList = string.Join(", ", keys.Select(Quote));
        var onClause = string.Join(" and ", keys.Select(k => $"t.{Quote(k)} = s.{Quote(k)}"));
        var insertCols = string.Join(", ", all.Select(Quote));
        var insertVals = string.Join(", ", all.Select(c => $"s.{Quote(c)}"));

        var sql = new StringBuilder()
            .Append($"merge into {Quote(schema)}.{Quote(table)} as t using (")
            .Append($"select {insertCols} from {tempTable} ")
            .Append($"qualify row_number() over (partition by {keyList} order by {Quote(StagingSequenceColumn)} desc) = 1")
            .Append($") as s on {onClause}");
        if (nonKeys.Length > 0)
        {
            sql.Append(" when matched then update set ")
               .Append(string.Join(", ", nonKeys.Select(c => $"{Quote(c)} = s.{Quote(c)}")));
        }

        sql.Append($" when not matched then insert ({insertCols}) values ({insertVals})");
        return sql.ToString();
    }

    /// <summary>The sink's session-scoped temp stage. <paramref name="qualifiedStage"/> is already
    /// schema-qualified (<c>"schema"."name"</c>) -- the connection sets a default database but no
    /// default schema, so an unqualified temp object has nothing to resolve against.</summary>
    public static string BuildCreateStageSql(string qualifiedStage) => $"create temporary stage {qualifiedStage}";

    /// <summary>One spool file's upload. The URI is single-quoted with forward slashes: a Windows
    /// temp path can contain spaces and backslashes, neither of which survives an unquoted
    /// <c>file://</c> token. The file is already gzip-compressed by the sink's CSV writer, so
    /// <c>auto_compress</c> is off (uploading it again would double-compress) and
    /// <c>source_compression</c> tells the server what it already has.</summary>
    public static string BuildPutSql(string filePath, string qualifiedStage) =>
        $"put 'file://{filePath.Replace('\\', '/')}' @{qualifiedStage} auto_compress = false " +
        "source_compression = gzip";

    /// <summary>The sink's temp staging table, schema-qualified like <see cref="BuildCreateStageSql"/>.
    /// Merge alone carries <see cref="StagingSequenceColumn"/> as a real bigint -- populated by
    /// <c>SfCsv.WriteBatch</c>'s session-monotonic counter via COPY, never an autoincrement, because
    /// Snowflake's COPY can load a stage's files in parallel and an autoincrement's fill order would
    /// not reliably track write order across files.</summary>
    public static string BuildCreateStagingTableSql(string qualifiedStaging, Schema arrowSchema, bool includeSequenceColumn)
    {
        var loadColumns = string.Join(", ",
            arrowSchema.FieldsList.Select(f => $"{Quote(f.Name)} {SfTypeMap.ToSnowflakeDdl(f.DataType)}"));
        var seqColumn = includeSequenceColumn ? $", {Quote(StagingSequenceColumn)} bigint" : "";
        return $"create temporary table {qualifiedStaging} ({loadColumns}{seqColumn})";
    }

    /// <summary>Loads the spooled CSV into the staging table via COPY's TRANSFORMATION form --
    /// required because a target column list (needed so an autoincrement-free sequence column and the
    /// data columns land in the right places) is not valid on the standard <c>from @stage</c> form,
    /// only on <c>from (select $1, $2, ... from @stage)</c>. The <c>$</c>-positions are the CSV's own
    /// column order: the data columns, then (merge only) the trailing sequence column
    /// <c>SfCsv.WriteBatch</c> wrote.</summary>
    public static string BuildCopyIntoStagingSql(
        string qualifiedStaging, string qualifiedStage, Schema arrowSchema, bool includeSequenceColumn)
    {
        var csvColumnCount = arrowSchema.FieldsList.Count + (includeSequenceColumn ? 1 : 0);
        var positions = string.Join(", ", Enumerable.Range(1, csvColumnCount).Select(i => $"${i}"));
        var copyColumnNames = arrowSchema.FieldsList.Select(f => Quote(f.Name));
        if (includeSequenceColumn)
        {
            copyColumnNames = copyColumnNames.Append(Quote(StagingSequenceColumn));
        }

        var copyColumns = string.Join(", ", copyColumnNames);
        return $"copy into {qualifiedStaging} ({copyColumns}) from (select {positions} from @{qualifiedStage}) " +
            $"{SfCsv.FileFormatClause} on_error = abort_statement";
    }

    /// <summary>Ensures the target exists before the sink's one commit statement ever touches it.
    /// <c>evolve</c> is rejected outright -- this sink has no ALTER/drift-repair machinery, so
    /// pretending to support it would silently skip real schema evolution. Every other policy (the
    /// default <c>fail_on_change</c> included) probes <c>information_schema.columns</c>,
    /// database-qualified because the sink's connection string sets no default schema (only a
    /// default database, via <c>db=</c>) -- an unqualified <c>information_schema.columns</c> query
    /// would depend on session state this sink never establishes. Missing -> create it. Present ->
    /// compare every declared column's name and canonical type against what Snowflake actually
    /// reports, aggregating every mismatch into one error (never failing on the first), mirroring
    /// <c>MsDdl.EnsureTargetAsync</c>'s shape.</summary>
    public static async Task EnsureTargetAsync(
        SnowflakeDbConnection connection, string schemaPolicy, string schema, string table,
        Schema arrowSchema, string outputName, CancellationToken ct)
    {
        if (string.Equals(schemaPolicy, "evolve", StringComparison.OrdinalIgnoreCase))
        {
            throw new PzConnectorException(
                $"output '{outputName}': schema_policy 'evolve' is not supported by the snowflake sink -- " +
                "hint: use 'fail_on_change' (the default) and align the target schema by hand",
                isTransient: false);
        }

        var database = await ScalarStringAsync(connection, "select current_database()", ct).ConfigureAwait(false);
        var existing = await LoadExistingColumnsAsync(connection, database, schema, table, ct).ConfigureAwait(false);
        if (existing.Count == 0)
        {
            await using var create = connection.CreateCommand();
            create.CommandText = BuildCreateTableSql(schema, table, arrowSchema);
            await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            return;
        }

        var errors = new List<string>();
        foreach (var field in arrowSchema.FieldsList)
        {
            var expected = SfTypeMap.ToInformationSchemaDisplay(field.DataType);
            if (!existing.TryGetValue(field.Name, out var actual))
            {
                errors.Add($"target column '{field.Name}' is missing from {schema}.{table} (expected '{expected}')");
                continue;
            }

            if (!string.Equals(actual, expected, StringComparison.Ordinal))
            {
                errors.Add($"target column '{field.Name}' in {schema}.{table} has type '{actual}', expected '{expected}'");
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
    }

    /// <summary>Reads existing columns as CANONICAL <c>information_schema</c> spellings -- directly
    /// comparable to <see cref="SfTypeMap.ToInformationSchemaDisplay"/>'s output. <c>table_schema</c>
    /// and <c>table_name</c> are compared byte-for-byte against pz's own identifiers (never
    /// uppercased): every object pz creates is double-quoted (<see cref="Quote"/>), and Snowflake
    /// stores a quoted identifier's case exactly as given -- unlike an unquoted one, which it would
    /// fold to uppercase.</summary>
    private static async Task<Dictionary<string, string>> LoadExistingColumnsAsync(
        SnowflakeDbConnection connection, string database, string schema, string table, CancellationToken ct)
    {
        var sql =
            "select column_name, data_type, numeric_precision, numeric_scale, datetime_precision " +
            $"from {Quote(database)}.information_schema.columns " +
            $"where table_schema = {Literal(schema)} and table_name = {Literal(table)}";
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            var dataType = reader.GetString(1);
            long? numericPrecision = reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2));
            long? numericScale = reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3));
            long? datetimePrecision = reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4));
            result[name] = dataType.ToUpperInvariant() switch
            {
                "NUMBER" => $"NUMBER({numericPrecision},{numericScale})",
                "FLOAT" => "FLOAT",
                "TEXT" => "TEXT",
                "BOOLEAN" => "BOOLEAN",
                "DATE" => "DATE",
                "TIMESTAMP_NTZ" => $"TIMESTAMP_NTZ({datetimePrecision})",
                var other => other,
            };
        }

        return result;
    }

    private static async Task<string> ScalarStringAsync(SnowflakeDbConnection connection, string sql, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return (string)result!;
    }

    /// <summary>SQL string literal, single-quote-doubled -- the injection-safety pattern for a VALUE
    /// (as opposed to an identifier, which goes through <see cref="Quote"/>) interpolated into
    /// generated SQL; mirrors <c>SnowflakeSource.BuildSelect</c>'s watermark-literal escaping.</summary>
    private static string Literal(string value) => $"'{value.Replace("'", "''")}'";
}
