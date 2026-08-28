using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

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
}
