using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Sql;

namespace Pz.Connector.DuckDb;

/// <summary>All SQL/text generation for the native-only DuckDB-file connector. Everything here is
/// pure string building — the connector performs no I/O on the data plane at all. One dialect:
/// every statement (attach, scan fragment, copy) is parsed by DuckDB itself, so identifiers are
/// double-quoted (doubled to escape) and string literals double single quotes.
///
/// ONE alias per connection, shared by reads and writes: a DuckDB database file cannot be attached
/// twice in one session (a second attach of the same file under another alias is a unique-file-handle
/// conflict), so the read-only/read-write alias split other database connectors use is not available
/// here.</summary>
internal static class DuckDbSql
{
    /// <summary>The connection's attach alias. Must stay distinct per raw connection name:
    /// <c>attach if not exists</c> is first-wins, so two connections sharing an alias would silently
    /// read and write the wrong file (see <see cref="DuckDbSqlText.ConnectionAlias"/>).</summary>
    internal static string Alias(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_duckdb", connectionName);

    /// <summary>Resolves the connection's <c>path:</c> to an absolute path: relative paths join the
    /// CLI-injected <c>base_dir</c> (never user-written), falling back to the process working
    /// directory for hosts that don't inject it. Statements always embed the absolute form — the
    /// engine's DuckDB session must not depend on process cwd.</summary>
    internal static string ResolvePath(ConnectorConfig config)
    {
        var path = config.GetString("path") ??
            throw new PzConnectorException("duckdb connection requires 'path'", isTransient: false, code: PzConnectorErrorCodes.Config);
        if (Path.IsPathRooted(path))
        {
            return Path.GetFullPath(path);
        }

        var baseDir = config.GetString("base_dir") ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(baseDir, path));
    }

    /// <summary>An entity is <c>table</c> (DuckDB's default schema) or <c>schema.table</c>: exactly
    /// one split on the first dot; more dots, or an empty side, is a permanent error — there is no
    /// three-part name inside one attached catalog.</summary>
    internal static (string? Schema, string Table) SplitEntity(string entity)
    {
        var dot = entity.IndexOf('.', StringComparison.Ordinal);
        if (dot < 0)
        {
            return entity.Length > 0 ? (null, entity) : throw Malformed(entity);
        }

        var schema = entity[..dot];
        var table = entity[(dot + 1)..];
        if (schema.Length == 0 || table.Length == 0 || table.Contains('.'))
        {
            throw Malformed(entity);
        }

        return (schema, table);

        static PzConnectorException Malformed(string entity) => new(
            $"entity '{entity}': expected 'table' or 'schema.table'", isTransient: false,
            code: PzConnectorErrorCodes.Config);
    }

    internal static string QualifiedTable(string alias, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{alias}.{DuckDbSqlText.QuoteIdentifier(table)}"
            : $"{alias}.{DuckDbSqlText.QuoteIdentifier(schema)}.{DuckDbSqlText.QuoteIdentifier(table)}";
    }

    /// <summary>Setup for either direction: one read-write attach of the database file, idempotent
    /// should a node retry re-issue it — the engine runs each setup statement once per run
    /// (<c>attach if not exists</c> skips an existing alias).
    /// No extension is needed and the path carries no credentials, so a connect-failure echo of it is
    /// harmless and useful.</summary>
    internal static IReadOnlyList<string> SetupStatements(string resolvedPath, string alias) =>
        [$"attach if not exists '{DuckDbSqlText.EscapeLiteral(resolvedPath)}' as {alias}"];

    /// <summary>The scan fragment: a declared <c>columns:</c> contract prunes the read (only declared
    /// columns are projected); the plain (unwindowed) incremental watermark IS pushed into the
    /// fragment (DuckDB pushes the filter into the attached catalog's scan); the window ceiling is
    /// MUST-apply. The engine embeds the fragment after FROM, so a plain scan stays the bare qualified
    /// table and anything with a projection or predicate becomes a parenthesized subquery.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec) =>
        DuckDbSqlText.ScanFragment(QualifiedTable(alias, spec.Dataset), spec);

    /// <summary>The copy statement(s) for one output. <c>{{source}}</c> is the engine's placeholder
    /// for the staged relation (substituted by SinkWriteExecutor). Append and merge first create the
    /// target from the staged shape so a first run needs no pre-created table. Append inserts BY NAME:
    /// the target may predate pz or order its columns differently, and a positional insert between
    /// type-compatible columns succeeds with every value in the wrong place. Merge then matches on
    /// every declared key, updating all columns by name on a match and inserting otherwise. A
    /// keyless merge is refused at compile time; the throw here is ABI defense-in-depth.</summary>
    internal static bool TryCopySql(string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        const string create = "create table if not exists {0} as select * from {{{{source}}}} limit 0;\n";
        switch (mode)
        {
            case "append":
                sql = string.Format(null, create, table) +
                    $"insert into {table} by name select * from {{{{source}}}};";
                mechanism = "duckdb insert";
                return true;
            case "replace":
                sql = $"create or replace table {table} as select * from {{{{source}}}}";
                mechanism = "duckdb create-or-replace";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException(
                        $"output '{table}': merge requires at least one key column", isTransient: false, code: PzConnectorErrorCodes.Config);
                }

                var on = string.Join(" and ", keys.Select(k => $"t.{DuckDbSqlText.QuoteIdentifier(k)} = s.{DuckDbSqlText.QuoteIdentifier(k)}"));

                // DuckDB's MERGE matches every source row independently against the target as it stood
                // before the statement ran: two staged rows sharing a key the target lacks would BOTH take
                // the not-matched branch and both insert, and duplicates of a held key would update it
                // in an undefined order. The sink contract says duplicate keys within one batch collapse
                // to a single connector-determined survivor, so the staged side is made key-unique
                // (qualify row_number() per key group) before MERGE ever sees it.
                var partitionBy = string.Join(", ", keys.Select(k => $"s.{DuckDbSqlText.QuoteIdentifier(k)}"));
                var dedupedSource = $"(select s.* from {{{{source}}}} as s qualify row_number() over (partition by {partitionBy}) = 1)";
                sql = string.Format(null, create, table) +
                    $"merge into {table} as t using {dedupedSource} as s on {on} " +
                    "when matched then update when not matched then insert;";
                mechanism = "duckdb merge";
                return true;
            default:
                sql = "";
                mechanism = "";
                return false;
        }
    }

}
