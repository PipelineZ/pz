using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Shared.Quack;
using Pz.Shared.Sql;

namespace Pz.Connector.Quack;

/// <summary>All SQL/text generation for the native-only remote-DuckDB connector. Pure string
/// building; DuckDB parses every statement (double-quoted identifiers, ''-escaped literals). ONE
/// read-write alias per connection shared by reads and writes. The token NEVER rides the attach
/// string (a failed ATTACH echoes its path verbatim into the engine error); it rides a quack secret
/// scoped to the server URI.
///
/// Merge runs on the server: the staged batch crosses the wire once, into a scratch table beside the
/// target, and one <c>MERGE INTO</c> applies it there. Quack executes UPDATE/MERGE only when every
/// table they read is on the server (a statement reading the client's staging is refused with
/// "PlanUpdate not implemented"), which is why the batch is shipped first. The target keeps its
/// constraints, indexes and column order.</summary>
internal static class QuackSql
{
    /// <summary>Sanitized connection name plus the first 8 lowercase hex chars of SHA-256 of the RAW
    /// name, so "prod-db" and "prod_db" (both sanitize identically) never share an alias — without
    /// the hash suffix, a second connection could silently reuse the first's attach, since
    /// <c>attach if not exists</c> is first-wins.</summary>
    internal static string Alias(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_quack", connectionName);

    internal static string SecretName(string alias) => alias + "_secret";

    /// <summary>Setup for either direction: extension, the scoped token secret, one attach. All
    /// idempotent should a node retry re-issue them; the engine runs each once per run.
    ///
    /// The uri is canonicalized to <c>quack:host:port</c> before it lands in either the secret's
    /// scope or the attach string — <c>quack:host</c>, <c>quack:host:port</c> and
    /// <c>quack://host[:port]</c> are all accepted, but the secret's scope and the attach must name
    /// the server identically or the scoped secret silently fails to match.</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string alias)
    {
        var uri = Require(config, "uri");
        var token = Require(config, "token");
        if (!QuackUri.TryParse(uri, out var host, out var port))
        {
            throw new PzConnectorException("quack connection 'uri' must be of the form quack:host[:port]", isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        var canonical = $"quack:{host}:{port}";
        return
        [
            "install quack",
            "load quack",
            $"create or replace secret {SecretName(alias)} (type quack, token '{DuckDbSqlText.EscapeLiteral(token)}', scope '{DuckDbSqlText.EscapeLiteral(canonical)}')",
            $"attach if not exists '{DuckDbSqlText.EscapeLiteral(canonical)}' as {alias}",
        ];
    }

    /// <summary>An entity is <c>table</c> (the server's default schema) or <c>schema.table</c>:
    /// exactly one split on the first dot; more dots, or an empty side, is a permanent error.</summary>
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
            $"entity '{entity}': expected 'table' or 'schema.table'", isTransient: false, code: PzConnectorErrorCodes.Config);
    }

    internal static string QualifiedTable(string alias, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{alias}.{DuckDbSqlText.QuoteIdentifier(table)}"
            : $"{alias}.{DuckDbSqlText.QuoteIdentifier(schema)}.{DuckDbSqlText.QuoteIdentifier(table)}";
    }

    /// <summary>Contract prunes the projection; the plain incremental watermark is pushed down (the
    /// server evaluates the predicate); the window ceiling is MUST-apply. The engine embeds the
    /// fragment after FROM, which is why a bare table is returned unwrapped — anything with a
    /// projection or predicate becomes a parenthesized subquery instead.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec) =>
        DuckDbSqlText.ScanFragment(QualifiedTable(alias, spec.Dataset), spec);

    /// <summary>The copy statement(s) for one output; <paramref name="alias"/> is the connection's attach
    /// alias, which also holds a merge's scratch table.</summary>
    internal static bool TryCopySql(string alias, string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        var create = $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n";
        switch (mode)
        {
            case "append":
                sql = create + $"insert into {table} by name select * from {{{{source}}}};";
                mechanism = "quack insert";
                return true;
            case "replace":
                sql = $"create or replace table {table} as select * from {{{{source}}}}";
                mechanism = "quack create-or-replace";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException($"output '{table}': merge requires at least one key column", isTransient: false, code: PzConnectorErrorCodes.Config);
                }

                var on = string.Join(" and ", keys.Select(k => $"t.{DuckDbSqlText.QuoteIdentifier(k)} = s.{DuckDbSqlText.QuoteIdentifier(k)}"));

                // DuckDB's MERGE matches every source row independently against the target as it stood
                // before the statement ran: two staged rows sharing a key the target lacks would BOTH take
                // the not-matched branch and both insert, and duplicates of a held key would update it
                // in an undefined order. The sink contract says duplicate keys within one batch collapse
                // to a single connector-determined survivor, so the batch is made key-unique (qualify
                // row_number() per key group) on its way into the scratch table.
                //
                // One transaction around the scratch table, the merge and the drop: a failed merge (a
                // target constraint the batch violates) rolls back the scratch table with it, so nothing
                // is left on the server. The scratch name is stable per target, so concurrent merges into
                // one target from two runs would share it; one writer per target is assumed, as for every
                // sink.
                var partitionBy = string.Join(", ", keys.Select(k => $"s.{DuckDbSqlText.QuoteIdentifier(k)}"));
                var scratch = $"{alias}.{DuckDbSqlText.QuoteIdentifier("pz_merge_" + DuckDbSqlText.StableSuffix(table))}";
                sql = create +
                    "begin transaction;\n" +
                    $"create or replace table {scratch} as select s.* from {{{{source}}}} as s qualify row_number() over (partition by {partitionBy}) = 1;\n" +
                    $"merge into {table} as t using {scratch} as s on {on} when matched then update when not matched then insert;\n" +
                    $"drop table {scratch};\n" +
                    "commit;";
                mechanism = "quack merge";
                return true;
            default:
                sql = "";
                mechanism = "";
                return false;
        }
    }

    private static string Require(ConnectorConfig config, string key) =>
        config.GetString(key) is { Length: > 0 } s ? s : throw new PzConnectorException($"quack connection requires '{key}'", isTransient: false, code: PzConnectorErrorCodes.Config);

}
