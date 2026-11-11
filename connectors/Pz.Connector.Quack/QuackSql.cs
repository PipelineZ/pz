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
/// A quack-attached table accepts only bulk CREATE TABLE AS / INSERT from the wire protocol — no
/// row-level UPDATE/DELETE/MERGE — so merge pulls the target through the client into a temp table,
/// resolves conflicts there, and rewrites the whole remote table with one <c>create or replace
/// table</c>: same guarantee class as replace. That rewrite is the whole blast radius of
/// merge-by-replace: (a) primary keys, NOT NULL/DEFAULT constraints and indexes on the target do not
/// survive it — the replacement table has none of them; (b) the target's column order follows the
/// source batch's order, not whatever order it had before; (c) a column the source batch omits
/// becomes NULL on matched rows — a matched row is replaced wholesale, not column-patched, so the
/// pipeline's column set must stay stable across runs; (d) duplicate keys within one source batch
/// collapse to one connector-determined survivor before the rewrite runs; (e) whether the
/// <c>create or replace table</c> itself is atomic is the server's guarantee, not pz's — a failed
/// rewrite can leave the target missing or partial until the next run, which recomputes the same
/// result (the merge is idempotent). Cost grows with the target table's size, since the whole table
/// crosses the wire on every merge.</summary>
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

    internal static bool TryCopySql(string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
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

                // Two source rows sharing a key within one batch would otherwise both survive into
                // the union (neither is "the target row" so neither takes the not-exists branch) and
                // the final create-or-replace would then fail on a duplicate key from the caller's
                // perspective. qualify row_number() keeps exactly one row per key group before the
                // union runs — connector-determined, not first/last-wins — which is the sink
                // contract's Absorb behaviour that a real MERGE gives the siblings for free.
                var partitionBy = string.Join(", ", keys.Select(k => $"s.{DuckDbSqlText.QuoteIdentifier(k)}"));
                var dedupedSource = $"select s.* from {{{{source}}}} as s qualify row_number() over (partition by {partitionBy}) = 1";
                var scratch = "pz_quack_merge_" + DuckDbSqlText.StableSuffix(table);
                sql = create +
                    $"create or replace temp table {scratch} as {dedupedSource} union all by name select t.* from {table} as t where not exists (select 1 from {{{{source}}}} as s where {on});\n" +
                    $"create or replace table {table} as select * from {scratch};\n" +
                    $"drop table {scratch};";
                mechanism = "quack merge-by-replace";
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
