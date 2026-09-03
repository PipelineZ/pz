using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Quack;

/// <summary>All SQL/text generation for the native-only remote-DuckDB connector. Pure string
/// building; DuckDB parses every statement (double-quoted identifiers, ''-escaped literals). ONE
/// read-write alias per connection shared by reads and writes. The token NEVER rides the attach
/// string (a failed ATTACH echoes its path verbatim into the engine error); it rides a quack secret
/// scoped to the server URI. A quack-attached table accepts only bulk CREATE TABLE AS / INSERT from
/// the wire protocol — no row-level UPDATE/DELETE/MERGE — so merge pulls the target through the
/// client into a temp table, resolves conflicts there, and rewrites the whole remote table: one
/// remote mutation, same guarantee class as replace. A matched row is replaced wholesale by the
/// source row, not column-patched — a column the source batch omits becomes NULL on matched rows,
/// so the pipeline's column set must stay stable across runs; cost grows with the target table's
/// size, since the whole table crosses the wire on every merge. Kept in lockstep with the
/// duckdb/ducklake/motherduck connectors' Sql classes for append/replace (replicated, never
/// referenced); merge necessarily diverges for the reason above.</summary>
internal static class QuackSql
{
    internal static string Alias(string connectionName) =>
        $"pz_quack_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    internal static string SecretName(string alias) => alias + "_secret";

    /// <summary>Setup for either direction: extension, the scoped token secret, one attach. All
    /// idempotent under NativeSetup's per-node repetition.
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
            throw new PzConnectorException("quack connection 'uri' must be of the form quack:host[:port]", isTransient: false);
        }

        var canonical = $"quack:{host}:{port}";
        return
        [
            "install quack",
            "load quack",
            $"create or replace secret {SecretName(alias)} (type quack, token '{EscapeLiteral(token)}', scope '{EscapeLiteral(canonical)}')",
            $"attach if not exists '{EscapeLiteral(canonical)}' as {alias}",
        ];
    }

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
            $"entity '{entity}': expected 'table' or 'schema.table'", isTransient: false);
    }

    internal static string QualifiedTable(string alias, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{alias}.{QuoteIdent(table)}"
            : $"{alias}.{QuoteIdent(schema)}.{QuoteIdent(table)}";
    }

    /// <summary>Contract prunes the projection; the plain incremental watermark is pushed down (the
    /// server evaluates the predicate); the window ceiling is MUST-apply.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec)
    {
        var table = QualifiedTable(alias, spec.Dataset);
        var columns = ExtractColumns(spec);
        var projection = columns is { Count: > 0 } ? string.Join(", ", columns.Keys.Select(QuoteIdent)) : "*";

        var predicates = new List<string>();
        if (spec is { WatermarkCursor: { } cursor, WatermarkValue: { } low })
        {
            predicates.Add($"{QuoteIdent(cursor)} {(spec.WatermarkLowerInclusive ? ">=" : ">")} {RenderWatermarkLiteral(low)}");
        }

        if (spec is { WatermarkCursor: { } upperCursor, WatermarkUpperBound: { } high })
        {
            predicates.Add($"{QuoteIdent(upperCursor)} <= {RenderWatermarkLiteral(high)}");
        }

        if (projection == "*" && predicates.Count == 0)
        {
            return table;
        }

        var where = predicates.Count > 0 ? $" where {string.Join(" and ", predicates)}" : "";
        return $"(select {projection} from {table}{where})";
    }

    internal static bool TryCopySql(string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        var create = $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n";
        switch (mode)
        {
            case "append":
                sql = create + $"insert into {table} select * from {{{{source}}}};";
                mechanism = "quack insert";
                return true;
            case "replace":
                sql = $"create or replace table {table} as select * from {{{{source}}}}";
                mechanism = "quack create-or-replace";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException($"output '{table}': merge requires at least one key column", isTransient: false);
                }

                var on = string.Join(" and ", keys.Select(k => $"t.{QuoteIdent(k)} = s.{QuoteIdent(k)}"));
                var scratch = "pz_quack_merge_" + HashSuffix(table);
                sql = create +
                    $"create or replace temp table {scratch} as select s.* from {{{{source}}}} as s union all by name select t.* from {table} as t where not exists (select 1 from {{{{source}}}} as s where {on});\n" +
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

    internal static string RenderWatermarkLiteral(string canonical)
    {
        if (IsCanonicalNumeric(canonical))
        {
            return canonical;
        }

        var value = IsCanonicalTimestamp(canonical) ? canonical[..10] + ' ' + canonical[11..] : canonical;
        return $"'{EscapeLiteral(value)}'";
    }

    private static bool IsCanonicalNumeric(string value)
    {
        var i = value.StartsWith('-') ? 1 : 0;
        var sawDigit = false;
        var sawDot = false;
        for (; i < value.Length; i++)
        {
            if (value[i] == '.')
            {
                if (sawDot)
                {
                    return false;
                }

                sawDot = true;
            }
            else if (char.IsAsciiDigit(value[i]))
            {
                sawDigit = true;
            }
            else
            {
                return false;
            }
        }

        return sawDigit;
    }

    private static bool IsCanonicalTimestamp(string value) =>
        value.Length >= 19 && value[10] == 'T' && value[4] == '-' && value[7] == '-' && value[13] == ':' && value[16] == ':';

    internal static string QuoteIdent(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    internal static string EscapeLiteral(string value) => value.Replace("'", "''");

    internal static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) && value is IReadOnlyDictionary<string, string> { Count: > 0 } columns ? columns : null;

    private static string Require(ConnectorConfig config, string key) =>
        config.GetString(key) is { Length: > 0 } s ? s : throw new PzConnectorException($"quack connection requires '{key}'", isTransient: false);

    private static string Sanitize(string name) => new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    private static string HashSuffix(string name) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
