using Pz.Connectors.Abstractions;

namespace Pz.Connector.MotherDuck;

/// <summary>Native-only MotherDuck connector text generation: DuckDB's motherduck extension is the
/// entire data plane, so this class IS the connector. Pure string building; DuckDB parses every
/// statement (double-quoted identifiers, ''-escaped literals). MotherDuck refuses an alias on a
/// database the user owns, so there is no read-only/read-write alias split — the attach name is the
/// database name itself, quoted for every reference. The session token rides a SET statement (the
/// engine describes it without echoing) and never the attach string, since a failed <c>attach</c>
/// echoes its literal verbatim into the engine error. Kept in lockstep with the duckdb/ducklake/quack
/// connectors' Sql classes (replicated, never referenced).</summary>
internal static class MotherDuckSql
{
    /// <summary>MotherDuck refuses an alias on a database the user owns, so the attach name IS the
    /// database name, quoted for every reference.</summary>
    internal static string Database(ConnectorConfig config) => QuoteIdent(Require(config, "database"));

    /// <summary>Setup for either direction: extension, the session token (a SET statement the engine
    /// describes without echoing — never the <c>md:?motherduck_token=</c> URL form, which a failed
    /// attach would echo verbatim), and one alias-less attach. The SET is accepted only before the
    /// extension's first attach, so it relies on the engine running each distinct setup statement once
    /// per run; two connections with different tokens cannot share a run.</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config)
    {
        var database = Require(config, "database");
        var token = Require(config, "token");
        return
        [
            "install motherduck",
            "load motherduck",
            $"set motherduck_token = '{EscapeLiteral(token)}'",
            $"attach if not exists 'md:{EscapeLiteral(database)}'",
        ];
    }

    /// <summary>An entity is <c>table</c> (the database's default schema) or <c>schema.table</c>:
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
            $"entity '{entity}': expected 'table' or 'schema.table'", isTransient: false);
    }

    internal static string QualifiedTable(string database, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{database}.{QuoteIdent(table)}"
            : $"{database}.{QuoteIdent(schema)}.{QuoteIdent(table)}";
    }

    /// <summary>Contract prunes the projection; the plain incremental watermark is pushed down (the
    /// server evaluates the predicate); the window ceiling is MUST-apply. The engine embeds the
    /// fragment after FROM, which is why a bare table is returned unwrapped — anything with a
    /// projection or predicate becomes a parenthesized subquery instead.</summary>
    internal static string ScanFragment(string database, DatasetSpec spec)
    {
        var table = QualifiedTable(database, spec.Dataset);
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

    /// <summary>The copy statement(s) for one output. <c>{{source}}</c> is the engine's placeholder
    /// for the staged relation (substituted by SinkWriteExecutor). Append and merge first create the
    /// target from the staged shape so a first run needs no pre-created table; merge then matches on
    /// every declared key, updating all columns by name on a match and inserting otherwise. A
    /// keyless merge is refused at compile time; the throw here is ABI defense-in-depth.</summary>
    internal static bool TryCopySql(string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        var create = $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n";
        switch (mode)
        {
            case "append":
                sql = create + $"insert into {table} select * from {{{{source}}}};";
                mechanism = "motherduck insert";
                return true;
            case "replace":
                sql = $"create or replace table {table} as select * from {{{{source}}}}";
                mechanism = "motherduck create-or-replace";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException($"output '{table}': merge requires at least one key column", isTransient: false);
                }

                var on = string.Join(" and ", keys.Select(k => $"t.{QuoteIdent(k)} = s.{QuoteIdent(k)}"));

                // DuckDB's MERGE matches every source row independently against the target as it stood
                // before the statement ran: two staged rows sharing a key the target lacks would BOTH take
                // the not-matched branch and both insert, and duplicates of a held key would update it
                // in an undefined order. The sink contract says duplicate keys within one batch collapse
                // to a single connector-determined survivor, so the staged side is made key-unique
                // (qualify row_number() per key group) before MERGE ever sees it.
                var partitionBy = string.Join(", ", keys.Select(k => $"s.{QuoteIdent(k)}"));
                var dedupedSource = $"(select s.* from {{{{source}}}} as s qualify row_number() over (partition by {partitionBy}) = 1)";
                sql = create +
                    $"merge into {table} as t using {dedupedSource} as s on {on} " +
                    "when matched then update when not matched then insert;";
                mechanism = "motherduck merge";
                return true;
            default:
                sql = "";
                mechanism = "";
                return false;
        }
    }

    /// <summary>Renders the engine's canonical watermark string (plain digits for int/bigint/decimal,
    /// <c>yyyy-MM-dd</c> for date, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> for timestamp) as a literal:
    /// numerics stay bare; anything else is quoted, with a canonical timestamp's <c>T</c> separator
    /// becoming a space so it coerces against DATE/TIMESTAMP columns.</summary>
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
        config.GetString(key) is { Length: > 0 } s ? s : throw new PzConnectorException($"motherduck connection requires '{key}'", isTransient: false);
}
