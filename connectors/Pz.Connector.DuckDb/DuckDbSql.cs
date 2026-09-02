using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckDb;

/// <summary>All SQL/text generation for the native-only DuckDB-file connector. Everything here is
/// pure string building — the connector performs no I/O on the data plane at all. One dialect:
/// every statement (attach, scan fragment, copy) is parsed by DuckDB itself, so identifiers are
/// double-quoted (doubled to escape) and string literals double single quotes.
///
/// ONE alias per connection, shared by reads and writes: a DuckDB database file cannot be attached
/// twice in one session (a second attach of the same file under another alias is a unique-file-handle
/// conflict), so the read-only/read-write alias split other database connectors use is not available
/// here. The alias carries the same determinism/collision rule those connectors' aliases follow.
/// Kept in lockstep with the ducklake/quack/motherduck connectors' Sql classes (replicated, never
/// referenced).</summary>
internal static class DuckDbSql
{
    /// <summary>Sanitized connection name plus the first 8 lowercase hex chars of SHA-256 of the RAW
    /// name: <see cref="Sanitize"/> maps every non-alphanumeric to '_', so distinct names like
    /// "prod-db" and "prod_db" would otherwise collide on one alias — and <c>attach if not exists</c>
    /// is first-wins, a silent wrong-file I/O bug.</summary>
    internal static string Alias(string connectionName) =>
        $"pz_duckdb_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    /// <summary>Resolves the connection's <c>path:</c> to an absolute path: relative paths join the
    /// CLI-injected <c>base_dir</c> (never user-written), falling back to the process working
    /// directory for hosts that don't inject it. Statements always embed the absolute form — the
    /// engine's DuckDB session must not depend on process cwd.</summary>
    internal static string ResolvePath(ConnectorConfig config)
    {
        var path = config.GetString("path") ??
            throw new PzConnectorException("duckdb connection requires 'path'", isTransient: false);
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
            $"entity '{entity}': expected 'table' or 'schema.table'", isTransient: false);
    }

    internal static string QualifiedTable(string alias, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{alias}.{QuoteIdent(table)}"
            : $"{alias}.{QuoteIdent(schema)}.{QuoteIdent(table)}";
    }

    /// <summary>Setup for either direction: one read-write attach of the database file, idempotent
    /// under NativeSetup's per-node repetition (<c>attach if not exists</c> skips an existing alias).
    /// No extension is needed and the path carries no credentials, so a connect-failure echo of it is
    /// harmless and useful.</summary>
    internal static IReadOnlyList<string> SetupStatements(string resolvedPath, string alias) =>
        [$"attach if not exists '{EscapeLiteral(resolvedPath)}' as {alias}"];

    /// <summary>The scan fragment: a declared <c>columns:</c> contract prunes the read (only declared
    /// columns are projected); the plain (unwindowed) incremental watermark IS pushed into the
    /// fragment (DuckDB pushes the filter into the attached catalog's scan); the window ceiling is
    /// MUST-apply. The engine embeds the fragment after FROM, so a plain scan stays the bare qualified
    /// table and anything with a projection or predicate becomes a parenthesized subquery.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec)
    {
        var table = QualifiedTable(alias, spec.Dataset);

        var columns = ExtractColumns(spec);
        var projection = columns is { Count: > 0 }
            ? string.Join(", ", columns.Keys.Select(QuoteIdent))
            : "*";

        var predicates = new List<string>();
        if (spec is { WatermarkCursor: { } cursor, WatermarkValue: { } low })
        {
            var op = spec.WatermarkLowerInclusive ? ">=" : ">";
            predicates.Add($"{QuoteIdent(cursor)} {op} {RenderWatermarkLiteral(low)}");
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

    /// <summary>Renders the engine's canonical watermark string (plain digits for int/bigint/decimal,
    /// <c>yyyy-MM-dd</c> for date, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> for timestamp) as a literal:
    /// numerics stay bare; anything else is quoted, with a canonical timestamp's <c>T</c> separator
    /// becoming a space so the literal coerces against DATE/TIMESTAMP columns and still compares
    /// lexically against a text-typed cursor.</summary>
    internal static string RenderWatermarkLiteral(string canonical)
    {
        if (IsCanonicalNumeric(canonical))
        {
            return canonical;
        }

        var value = IsCanonicalTimestamp(canonical)
            ? canonical[..10] + ' ' + canonical[11..]
            : canonical;
        return $"'{EscapeLiteral(value)}'";
    }

    /// <summary>Optional leading '-', digits, at most one '.'. A canonical date ("2020-01-01") has an
    /// interior '-', so it never matches and falls through to the quoted form.</summary>
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
        value.Length >= 19 && value[10] == 'T' &&
        value[4] == '-' && value[7] == '-' && value[13] == ':' && value[16] == ':';

    internal static string QuoteIdent(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    internal static string EscapeLiteral(string value) => value.Replace("'", "''");

    internal static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) &&
        value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;

    private static string Sanitize(string name) => new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    /// <summary>Deterministic, no randomness, so an alias is stable across runs and processes.</summary>
    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
