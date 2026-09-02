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
