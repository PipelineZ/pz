using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake;

/// <summary>All SQL/text generation for the native-only DuckLake connector. Pure string building —
/// the connector performs no data-plane I/O. DuckDB parses every statement: identifiers are
/// double-quoted (doubled to escape), literals single-quoted ('' -escaped).
///
/// ONE alias per connection, read-write, shared by reads and writes: a lake whose catalog is a
/// DuckDB file cannot be attached twice in one session (unique-file-handle conflict on the metadata
/// database), so there is no read-only/read-write alias split. Credentials NEVER ride the attach
/// string — a failed ATTACH echoes its path verbatim into the engine error — they ride secrets or a
/// SET statement, which the engine describes without echoing. Kept in lockstep with the
/// duckdb/quack/motherduck connectors' Sql classes (replicated, never referenced).</summary>
internal static class DuckLakeSql
{
    /// <summary>Sanitized connection name plus the first 8 lowercase hex chars of SHA-256 of the RAW
    /// name, so "prod-db" and "prod_db" (both sanitize to "prod_db") never share an alias —
    /// <c>attach if not exists</c> is first-wins.</summary>
    internal static string Alias(string connectionName) =>
        $"pz_ducklake_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    internal static string SecretName(string alias) => alias + "_secret";

    internal static string StorageSecretName(string alias) => alias + "_storage";

    internal static string PostgresSecretName(string alias) => alias + "_pg";

    /// <summary>Resolves a local file/directory option to an absolute path: relative joins the
    /// CLI-injected <c>base_dir</c>, else the process working directory.</summary>
    internal static string ResolveLocal(ConnectorConfig config, string key)
    {
        var value = config.GetString(key) ??
            throw new PzConnectorException($"ducklake connection requires '{key}'", isTransient: false);
        if (Path.IsPathRooted(value))
        {
            return Path.GetFullPath(value);
        }

        var baseDir = config.GetString("base_dir") ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(baseDir, value));
    }

    /// <summary>A <c>data_path</c> with a scheme is object storage and passes through verbatim; anything
    /// else is a project-relative directory.</summary>
    internal static bool IsUrl(string value) => value.Contains("://", StringComparison.Ordinal);

    internal static string? ResolveDataPath(ConnectorConfig config)
    {
        var value = config.GetString("data_path");
        if (value is not { Length: > 0 })
        {
            return null;
        }

        return IsUrl(value) ? value : ResolveLocal(config, "data_path");
    }

    /// <summary>An entity is <c>table</c> (the lake's <c>main</c> schema) or <c>schema.table</c>:
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

    internal static string QualifiedTable(string alias, string entity)
    {
        var (schema, table) = SplitEntity(entity);
        return schema is null
            ? $"{alias}.{QuoteIdent(table)}"
            : $"{alias}.{QuoteIdent(schema)}.{QuoteIdent(table)}";
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

        var value = IsCanonicalTimestamp(canonical)
            ? canonical[..10] + ' ' + canonical[11..]
            : canonical;
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

    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
