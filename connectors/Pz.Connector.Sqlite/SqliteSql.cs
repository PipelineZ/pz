using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sqlite;

/// <summary>All SQL/text generation for the native-only SQLite connector. Everything here is pure
/// string building — the connector performs no I/O on the data plane at all. Unlike MySQL there is
/// only ONE dialect: every statement (scan fragment, attach, copy) is parsed by DuckDB itself, so
/// identifiers are DuckDB double-quoted (doubled to escape) and string literals double single quotes.
/// Reads need no attach and no alias — <c>sqlite_scan('&lt;path&gt;', '&lt;table&gt;')</c> is a
/// self-contained table function — so only the sink carries the alias/hash machinery (the same
/// determinism/collision rule MySQL's aliases follow).</summary>
internal static class SqliteSql
{
    internal static string SinkAlias(string connectionName) =>
        $"pz_sqlite_snk_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    /// <summary>Resolves the connection's <c>path:</c> to an absolute path: relative paths join the
    /// CLI-injected <c>base_dir</c> (the localfiles precedent — never user-written), falling back to
    /// the process working directory for hosts that don't inject it. Fragments always embed the
    /// absolute form — the engine's DuckDB session must not depend on process cwd.</summary>
    internal static string ResolvePath(ConnectorConfig config)
    {
        var path = config.GetString("path") ??
            throw new PzConnectorException("sqlite connection requires 'path'", isTransient: false);
        if (Path.IsPathRooted(path))
        {
            return Path.GetFullPath(path);
        }

        var baseDir = config.GetString("base_dir") ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(baseDir, path));
    }

    /// <summary>Source setup: nothing beyond the extension itself. Both statements are
    /// idempotent no-ops when already installed/loaded.</summary>
    internal static IReadOnlyList<string> SetupStatements() => ["install sqlite", "load sqlite"];

    /// <summary>Sink setup: extension + one read-write attach of the database file. All idempotent
    /// under NativeSetup's per-node repetition (<c>attach if not exists</c> skips an existing alias).
    /// The attach path carries no credentials, so a connect-failure echo of it is harmless and useful
    /// — MySQL's empty-path/secret indirection has nothing to protect here.</summary>
    internal static IReadOnlyList<string> SinkSetupStatements(string resolvedPath, string alias) =>
    [
        "install sqlite",
        "load sqlite",
        $"attach if not exists '{EscapeLiteral(resolvedPath)}' as {alias} (type sqlite)",
    ];

    /// <summary>The scan fragment: a declared <c>columns:</c> contract prunes the read (only
    /// declared columns are projected — the csv/json/mysql rule); the plain (unwindowed) incremental
    /// watermark IS pushed into the fragment (database-source precedent; DuckDB's sqlite scanner pushes
    /// the filter into the file scan); the window ceiling is MUST-apply. There is no <c>query:</c>
    /// option on this connector — upstream <c>sqlite_query</c> is unusable.
    ///
    /// The engine embeds the fragment after FROM (<c>create … as select * from &lt;fragment&gt;</c>),
    /// so a plain scan stays the bare table-function call (the read_csv/read_parquet shape) and
    /// anything with a projection or predicate becomes a parenthesized subquery.</summary>
    internal static string ScanFragment(string resolvedPath, DatasetSpec spec)
    {
        var scan = $"sqlite_scan('{EscapeLiteral(resolvedPath)}', '{EscapeLiteral(spec.Dataset)}')";

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
            return scan;
        }

        var where = predicates.Count > 0 ? $" where {string.Join(" and ", predicates)}" : "";
        return $"(select {projection} from {scan}{where})";
    }

    /// <summary>Renders the engine's canonical watermark string (plain digits for int/bigint/decimal,
    /// <c>yyyy-MM-dd</c> for date, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> for timestamp) as a literal:
    /// numerics stay bare; anything else is quoted, with a canonical timestamp's <c>T</c> separator
    /// becoming a space. The space form matters more here than for MySQL: a text-stored
    /// sqlite cursor column surfaces as VARCHAR and compares LEXICALLY, and sqlite's own timestamp
    /// convention (<c>CURRENT_TIMESTAMP</c>) is the space-separated ISO form — which also still
    /// coerces correctly against a real DATE/TIMESTAMP column.</summary>
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

    /// <summary>First 8 lowercase hex chars of SHA-256(name) — deterministic, no randomness, so an
    /// alias is stable across runs and processes.</summary>
    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
