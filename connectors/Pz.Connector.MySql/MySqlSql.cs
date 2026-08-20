using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MySql;

/// <summary>All SQL/text generation for the native-only MySQL connector. Everything here is pure
/// string building — the connector performs no I/O on the data plane at all. Identifier quoting is
/// MySQL backticks (doubled to escape); string literals double single quotes; the completed inner
/// SELECT is escaped again when embedded into the <c>mysql_query('alias', '…')</c> DuckDB literal.
/// This is the typed-literals + Quote injection-safety pattern the other connectors follow.
///
/// Credentials ride a DuckDB secret (S3/Azure precedent), never the attach path: a failed ATTACH
/// connect echoes its path VERBATIM, in double quotes, into the thrown engine error
/// (<c>Failed to connect to MySQL database with parameters "…"</c>) — a shape
/// <see cref="Pz.Engine.Execution.NativeStatementRedactor.SanitizeEngineMessage"/>'s single-quote
/// masking does not catch. The attach path is therefore always the empty string, so that echo is
/// credential-free by construction. Verified empirically against DuckDB v1.5.5/v1.5.4: the
/// <c>mysql</c> secret type accepts host/port/database/user/password/ssl_mode as ordinary (quotable,
/// '' -escaped) keyword parameters — nothing credential-bearing rides a non-quotable
/// position.</summary>
internal static class MySqlSql
{
    /// <summary>Direction-specific attach aliases so the source attach stays READ_ONLY even when the
    /// same connection also writes (one DuckDB session cannot hold one alias both ways). A short stable
    /// hash of the RAW connection name is appended: <see cref="Sanitize"/> maps
    /// every non-alphanumeric to '_', so distinct names like "prod-db" and "prod_db" would otherwise
    /// collide on one alias — and <c>attach if not exists</c> is first-wins, a silent wrong-server I/O
    /// bug. The hash is deterministic (SHA-256 of the exact connection name, first 8 lowercase hex
    /// chars) — no randomness, so aliases stay stable across runs and processes.</summary>
    internal static string SourceAlias(string connectionName) =>
        $"pz_mysql_src_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    internal static string SinkAlias(string connectionName) =>
        $"pz_mysql_snk_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    /// <summary>The secret backing one alias's attach — <c>create or replace secret</c> is last-wins
    /// and idempotent under NativeSetup's per-node repetition, just like the attach it precedes.</summary>
    internal static string SecretName(string alias) => alias + "_secret";

    /// <summary>The DuckDB <c>mysql</c> secret's CREATE SECRET body: every credential-bearing value is
    /// an ordinary single-quoted SQL string literal ('' -escaped), so no "no spaces or quotes"
    /// restriction applies to any value. Only configured optional keys
    /// are included; <c>create or replace</c> makes repeated calls (one per node) idempotent.</summary>
    internal static string CreateSecretSql(ConnectorConfig config, string secretName)
    {
        var host = config.GetString("host") ??
            throw new PzConnectorException("mysql connection requires 'host'", isTransient: false);
        var database = config.GetString("database") ??
            throw new PzConnectorException("mysql connection requires 'database'", isTransient: false);

        var body = new StringBuilder(
            $"host '{EscapeLiteral(host)}', port {config.GetInt("port") ?? 3306}, database '{EscapeLiteral(database)}'");
        if (config.GetString("user") is { Length: > 0 } user)
        {
            body.Append(", user '").Append(EscapeLiteral(user)).Append('\'');
        }

        if (config.GetString("password") is { Length: > 0 } password)
        {
            body.Append(", password '").Append(EscapeLiteral(password)).Append('\'');
        }

        if (config.GetString("ssl_mode") is { Length: > 0 } sslMode)
        {
            body.Append(", ssl_mode '").Append(EscapeLiteral(sslMode)).Append('\'');
        }

        return $"create or replace secret {secretName} (type mysql, {body})";
    }

    /// <summary>Setup statements for either direction. All idempotent under NativeSetup's per-node
    /// repetition: install/load are no-ops when present, CREATE OR REPLACE SECRET is last-wins, and
    /// ATTACH IF NOT EXISTS skips an existing alias (verified against DuckDB v1.5.5). The
    /// attach path is always the empty string — the secret carries every credential.</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string alias, bool readOnly)
    {
        var secretName = SecretName(alias);
        return
        [
            "install mysql",
            "load mysql",
            CreateSecretSql(config, secretName),
            $"attach if not exists '' as {alias} (type mysql, secret {secretName}" +
                (readOnly ? ", read_only)" : ")"),
        ];
    }

    /// <summary>The scan fragment: always <c>mysql_query('alias', '…')</c>, never a bare attached-table
    /// scan, so projection and the watermark window are guaranteed to execute inside MySQL.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec) =>
        $"mysql_query('{EscapeLiteral(alias)}', '{EscapeLiteral(InnerSelect(spec))}')";

    /// <summary>The MySQL-side SELECT: a declared <c>columns:</c> contract prunes the read (only
    /// declared columns are projected — the csv/json rule); <c>query:</c> replaces the table as the
    /// read's base; the watermark window renders as typed literals. The plain (unwindowed) incremental
    /// watermark IS pushed down — unlike the file connectors, extraction savings are the entire point
    /// of a database source, and DatasetSpec's contract explicitly permits it.</summary>
    internal static string InnerSelect(DatasetSpec spec)
    {
        var columns = ExtractColumns(spec);
        var projection = columns is { Count: > 0 }
            ? string.Join(", ", columns.Keys.Select(QuoteIdent))
            : "*";

        var query = spec.Options.TryGetValue("query", out var q) ? q?.ToString() : null;

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

        if (query is { Length: > 0 })
        {
            var trimmed = query.Trim().TrimEnd(';');
            if (projection == "*" && predicates.Count == 0)
            {
                return trimmed; // nothing to add — the user's SELECT goes to MySQL verbatim
            }

            return Compose(projection, $"({trimmed}) pzq", predicates);
        }

        return Compose(projection, QuoteIdent(spec.Dataset), predicates);
    }

    private static string Compose(string projection, string from, List<string> predicates)
    {
        var where = predicates.Count > 0 ? $" WHERE {string.Join(" AND ", predicates)}" : "";
        return $"SELECT {projection} FROM {from}{where}";
    }

    /// <summary>Renders the engine's canonical watermark string (plain digits for int/bigint/decimal,
    /// <c>yyyy-MM-dd</c> for date, <c>yyyy-MM-ddTHH:mm:ss.ffffff</c> for timestamp) as a MySQL literal:
    /// numerics stay bare; anything else is quoted, with a canonical timestamp's <c>T</c> separator
    /// becoming the space form MySQL universally accepts.</summary>
    internal static string RenderWatermarkLiteral(string canonical)
    {
        if (IsCanonicalNumeric(canonical))
        {
            return canonical; // int/bigint/decimal canonical form
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

    internal static string QuoteIdent(string name) => $"`{name.Replace("`", "``")}`";

    /// <summary>Quotes an identifier for a statement DuckDB itself parses (the sink's CopySql: the
    /// table reference in <c>create table alias.&lt;name&gt; ...</c>) -- DuckDB's own grammar has no
    /// backtick-quoted-identifier production at all (verified against DuckDB v1.5.5: <c>create table
    /// x (`a` int)</c> is a bare parser error, independent of any attached catalog), so
    /// <see cref="QuoteIdent"/>'s MySQL backticks are wrong here even though they are correct for
    /// every other identifier in this file, which all ride inside the <c>mysql_query('alias', '…')</c>
    /// string literal that MySQL itself parses.</summary>
    internal static string QuoteDuckIdent(string name) => $"\"{name.Replace("\"", "\"\"")}\"";

    internal static string EscapeLiteral(string value) => value.Replace("'", "''");

    internal static IReadOnlyDictionary<string, string>? ExtractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) &&
        value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;

    private static string Sanitize(string name) => new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    /// <summary>First 8 lowercase hex chars of SHA-256(name) — deterministic, no randomness, so an
    /// alias/secret name is stable across runs and processes.</summary>
    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
