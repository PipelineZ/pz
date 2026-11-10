using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Sql;

namespace Pz.Connector.MySql;

/// <summary>All SQL/text generation for the native-only MySQL connector. Everything here is pure
/// string building — the connector performs no I/O on the data plane at all. Identifier quoting is
/// MySQL backticks (doubled to escape); string literals double single quotes; the completed inner
/// SELECT is escaped again when embedded into the <c>mysql_query('alias', '…')</c> DuckDB literal.
/// This is the typed-literals + Quote injection-safety pattern the other connectors follow.
///
/// Credentials ride a DuckDB secret, never the attach path: a failed ATTACH
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
    /// same connection also writes (one DuckDB session cannot hold one alias both ways). Aliases must
    /// stay distinct per raw connection name: <c>attach if not exists</c> is first-wins, so two
    /// connections sharing one would silently talk to the wrong server (see
    /// <see cref="DuckDbSqlText.ConnectionAlias"/>).</summary>
    internal static string SourceAlias(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_mysql_src", connectionName);

    internal static string SinkAlias(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_mysql_snk", connectionName);

    /// <summary>The secret backing one alias's attach — <c>create or replace secret</c> is last-wins
    /// and idempotent should a node retry re-issue it (the engine runs each setup statement once per
    /// run), just like the attach it precedes.</summary>
    internal static string SecretName(string alias) => alias + "_secret";

    /// <summary>The DuckDB <c>mysql</c> secret's CREATE SECRET body: every credential-bearing value is
    /// an ordinary single-quoted SQL string literal ('' -escaped), so no "no spaces or quotes"
    /// restriction applies to any value. Only configured optional keys
    /// are included; <c>create or replace</c> makes repeated calls (one per node) idempotent.</summary>
    internal static string CreateSecretSql(ConnectorConfig config, string secretName)
    {
        var host = config.GetString("host") ??
            throw new PzConnectorException("mysql connection requires 'host'", isTransient: false, code: PzConnectorErrorCodes.Config);
        var database = config.GetString("database") ??
            throw new PzConnectorException("mysql connection requires 'database'", isTransient: false, code: PzConnectorErrorCodes.Config);

        var body = new StringBuilder(
            $"host '{DuckDbSqlText.EscapeLiteral(host)}', port {config.GetInt("port") ?? 3306}, database '{DuckDbSqlText.EscapeLiteral(database)}'");
        if (config.GetString("user") is { Length: > 0 } user)
        {
            body.Append(", user '").Append(DuckDbSqlText.EscapeLiteral(user)).Append('\'');
        }

        if (config.GetString("password") is { Length: > 0 } password)
        {
            body.Append(", password '").Append(DuckDbSqlText.EscapeLiteral(password)).Append('\'');
        }

        if (config.GetString("ssl_mode") is { Length: > 0 } sslMode)
        {
            body.Append(", ssl_mode '").Append(DuckDbSqlText.EscapeLiteral(sslMode)).Append('\'');
        }

        return $"create or replace secret {secretName} (type mysql, {body})";
    }

    /// <summary>Setup statements for either direction. All idempotent should a node retry re-issue
    /// them (the engine runs each once per run): install/load are no-ops when present, CREATE OR REPLACE SECRET is last-wins, and
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
    internal static string ScanFragment(string alias, DatasetSpec spec, IReadOnlyList<string>? projected = null) =>
        $"mysql_query('{DuckDbSqlText.EscapeLiteral(alias)}', '{DuckDbSqlText.EscapeLiteral(InnerSelect(spec, projected))}')";

    /// <summary>The MySQL-side SELECT: a declared <c>columns:</c> contract prunes the read (only
    /// declared columns are projected — the csv/json rule); <c>query:</c> replaces the table as the
    /// read's base; a non-empty <paramref name="projected"/> narrows the projection, and under a contract
    /// keeps only contract columns (contract spelling) so the read never exceeds it; the watermark window
    /// renders as typed literals. The plain (unwindowed) incremental
    /// watermark IS pushed down — unlike the file connectors, extraction savings are the entire point
    /// of a database source, and DatasetSpec's contract explicitly permits it.</summary>
    internal static string InnerSelect(DatasetSpec spec, IReadOnlyList<string>? projected = null)
    {
        var columns = ColumnTypeCatalog.ContractColumns(spec);
        if (projected is { Count: > 0 } && columns is { Count: > 0 })
        {
            projected = [.. projected
                .Select(p => columns.Keys.FirstOrDefault(k => string.Equals(k, p, StringComparison.OrdinalIgnoreCase)))
                .OfType<string>()
                .Distinct(StringComparer.Ordinal)];
        }

        var projection = projected is { Count: > 0 }
            ? string.Join(", ", projected.Select(QuoteIdent))
            : columns is { Count: > 0 }
                ? string.Join(", ", columns.Keys.Select(QuoteIdent))
                : "*";

        var query = spec.Options.TryGetValue("query", out var q) ? q?.ToString() : null;

        var predicates = new List<string>();
        if (spec is { WatermarkCursor: { } cursor, WatermarkValue: { } low })
        {
            var op = spec.WatermarkLowerInclusive ? ">=" : ">";
            predicates.Add($"{QuoteIdent(cursor)} {op} {DuckDbSqlText.WatermarkLiteral(low)}");
        }

        if (spec is { WatermarkCursor: { } upperCursor, WatermarkUpperBound: { } high })
        {
            predicates.Add($"{QuoteIdent(upperCursor)} <= {DuckDbSqlText.WatermarkLiteral(high)}");
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

    internal static string QuoteIdent(string name) => $"`{name.Replace("`", "``")}`";
}
