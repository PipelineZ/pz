using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Sql;

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
        DuckDbSqlText.ConnectionAlias("pz_sqlite_snk", connectionName);

    /// <summary>Resolves the connection's <c>path:</c> to an absolute path: relative paths join the
    /// CLI-injected <c>base_dir</c> (never user-written), falling back to
    /// the process working directory for hosts that don't inject it. Fragments always embed the
    /// absolute form — the engine's DuckDB session must not depend on process cwd.</summary>
    internal static string ResolvePath(ConnectorConfig config)
    {
        var path = config.GetString("path") ??
            throw new PzConnectorException("sqlite connection requires 'path'", isTransient: false, code: PzConnectorErrorCodes.Config);
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
    /// should a node retry re-issue them — the engine runs each once per run (<c>attach if not
    /// exists</c> skips an existing alias).
    /// The attach path carries no credentials, so a connect-failure echo of it is harmless and useful
    /// — MySQL's empty-path/secret indirection has nothing to protect here.</summary>
    internal static IReadOnlyList<string> SinkSetupStatements(string resolvedPath, string alias) =>
    [
        "install sqlite",
        "load sqlite",
        $"attach if not exists '{DuckDbSqlText.EscapeLiteral(resolvedPath)}' as {alias} (type sqlite)",
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
    internal static string ScanFragment(string resolvedPath, DatasetSpec spec) =>
        DuckDbSqlText.ScanFragment(
            $"sqlite_scan('{DuckDbSqlText.EscapeLiteral(resolvedPath)}', '{DuckDbSqlText.EscapeLiteral(spec.Dataset)}')", spec);

}
