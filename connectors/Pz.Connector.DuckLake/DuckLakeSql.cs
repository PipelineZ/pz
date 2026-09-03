using System.Globalization;
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
    /// CLI-injected <c>base_dir</c>, else the process working directory. Statements always embed the
    /// absolute form — the engine's DuckDB session must not depend on process cwd.</summary>
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

    /// <summary>Resolves <c>data_path</c> for the attach clause: a URL passes through verbatim,
    /// anything else goes through <see cref="ResolveLocal"/> and comes back absolute — the engine's
    /// DuckDB session must not depend on the process working directory.</summary>
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

    /// <summary>Setup for either direction, in order: extension installs/loads (ducklake, the
    /// catalog's own, httpfs when the data path is object storage), the storage secret (only when
    /// the data path is object storage — <c>storage_key_id</c>/<c>storage_secret_key</c> against a
    /// local data path is a validation error, so this is a defensive no-op, not the primary guard),
    /// the catalog's credential carrier, then ONE read-write attach. All idempotent
    /// should a node retry re-issue them (the engine runs each once per run): install/load are no-ops, <c>create or replace secret</c>
    /// is last-wins, <c>set</c> is repeatable, <c>attach if not exists</c> skips an existing alias.
    ///
    /// The attach string carries no credential for any catalog: postgres credentials ride a
    /// postgres secret referenced from a ducklake secret whose metadata_path is empty by
    /// construction; the quack token rides a quack secret scoped to the server URI; the MotherDuck
    /// token is a session setting. A failed attach therefore echoes only a path, a URI, or a
    /// database name.
    ///
    /// The quack URI is canonicalized to <c>quack:host:port</c> before it lands in either the
    /// attach string or the secret's scope — <c>quack:host</c>, <c>quack:host:port</c> and
    /// <c>quack://host[:port]</c> are all accepted at validation, but the secret's scope and the
    /// attach must name the server identically or the scoped secret silently fails to match.</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string alias)
    {
        var catalog = DuckLakeCatalog.Of(config);
        var dataPath = ResolveDataPath(config);
        var statements = new List<string> { "install ducklake", "load ducklake" };

        switch (catalog)
        {
            case DuckLakeCatalog.Sqlite:
                statements.AddRange(["install sqlite", "load sqlite"]);
                break;
            case DuckLakeCatalog.Postgres:
                statements.AddRange(["install postgres", "load postgres"]);
                break;
            case DuckLakeCatalog.Quack:
                statements.AddRange(["install quack", "load quack"]);
                break;
            case DuckLakeCatalog.MotherDuck:
                statements.AddRange(["install motherduck", "load motherduck"]);
                break;
        }

        if (dataPath is not null && IsUrl(dataPath))
        {
            statements.AddRange(["install httpfs", "load httpfs"]);
        }

        if (dataPath is not null && IsUrl(dataPath) && config.GetString("storage_key_id") is { Length: > 0 })
        {
            statements.Add(StorageSecretSql(config, alias, dataPath));
        }

        var dataClause = dataPath is null ? "" : $" (data_path '{EscapeLiteral(dataPath)}')";
        switch (catalog)
        {
            case DuckLakeCatalog.DuckDb:
                statements.Add($"attach if not exists 'ducklake:{EscapeLiteral(ResolveLocal(config, "path"))}' as {alias}{dataClause}");
                break;
            case DuckLakeCatalog.Sqlite:
                statements.Add($"attach if not exists 'ducklake:sqlite:{EscapeLiteral(ResolveLocal(config, "path"))}' as {alias}{dataClause}");
                break;
            case DuckLakeCatalog.Postgres:
                var pgDataPath = dataPath ??
                    throw new PzConnectorException("ducklake connection requires 'data_path'", isTransient: false);
                var pgSecret = PostgresSecretName(alias);
                var secret = SecretName(alias);
                statements.Add(PostgresSecretSql(config, pgSecret));
                statements.Add(
                    $"create or replace secret {secret} (type ducklake, metadata_path '', data_path '{EscapeLiteral(pgDataPath)}', " +
                    $"metadata_parameters map {{'TYPE': 'postgres', 'SECRET': '{pgSecret}'}})");
                statements.Add($"attach if not exists 'ducklake:{secret}' as {alias}");
                break;
            case DuckLakeCatalog.Quack:
                var uri = Require(config, "uri");
                if (!DuckLakeCatalog.TryParseQuackUri(uri, out var host, out var port))
                {
                    throw new PzConnectorException(
                        "ducklake connection 'uri' must be of the form quack:host[:port]", isTransient: false);
                }

                var canonical = $"quack:{host}:{port}";
                statements.Add(
                    $"create or replace secret {SecretName(alias)} (type quack, token '{EscapeLiteral(Require(config, "token"))}', " +
                    $"scope '{EscapeLiteral(canonical)}')");
                statements.Add($"attach if not exists 'ducklake:{EscapeLiteral(canonical)}' as {alias}{dataClause}");
                break;
            case DuckLakeCatalog.MotherDuck:
                statements.Add($"set motherduck_token = '{EscapeLiteral(Require(config, "token"))}'");
                statements.Add(
                    $"attach if not exists 'ducklake:md:__ducklake_metadata_{EscapeLiteral(Require(config, "database"))}' as {alias}{dataClause}");
                break;
            default:
                throw new PzConnectorException($"ducklake connection: unknown catalog '{catalog}'", isTransient: false);
        }

        return statements;
    }

    /// <summary>The DuckDB <c>postgres</c> secret: every value is an ordinary ''-escaped literal, so no
    /// value can break out of its position.</summary>
    private static string PostgresSecretSql(ConnectorConfig config, string secretName)
    {
        var body = new StringBuilder(
            $"host '{EscapeLiteral(Require(config, "host"))}', port {config.GetInt("port") ?? 5432}, " +
            $"database '{EscapeLiteral(Require(config, "database"))}'");
        if (config.GetString("user") is { Length: > 0 } user)
        {
            body.Append(", user '").Append(EscapeLiteral(user)).Append('\'');
        }

        if (config.GetString("password") is { Length: > 0 } password)
        {
            body.Append(", password '").Append(EscapeLiteral(password)).Append('\'');
        }

        return $"create or replace secret {secretName} (type postgres, {body})";
    }

    /// <summary>S3-compatible storage credentials as a secret SCOPED to the data path, so they apply
    /// to the lake's files and nothing else in the session. Defaults match the s3 connector's.</summary>
    internal static string StorageSecretSql(ConnectorConfig config, string alias, string dataPath)
    {
        var region = config.GetString("storage_region") ?? "us-east-1";
        var endpoint = config.GetString("storage_endpoint");
        var urlStyle = config.GetString("storage_url_style") ?? "vhost";
        var useSsl = config.GetBool("storage_use_ssl", defaultValue: true);

        return $"create or replace secret {StorageSecretName(alias)} (type s3, " +
            $"key_id '{EscapeLiteral(Require(config, "storage_key_id"))}', " +
            $"secret '{EscapeLiteral(Require(config, "storage_secret_key"))}', region '{EscapeLiteral(region)}'" +
            (endpoint is null ? "" : $", endpoint '{EscapeLiteral(endpoint)}'") +
            $", url_style '{EscapeLiteral(urlStyle)}', use_ssl {(useSsl ? "true" : "false")}, scope '{EscapeLiteral(dataPath)}')";
    }

    /// <summary>The scan fragment: a declared <c>columns:</c> contract prunes the read; the optional
    /// time-travel clause pins the snapshot; the plain (unwindowed) incremental watermark IS pushed
    /// down (DuckDB pushes the filter into the DuckLake scan); the window ceiling is MUST-apply. A
    /// plain scan stays the bare (optionally time-travelled) table reference; anything with a
    /// projection or predicate becomes a parenthesized subquery.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec)
    {
        var table = QualifiedTable(alias, spec.Dataset) + TimeTravelClause(spec);

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

    /// <summary><c>version:</c> pins a snapshot id, <c>timestamp:</c> the latest snapshot at or before
    /// an instant; declaring both is contradictory and refused. Thrown from TryGetNativeScan, which
    /// the planner probes, so the error surfaces at plan time. The rendered literal must be invariant
    /// and deterministic regardless of the host culture, since a Scriban kwarg can hand this a
    /// <c>DateTime</c>/<c>DateTimeOffset</c> rather than a string.</summary>
    internal static string TimeTravelClause(DatasetSpec spec)
    {
        var hasVersion = spec.Options.TryGetValue("version", out var versionValue) && versionValue is not null;
        var hasTimestamp = spec.Options.TryGetValue("timestamp", out var timestampValue) && timestampValue is not null;
        if (hasVersion && hasTimestamp)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': declare either version: or timestamp:, not both", isTransient: false);
        }

        if (hasVersion)
        {
            if (!long.TryParse(versionValue!.ToString(), out var version) || version < 0)
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': version: must be a non-negative snapshot id (got '{versionValue}')",
                    isTransient: false);
            }

            return $" at (version => {version})";
        }

        return hasTimestamp
            ? $" at (timestamp => timestamp '{EscapeLiteral(RenderTimestampOption(timestampValue!))}')"
            : "";
    }

    /// <summary>A <c>timestamp:</c> option string passes through for DuckDB's own parser to validate;
    /// a <c>DateTime</c>/<c>DateTimeOffset</c> (reachable through the Scriban kwarg surface) is
    /// formatted invariantly so the literal never depends on the host culture.</summary>
    private static string RenderTimestampOption(object value) => value switch
    {
        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
    };

    /// <summary>The copy statement(s) for one output; <c>{{source}}</c> is the engine's placeholder
    /// for the staged relation. Append and merge first create the target from the staged shape so a
    /// first run needs no pre-created table; merge matches on every declared key, updating all
    /// columns by name on a match and inserting otherwise. Each statement commits as one DuckLake
    /// snapshot. A keyless merge is refused at compile time; the throw is defense-in-depth.</summary>
    internal static bool TryCopySql(string table, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        var create = $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n";
        switch (mode)
        {
            case "append":
                sql = create + $"insert into {table} select * from {{{{source}}}};";
                mechanism = "ducklake insert";
                return true;
            case "replace":
                sql = $"create or replace table {table} as select * from {{{{source}}}}";
                mechanism = "ducklake create-or-replace";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException(
                        $"output '{table}': merge requires at least one key column", isTransient: false);
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
                sql = create + $"merge into {table} as t using {dedupedSource} as s on {on} " +
                    "when matched then update when not matched then insert;";
                mechanism = "ducklake merge";
                return true;
            default:
                sql = "";
                mechanism = "";
                return false;
        }
    }

    private static string Require(ConnectorConfig config, string key) =>
        config.GetString(key) is { Length: > 0 } s
            ? s
            : throw new PzConnectorException($"ducklake connection requires '{key}'", isTransient: false);

    private static string Sanitize(string name) => new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
