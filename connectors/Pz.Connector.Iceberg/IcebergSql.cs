using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>All SQL/text generation for the native-only Iceberg connector. Pure string building —
/// the connector performs no data-plane I/O. DuckDB parses every statement: identifiers are
/// double-quoted (doubled to escape), literals single-quoted ('' -escaped).
///
/// ONE alias per catalog connection, read-write, shared by reads and writes. Credentials NEVER ride
/// the attach string — a failed ATTACH echoes its warehouse and endpoint verbatim into the engine
/// error — they ride DuckDB secrets, which the engine describes without echoing. A <c>files</c>
/// connection has no catalog to attach: every read is an <c>iceberg_scan</c> over the table's
/// directory under <c>root</c>. Kept in lockstep with the duckdb/ducklake/quack/motherduck
/// connectors' Sql classes (replicated, never referenced).</summary>
internal static class IcebergSql
{
    /// <summary>Sanitized connection name plus the first 8 lowercase hex chars of SHA-256 of the RAW
    /// name, so "prod-db" and "prod_db" (both sanitize to "prod_db") never share an alias —
    /// <c>attach if not exists</c> is first-wins.</summary>
    internal static string Alias(string connectionName) =>
        $"pz_iceberg_{Sanitize(connectionName)}_{HashSuffix(connectionName)}";

    internal static string SecretName(string alias) => alias + "_secret";

    internal static string StorageSecretName(string alias) => alias + "_storage";

    /// <summary>A value with a scheme is object storage and passes through verbatim; anything else
    /// is a project-relative directory.</summary>
    internal static bool IsUrl(string value) => value.Contains("://", StringComparison.Ordinal);

    /// <summary>Resolves a local directory option to an absolute path: relative joins the
    /// CLI-injected <c>base_dir</c>, else the process working directory. Statements always embed the
    /// absolute form — the engine's DuckDB session must not depend on process cwd.</summary>
    internal static string ResolveLocal(ConnectorConfig config, string key)
    {
        var value = Require(config, key);
        if (Path.IsPathRooted(value))
        {
            return Path.GetFullPath(value);
        }

        var baseDir = config.GetString("base_dir") ?? Directory.GetCurrentDirectory();
        return Path.GetFullPath(Path.Combine(baseDir, value));
    }

    /// <summary>The <c>files</c> root: an object-store URL passes through (trailing slash trimmed so
    /// table paths join with exactly one separator), anything else comes back absolute.</summary>
    internal static string ResolveRoot(ConnectorConfig config)
    {
        var value = Require(config, "root");
        return IsUrl(value) ? value.TrimEnd('/') : ResolveLocal(config, "root");
    }

    /// <summary>The <c>warehouse</c> attach string as written (a URL, a name, an ARN, or a Glue
    /// account/catalog form) — never resolved: it names something on the catalog's side.</summary>
    internal static string? Warehouse(ConnectorConfig config) =>
        config.GetString("warehouse") is { Length: > 0 } value ? value : null;

    /// <summary>An Iceberg table always lives in a namespace, so a catalog entity is exactly
    /// <c>namespace.table</c>: one split on the first dot; a bare name, more dots, or an empty side
    /// is a permanent error. A <c>files</c> entity may also be a bare <c>table</c> directly under
    /// <c>root</c>. When <paramref name="namespaceRequired"/> (a catalog-attached
    /// <see cref="QualifiedTable"/>), the namespace <c>main</c> is refused: DuckDB's binder treats
    /// <c>main</c> as its own default schema name and never asks the catalog for it, so a table there
    /// is unreachable through the attach. A <c>files</c> read has no binder involved — it is a raw
    /// path under <c>root</c> — so <c>main</c> is a perfectly ordinary directory name there.</summary>
    internal static (string? Namespace, string Table) SplitEntity(string entity, bool namespaceRequired)
    {
        var dot = entity.IndexOf('.', StringComparison.Ordinal);
        if (dot < 0)
        {
            if (entity.Length == 0 || namespaceRequired)
            {
                throw new PzConnectorException(
                    $"entity '{entity}': expected 'namespace.table' (an Iceberg table always lives in a namespace)",
                    isTransient: false);
            }

            return (null, entity);
        }

        var ns = entity[..dot];
        var table = entity[(dot + 1)..];
        if (ns.Length == 0 || table.Length == 0 || table.Contains('.'))
        {
            throw new PzConnectorException(
                $"entity '{entity}': expected 'namespace.table' (nested namespaces are not supported)", isTransient: false);
        }

        if (ns == "main" && namespaceRequired)
        {
            throw new PzConnectorException(
                $"entity '{entity}': namespace 'main' cannot be addressed through DuckDB's iceberg extension -- " +
                "DuckDB reserves that name for its own default schema; use another namespace", isTransient: false);
        }

        return (ns, table);
    }

    internal static string QualifiedTable(string alias, string entity)
    {
        var (ns, table) = SplitEntity(entity, namespaceRequired: true);
        return $"{alias}.{QuoteIdent(ns!)}.{QuoteIdent(table)}";
    }

    /// <summary>The table directory under a <c>files</c> root: <c>root/namespace/table</c>, or
    /// <c>root/table</c> for a bare entity. URL roots join with '/'; local roots with the platform
    /// separator.</summary>
    internal static string TablePath(string root, string entity)
    {
        var (ns, table) = SplitEntity(entity, namespaceRequired: false);
        if (IsUrl(root))
        {
            return ns is null ? $"{root}/{table}" : $"{root}/{ns}/{table}";
        }

        return ns is null ? Path.Combine(root, table) : Path.Combine(root, ns, table);
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

    /// <summary>Setup for either direction, in order: extension installs/loads (iceberg; httpfs for
    /// every catalog and for an object-store <c>files</c> root; azure under <c>storage: azure</c>, in
    /// place of aws; aws when an AWS catalog runs on the credential chain), the storage secret, the
    /// catalog secret, then ONE read-write attach (none for <c>files</c>). All idempotent should a
    /// node retry re-issue them (the engine runs each once per run): install/load are no-ops,
    /// <c>create or replace secret</c> is last-wins, <c>attach if not exists</c> skips an existing
    /// alias.
    ///
    /// The attach string carries no credential for any catalog: a bearer token or OAuth2 client pair
    /// rides a <c>type iceberg</c> secret the attach references by name; AWS catalogs sign with the
    /// <c>type s3</c> secret (explicit keys, or the credential chain). A failed attach therefore
    /// echoes only the warehouse and the endpoint. Explicit storage keys on a REST catalog also turn
    /// credential vending off (<c>access_delegation_mode 'none'</c>): the keys ARE the data-plane
    /// credential, and a catalog that cannot vend (MinIO-backed dev catalogs, the Apache REST
    /// fixture) would otherwise be asked to. An Azure <c>storage_auth</c> method is the same kind of
    /// explicit data-plane credential and turns vending off the same way.</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string alias)
    {
        var catalog = IcebergCatalog.Of(config);
        var statements = new List<string> { "install iceberg", "load iceberg" };
        var root = catalog == IcebergCatalog.Files ? ResolveRoot(config) : null;
        var azure = IcebergCatalog.StorageOf(config) == IcebergCatalog.StorageAzure;
        var hasStorageKeys = IcebergCatalog.HasStorageCredentials(config);

        if (root is null || IsUrl(root))
        {
            statements.AddRange(["install httpfs", "load httpfs"]);
        }

        if (azure)
        {
            statements.AddRange(["install azure", "load azure"]);
        }
        else if (IcebergCatalog.IsAws(catalog) && !hasStorageKeys)
        {
            statements.AddRange(["install aws", "load aws"]);
        }

        if (azure)
        {
            if (hasStorageKeys)
            {
                statements.Add(AzureStorageSecretSql(config, alias, scope: root));
            }
        }
        else if (hasStorageKeys && (root is null || IsUrl(root)))
        {
            statements.Add(StorageSecretSql(config, alias, scope: root));
        }
        else if (IcebergCatalog.IsAws(catalog))
        {
            statements.Add(CredentialChainSecretSql(config, alias));
        }

        var nested = config.GetBool("nested_namespaces") ? ", support_nested_namespaces true" : "";
        switch (catalog)
        {
            case IcebergCatalog.Rest:
                var options = new List<string> { "type iceberg", $"endpoint '{EscapeLiteral(Require(config, "endpoint"))}'" };
                if (IcebergCatalog.HasCatalogCredentials(config))
                {
                    statements.Add(CatalogSecretSql(config, SecretName(alias)));
                    options.Add($"secret {SecretName(alias)}");
                }
                else
                {
                    options.Add("authorization_type 'none'");
                }

                if (hasStorageKeys)
                {
                    options.Add("access_delegation_mode 'none'");
                }

                statements.Add(
                    $"attach if not exists '{EscapeLiteral(Warehouse(config) ?? "")}' as {alias} ({string.Join(", ", options)}{nested})");
                break;
            case IcebergCatalog.Glue:
                statements.Add(
                    $"attach if not exists '{EscapeLiteral(Warehouse(config) ?? ":")}' as {alias} (type iceberg, endpoint_type 'glue'{nested})");
                break;
            case IcebergCatalog.S3Tables:
                statements.Add(
                    $"attach if not exists '{EscapeLiteral(Require(config, "warehouse"))}' as {alias} (type iceberg, endpoint_type 's3_tables'{nested})");
                break;
            case IcebergCatalog.Files:
                break;
            default:
                throw new PzConnectorException($"iceberg connection: unknown catalog '{catalog}'", isTransient: false);
        }

        return statements;
    }

    /// <summary>The DuckDB <c>iceberg</c> secret carrying the catalog credential: a bearer token, or
    /// an OAuth2 client pair with its optional token endpoint and scope. Every value is an ordinary
    /// ''-escaped literal, so no value can break out of its position.</summary>
    internal static string CatalogSecretSql(ConnectorConfig config, string secretName)
    {
        if (config.GetString("token") is { Length: > 0 } token)
        {
            return $"create or replace secret {secretName} (type iceberg, token '{EscapeLiteral(token)}')";
        }

        var body = new StringBuilder(
            $"client_id '{EscapeLiteral(Require(config, "client_id"))}', client_secret '{EscapeLiteral(Require(config, "client_secret"))}'");
        if (config.GetString("oauth2_server_uri") is { Length: > 0 } serverUri)
        {
            body.Append(", oauth2_server_uri '").Append(EscapeLiteral(serverUri)).Append('\'');
        }

        if (config.GetString("oauth2_scope") is { Length: > 0 } scope)
        {
            body.Append(", oauth2_scope '").Append(EscapeLiteral(scope)).Append('\'');
        }

        return $"create or replace secret {secretName} (type iceberg, {body})";
    }

    /// <summary>S3-compatible storage credentials as a secret, SCOPED to the <c>files</c> root so they
    /// apply to that root's tables and nothing else in the session. A catalog connection's data
    /// location is not knowable up front (the catalog hands out each table's location), so its secret
    /// is unscoped; DuckDB still prefers a longer-scoped secret (another connector's) for any path one
    /// covers. Defaults match the s3 connector's.</summary>
    internal static string StorageSecretSql(ConnectorConfig config, string alias, string? scope)
    {
        var region = config.GetString("storage_region") ?? "us-east-1";
        var endpoint = config.GetString("storage_endpoint");
        var urlStyle = config.GetString("storage_url_style") ?? "vhost";
        var useSsl = config.GetBool("storage_use_ssl", defaultValue: true);

        return $"create or replace secret {StorageSecretName(alias)} (type s3, " +
            $"key_id '{EscapeLiteral(Require(config, "storage_key_id"))}', " +
            $"secret '{EscapeLiteral(Require(config, "storage_secret_key"))}', region '{EscapeLiteral(region)}'" +
            (endpoint is null ? "" : $", endpoint '{EscapeLiteral(endpoint)}'") +
            $", url_style '{EscapeLiteral(urlStyle)}', use_ssl {(useSsl ? "true" : "false")}" +
            (scope is null ? "" : $", scope '{EscapeLiteral(scope)}'") + ")";
    }

    /// <summary>Azure storage credentials as a <c>type azure</c> secret. One body per
    /// <c>storage_auth</c> method, field-for-field the azureblob connector's shapes: the two
    /// key-bearing methods funnel through a connection string (a custom <c>storage_endpoint</c>
    /// becomes its <c>BlobEndpoint=</c>), the two token-bearing ones name a provider and the
    /// account. A <c>files</c> root is scoped WITH a trailing slash — the azure extension's scope
    /// match is a plain prefix test on the slash-terminated form, so <c>az://c/wh</c> alone would
    /// also cover <c>az://c/wh2</c>. A catalog connection's secret is unscoped, as for S3.</summary>
    internal static string AzureStorageSecretSql(ConnectorConfig config, string alias, string? scope)
    {
        var auth = IcebergCatalog.StorageAuth(config);
        var body = auth switch
        {
            "connection_string" => $"connection_string '{EscapeLiteral(Require(config, "storage_connection_string"))}'",
            "account_key" => $"connection_string '{EscapeLiteral(AssembleAzureConnectionString(config))}'",
            "service_principal" =>
                $"provider service_principal, tenant_id '{EscapeLiteral(Require(config, "storage_tenant_id"))}', " +
                $"client_id '{EscapeLiteral(Require(config, "storage_client_id"))}', " +
                $"client_secret '{EscapeLiteral(Require(config, "storage_client_secret"))}', " +
                $"account_name '{EscapeLiteral(Require(config, "storage_account_name"))}'",
            "credential_chain" => "provider credential_chain" +
                (config.GetString("storage_chain") is { Length: > 0 } chain ? $", chain '{EscapeLiteral(chain)}'" : "") +
                $", account_name '{EscapeLiteral(Require(config, "storage_account_name"))}'",
            _ => throw new PzConnectorException($"iceberg connection: unsupported storage_auth '{auth}'", isTransient: false),
        };

        var scoped = scope is null ? "" : $", scope '{EscapeLiteral(scope.TrimEnd('/') + "/")}'";
        return $"create or replace secret {StorageSecretName(alias)} (type azure, {body}{scoped})";
    }

    private static string AssembleAzureConnectionString(ConnectorConfig config)
    {
        var name = Require(config, "storage_account_name");
        var key = Require(config, "storage_account_key");
        var suffix = config.GetString("storage_endpoint") is { Length: > 0 } endpoint
            ? $";BlobEndpoint={endpoint}"
            : ";EndpointSuffix=core.windows.net";
        return $"DefaultEndpointsProtocol=https;AccountName={name};AccountKey={key}{suffix}";
    }

    /// <summary>An AWS catalog with no explicit keys signs with the ambient AWS credential chain
    /// (environment, profile, instance role) — the same chain the AWS CLI resolves.</summary>
    internal static string CredentialChainSecretSql(ConnectorConfig config, string alias)
    {
        var region = config.GetString("storage_region") ?? "us-east-1";
        return $"create or replace secret {StorageSecretName(alias)} (type s3, provider credential_chain, region '{EscapeLiteral(region)}')";
    }

    /// <summary>The catalog scan fragment: a declared <c>columns:</c> contract prunes the read; the
    /// optional time-travel clause pins the snapshot; the plain (unwindowed) incremental watermark IS
    /// pushed down (DuckDB pushes the filter into the Iceberg scan); the window ceiling is
    /// MUST-apply. A plain scan stays the bare (optionally time-travelled) table reference; anything
    /// with a projection or predicate becomes a parenthesized subquery.</summary>
    internal static string ScanFragment(string alias, DatasetSpec spec)
    {
        if (spec.Options.TryGetValue("metadata_version", out var metadataVersion) && metadataVersion is not null)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': metadata_version: applies to catalog 'files' only -- a catalog resolves the current metadata itself",
                isTransient: false);
        }

        var table = QualifiedTable(alias, spec.Dataset) + TimeTravelClause(spec);
        return Wrap(table, spec);
    }

    /// <summary>The <c>files</c> scan fragment: <c>iceberg_scan</c> over the table directory, always
    /// tolerant of relocated tables (<c>allow_moved_paths</c>), pinned by <c>version:</c>
    /// (<c>snapshot_from_id</c>) or <c>timestamp:</c> (<c>snapshot_from_timestamp</c>), and pointed
    /// at an explicit metadata file by <c>metadata_version:</c> for a table without a
    /// version-hint file (every table a REST catalog wrote).</summary>
    internal static string FilesScanFragment(string root, DatasetSpec spec)
    {
        var (snapshot, timestamp) = TimeTravelOptions(spec);
        var arguments = new StringBuilder($"'{EscapeLiteral(TablePath(root, spec.Dataset))}', allow_moved_paths = true");
        if (spec.Options.TryGetValue("metadata_version", out var metadataVersion) && metadataVersion is not null)
        {
            arguments.Append($", version => '{EscapeLiteral(Convert.ToString(metadataVersion, CultureInfo.InvariantCulture) ?? "")}'");
        }

        if (snapshot is { } id)
        {
            arguments.Append($", snapshot_from_id => {id}");
        }

        if (timestamp is { } at)
        {
            arguments.Append($", snapshot_from_timestamp => timestamp '{EscapeLiteral(at)}'");
        }

        return Wrap($"iceberg_scan({arguments})", spec);
    }

    private static string Wrap(string table, DatasetSpec spec)
    {
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

    /// <summary><c>version:</c> pins a snapshot id, <c>timestamp:</c> the snapshot current at an
    /// instant; declaring both is contradictory and refused. Thrown from TryGetNativeScan, which the
    /// planner probes, so the error surfaces at plan time. Rendered as literals: DuckDB's AT clause
    /// takes no subqueries.</summary>
    internal static string TimeTravelClause(DatasetSpec spec)
    {
        var (snapshot, timestamp) = TimeTravelOptions(spec);
        if (snapshot is { } id)
        {
            return $" at (version => {id})";
        }

        return timestamp is { } at ? $" at (timestamp => timestamp '{EscapeLiteral(at)}')" : "";
    }

    /// <summary>An Iceberg snapshot id is an unsigned 64-bit value. The timestamp literal must be
    /// invariant and deterministic regardless of the host culture, since a Scriban kwarg can hand
    /// this a <c>DateTime</c>/<c>DateTimeOffset</c> rather than a string.</summary>
    private static (ulong? Snapshot, string? Timestamp) TimeTravelOptions(DatasetSpec spec)
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
            var text = Convert.ToString(versionValue, CultureInfo.InvariantCulture) ?? "";
            if (!ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var snapshot))
            {
                throw new PzConnectorException(
                    $"dataset '{spec.Dataset}': version: must be a non-negative snapshot id (got '{versionValue}')",
                    isTransient: false);
            }

            return (snapshot, null);
        }

        return hasTimestamp ? (null, RenderTimestampOption(timestampValue!)) : (null, null);
    }

    private static string RenderTimestampOption(object value) => value switch
    {
        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "",
    };

    /// <summary>The copy statement(s) for one output; <c>{{source}}</c> is the engine's placeholder
    /// for the staged relation. Every mode first ensures the namespace and creates the target from
    /// the staged shape so a first run needs no pre-created table. Append is a plain INSERT (one
    /// <c>append</c> snapshot); replace is DELETE + INSERT inside one transaction — DuckDB's iceberg
    /// extension commits one snapshot PER DML statement, never a combined <c>overwrite</c> snapshot
    /// for a delete+insert pair (no SQL shape avoids this: a single MERGE with a "not matched by
    /// source" delete clause still splits into a delete snapshot and an append snapshot, and the
    /// extension outright refuses CREATE OR REPLACE against an attached catalog), so a replace is
    /// always exactly two new snapshots, a delete immediately followed by an append; the wrapping
    /// transaction's actual guarantee is that the two land together or not at all, never the delete
    /// alone. Dropping the table first was rejected too — it would discard the table's history; merge
    /// matches on every declared key, updating all columns by name on a match and inserting otherwise.
    /// A keyless merge is refused at compile time; the throw is defense-in-depth.</summary>
    internal static bool TryCopySql(string alias, string entity, string mode, IReadOnlyList<string> keys, out string sql, out string mechanism)
    {
        var (ns, _) = SplitEntity(entity, namespaceRequired: true);
        var table = QualifiedTable(alias, entity);
        var prelude =
            $"create schema if not exists {alias}.{QuoteIdent(ns!)};\n" +
            $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n";
        var insert = $"insert into {table} select * from {{{{source}}}};";
        switch (mode)
        {
            case "append":
                sql = prelude + insert;
                mechanism = "iceberg insert";
                return true;
            case "replace":
                sql = prelude + $"begin transaction;\ndelete from {table};\n{insert}\ncommit;";
                mechanism = "iceberg overwrite";
                return true;
            case "merge":
                if (keys.Count == 0)
                {
                    throw new PzConnectorException(
                        $"output '{entity}': merge requires at least one key column", isTransient: false);
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
                sql = prelude + $"merge into {table} as t using {dedupedSource} as s on {on} " +
                    "when matched then update when not matched then insert;";
                mechanism = "iceberg merge";
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
            : throw new PzConnectorException($"iceberg connection requires '{key}'", isTransient: false);

    private static string Sanitize(string name) => new([.. name.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')]);

    private static string HashSuffix(string name) =>
        Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..8];
}
