using Pz.Connectors.Abstractions;
using Pz.Shared.Sql;

namespace Pz.Connector.S3;

/// <summary>Shared text generation for the s3 connector's two directions: the
/// scoped <c>CREATE SECRET</c> both the source's native scan and the sink's native COPY ride, plus the
/// `root:` composition rules. Secret names are per direction and per raw connection name
/// (<see cref="DuckDbSqlText.ConnectionAlias"/>): <c>create or replace secret</c> is last-wins, so two
/// connections whose names sanitize identically ("prod-db"/"prod_db") must never share one.
/// Secret names are internal: nothing in plan.json, Reason strings, or events carries them.</summary>
internal static class S3Sql
{
    internal static string SourceSecretName(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_s3_src", connectionName);

    internal static string SinkSecretName(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_s3_snk", connectionName);

    /// <summary>The full setup-statement list for either direction: install/load httpfs (no-ops when
    /// present) + the scoped secret (<c>create or replace</c>, last-wins — all idempotent
    /// should a node retry re-issue them; the engine runs each once per run).</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string secretName) =>
        ["install httpfs", "load httpfs", CreateSecretSql(config, secretName)];

    internal static string CreateSecretSql(ConnectorConfig config, string secretName)
    {
        var accessKey = Require(config, "access_key");
        var secretKey = Require(config, "secret_key");
        var endpoint = config.GetString("endpoint");
        var region = config.GetString("region") ?? "us-east-1";
        var urlStyle = config.GetString("url_style") ?? "vhost";
        var useSsl = config.GetBool("use_ssl", defaultValue: true);

        return $"create or replace secret {secretName} (type s3, key_id '{DuckDbSqlText.EscapeLiteral(accessKey)}', " +
            $"secret '{DuckDbSqlText.EscapeLiteral(secretKey)}', region '{DuckDbSqlText.EscapeLiteral(region)}'" +
            (endpoint is null ? "" : $", endpoint '{DuckDbSqlText.EscapeLiteral(endpoint)}'") +
            $", url_style '{DuckDbSqlText.EscapeLiteral(urlStyle)}', use_ssl {(useSsl ? "true" : "false")})";
    }

    /// <summary>Splits a connection <c>root:</c> into its bucket and optional key prefix. Null root
    /// yields (null, ""), so a dataset/output naming its own bucket needs no root at all.</summary>
    internal static (string? Bucket, string Prefix) SplitRoot(string? root)
    {
        if (root is not { Length: > 0 })
        {
            return (null, "");
        }

        var trimmed = root.Trim('/');
        var slash = trimmed.IndexOf('/', StringComparison.Ordinal);
        return slash < 0 ? (trimmed, "") : (trimmed[..slash], trimmed[(slash + 1)..]);
    }

    internal static string Join(string left, string right) =>
        left.Length == 0 ? right : right.Length == 0 ? left : $"{left}/{right}";

    private static string Require(ConnectorConfig config, string key) =>
        config.GetString(key) is { Length: > 0 } s
            ? s
            : throw new PzConnectorException($"s3 connection requires '{key}'", isTransient: false, code: PzConnectorErrorCodes.Config);

}
