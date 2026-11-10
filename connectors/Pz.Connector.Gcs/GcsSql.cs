using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Sql;

namespace Pz.Connector.Gcs;

/// <summary>Shared text generation for the gcs connector's native tier: the scoped
/// <c>CREATE SECRET</c> (DuckDB's <c>type gcs</c> — HMAC interop keys, DuckDB's own
/// storage.googleapis.com default endpoint) both the source's native scan and the sink's native COPY
/// ride, plus the `root:` composition rules. Unlike s3 there is no region, and
/// endpoint/url_style/use_ssl are emitted only when configured, so DuckDB's gcs defaults stay
/// DuckDB's. Secret names are per direction and per raw connection name
/// (<see cref="DuckDbSqlText.ConnectionAlias"/>): <c>create or replace secret</c> is last-wins, so two
/// connections whose names sanitize identically ("prod-db"/"prod_db") must never share one. Secret names are internal: nothing in plan.json, Reason
/// strings, or events carries them.</summary>
internal static class GcsSql
{
    internal static string SourceSecretName(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_gcs_src", connectionName);

    internal static string SinkSecretName(string connectionName) =>
        DuckDbSqlText.ConnectionAlias("pz_gcs_snk", connectionName);

    /// <summary>The full setup-statement list for either direction: install/load httpfs (no-ops when
    /// present) + the scoped secret (<c>create or replace</c>, last-wins — all idempotent
    /// should a node retry re-issue them; the engine runs each once per run).</summary>
    internal static IReadOnlyList<string> SetupStatements(ConnectorConfig config, string secretName) =>
        ["install httpfs", "load httpfs", CreateSecretSql(config, secretName)];

    internal static string CreateSecretSql(ConnectorConfig config, string secretName)
    {
        var keyId = Require(config, "key_id");
        var secret = Require(config, "secret");
        var endpoint = config.GetString("endpoint");
        var urlStyle = config.GetString("url_style");
        var useSsl = config.Values.TryGetValue("use_ssl", out var raw) && raw is not null
            ? (bool?)config.GetBool("use_ssl", defaultValue: true)
            : null;

        return $"create or replace secret {secretName} (type gcs, key_id '{DuckDbSqlText.EscapeLiteral(keyId)}', " +
            $"secret '{DuckDbSqlText.EscapeLiteral(secret)}'" +
            (endpoint is null ? "" : $", endpoint '{DuckDbSqlText.EscapeLiteral(endpoint)}'") +
            (urlStyle is null ? "" : $", url_style '{DuckDbSqlText.EscapeLiteral(urlStyle)}'") +
            (useSsl is null ? "" : $", use_ssl {(useSsl.Value ? "true" : "false")}") + ")";
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
            : throw new PzConnectorException($"gcs connection requires '{key}'", isTransient: false, code: PzConnectorErrorCodes.Config);

}
