using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake;

/// <summary>The catalog key matrix: which connection keys each catalog requires and which it
/// forbids. Validation is offline and aggregate — every stray or missing key is one error naming
/// the catalog it belongs to — so an author fixes a whole block in one pass.</summary>
internal static class DuckLakeCatalog
{
    internal const string DuckDb = "duckdb";
    internal const string Sqlite = "sqlite";
    internal const string Postgres = "postgres";
    internal const string Quack = "quack";
    internal const string MotherDuck = "motherduck";

    private const int DefaultQuackPort = 9494;

    private static readonly string[] PostgresKeys = ["host", "port", "database", "user", "password"];
    private static readonly string[] QuackKeys = ["uri", "token"];
    private static readonly string[] StorageKeys = ["storage_region", "storage_endpoint", "storage_url_style", "storage_use_ssl"];

    internal static string Of(ConnectorConfig config) => config.GetString("catalog") ?? DuckDb;

    internal static IReadOnlyList<string> Validate(ConnectorConfig config)
    {
        var catalog = Of(config);
        var errors = new List<string>();

        void Require(string key) { if (config.GetString(key) is not { Length: > 0 }) { errors.Add($"catalog '{catalog}' requires '{key}'"); } }
        void Forbid(IEnumerable<string> keys, string owner)
        {
            foreach (var key in keys)
            {
                if (config.Values.ContainsKey(key))
                {
                    errors.Add($"'{key}' belongs to catalog '{owner}' and is not valid for catalog '{catalog}'");
                }
            }
        }

        switch (catalog)
        {
            case DuckDb:
                Require("path");
                Forbid(PostgresKeys, Postgres);
                Forbid(QuackKeys, Quack);
                break;
            case Sqlite:
                Require("path");
                Require("data_path");
                Forbid(PostgresKeys, Postgres);
                Forbid(QuackKeys, Quack);
                break;
            case Postgres:
                Require("host");
                Require("database");
                Require("data_path");
                Forbid(["path"], DuckDb);
                Forbid(["uri", "token"], Quack);
                break;
            case Quack:
                Require("uri");
                Require("token");
                Require("data_path");
                Forbid(["path"], DuckDb);
                Forbid(PostgresKeys, Postgres);
                if (config.GetString("uri") is { Length: > 0 } uri && !TryParseQuackUri(uri, out _, out _))
                {
                    errors.Add("'uri' must be of the form quack:host[:port]");
                }

                break;
            case MotherDuck:
                Require("database");
                Require("token");
                Require("data_path");
                Forbid(["path"], DuckDb);
                Forbid(PostgresKeys.Where(k => k != "database"), Postgres);
                Forbid(["uri"], Quack);
                break;
            default:
                errors.Add($"unknown catalog '{catalog}' (expected duckdb, sqlite, postgres, quack or motherduck)");
                break;
        }

        var hasKey = config.GetString("storage_key_id") is { Length: > 0 };
        var hasSecret = config.GetString("storage_secret_key") is { Length: > 0 };
        if (hasKey != hasSecret)
        {
            errors.Add("'storage_key_id' and 'storage_secret_key' must be declared together");
        }

        if (!hasKey && !hasSecret)
        {
            foreach (var key in StorageKeys.Where(k => config.Values.ContainsKey(k)))
            {
                errors.Add($"'{key}' requires 'storage_key_id' and 'storage_secret_key'");
            }
        }

        return errors;
    }

    /// <summary>Accepts <c>quack:host</c>, <c>quack:host:port</c> and <c>quack://host[:port]</c>;
    /// the port defaults to the quack server's own default.</summary>
    internal static bool TryParseQuackUri(string uri, out string host, out int port)
    {
        host = "";
        port = DefaultQuackPort;
        if (!uri.StartsWith("quack:", StringComparison.Ordinal))
        {
            return false;
        }

        var rest = uri["quack:".Length..].TrimStart('/');
        if (rest.Length == 0)
        {
            return false;
        }

        var colon = rest.LastIndexOf(':');
        if (colon < 0)
        {
            host = rest;
            return true;
        }

        host = rest[..colon];
        return host.Length > 0 && int.TryParse(rest[(colon + 1)..], out port) && port is > 0 and <= 65535;
    }
}
