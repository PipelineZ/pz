using Pz.Connectors.Abstractions;

[assembly: PzConnector("ducklake", typeof(Pz.Connector.DuckLake.DuckLakeConnector))]

namespace Pz.Connector.DuckLake;

/// <summary>DuckLake source + sink connector — native-path-only. DuckDB's own `ducklake` extension
/// is the ENTIRE data plane: the engine's session attaches the lake once per connection and every
/// read/write is a plain statement against that alias. The connection names the lake's catalog
/// (a DuckDB file, a SQLite file, a Postgres server, a DuckDB server reached over Quack, or a
/// MotherDuck database) and its data path; optional S3-compatible storage credentials cover an
/// object-store data path. Zero drivers: `pz validate --connect` verifies file catalogs by header
/// magic and server catalogs by TCP reachability only, and its schema fetch works only for datasets
/// with a declared `columns:` contract. Registered under the logical name "ducklake".</summary>
public sealed class DuckLakeConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    private static readonly byte[] DuckDbMagic = "DUCK"u8.ToArray();
    private static readonly byte[] SqliteMagic = "SQLite format 3\0"u8.ToArray();

    public ConnectorInfo Info => new("ducklake", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.Transactional |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """
        { "type": "object", "properties": {
          "catalog": { "enum": ["duckdb", "sqlite", "postgres", "quack", "motherduck"] },
          "path": { "type": "string" }, "data_path": { "type": "string" },
          "host": { "type": "string" }, "port": { "type": "integer", "minimum": 1, "maximum": 65535 },
          "database": { "type": "string" }, "user": { "type": "string" }, "password": { "type": "string" },
          "uri": { "type": "string" }, "token": { "type": "string" },
          "storage_key_id": { "type": "string" }, "storage_secret_key": { "type": "string" },
          "storage_region": { "type": "string" }, "storage_endpoint": { "type": "string" },
          "storage_url_style": { "enum": ["vhost", "path"] }, "storage_use_ssl": { "type": "boolean" }
        }, "additionalProperties": false }
        """;

    public string DatasetConfigSchema =>
        """
        { "type": "object", "properties": {
          "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } },
          "version": { "type": "integer", "minimum": 0 },
          "timestamp": { "type": "string" }
        }, "additionalProperties": false }
        """;

    /// <summary>The root a RELATIVE <c>path</c>/<c>data_path</c> is normalized against when no
    /// <c>base_dir</c> is present. Config validation runs on the connection as the user wrote it,
    /// before the host injects the anchor, and a relative value is project-relative by definition --
    /// so the containment question ("does this land under the project's .pz/?") is answerable
    /// without knowing where the project is, as long as both sides of the comparison share one
    /// stand-in root. A value that climbs out with <c>../</c> resolves above this root, matches
    /// nothing, and is none of this rule's business.</summary>
    private static readonly string StandInProjectRoot =
        Path.GetFullPath(Path.DirectorySeparatorChar + "pz-project");

    /// <summary>One cross-field rule, applied to both <c>path</c> and <c>data_path</c>: neither may
    /// live under the project's <c>.pz/</c>, the engine's own staging/state area (attaching a run's
    /// staging database to itself has no legitimate use). A relative value is checked against
    /// <see cref="StandInProjectRoot"/> so the rule fires on the pre-injection config tier-3
    /// validation actually sees; an ABSOLUTE value is only comparable when the host did inject
    /// <c>base_dir</c>. <c>data_path</c> is skipped when it names an object store (a URL) — object
    /// storage cannot land under a local <c>.pz/</c> by construction.</summary>
    private static string? PzDirError(ConnectorConfig config, string key)
    {
        if (config.GetString(key) is not { Length: > 0 } value)
        {
            return null;
        }

        if (key == "data_path" && DuckLakeSql.IsUrl(value))
        {
            return null;
        }

        var baseDir = config.GetString("base_dir") is { Length: > 0 } injected ? injected : null;
        if (Path.IsPathRooted(value) && baseDir is null)
        {
            return null;
        }

        var root = baseDir ?? StandInProjectRoot;
        var resolved = Path.GetFullPath(Path.IsPathRooted(value) ? value : Path.Combine(root, value));
        var pzDir = Path.GetFullPath(Path.Combine(root, ".pz")) + Path.DirectorySeparatorChar;
        return resolved.StartsWith(pzDir, StringComparison.Ordinal)
            ? $"ducklake connection '{key}' resolves inside the project's .pz/ directory, which is pz's own " +
              "staging and state area -- point it outside .pz/"
            : null;
    }

    /// <summary>The catalog key matrix, aggregate (see <see cref="DuckLakeCatalog.Validate"/>), plus
    /// the <c>.pz/</c>-containment rule on <c>path</c> and <c>data_path</c> (see
    /// <see cref="PzDirError"/>).</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>(DuckLakeCatalog.Validate(config));
        if (PzDirError(config, "path") is { } pathError)
        {
            errors.Add(pathError);
        }

        if (PzDirError(config, "data_path") is { } dataPathError)
        {
            errors.Add(dataPathError);
        }

        return new(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = DuckLakeCatalog.Validate(config);
        if (errors.Count > 0)
        {
            return new(new ConnectionCheck(false, "permanent: " + errors[0]));
        }

        switch (DuckLakeCatalog.Of(config))
        {
            case DuckLakeCatalog.DuckDb:
                return DuckLakeProbe.CheckFileAsync(DuckLakeSql.ResolveLocal(config, "path"), DuckDbMagic, 8, "DuckDB database", ct);
            case DuckLakeCatalog.Sqlite:
                return DuckLakeProbe.CheckFileAsync(DuckLakeSql.ResolveLocal(config, "path"), SqliteMagic, 0, "SQLite database", ct);
            case DuckLakeCatalog.Postgres:
                return DuckLakeProbe.TcpAsync(config.GetString("host")!, (int)(config.GetInt("port") ?? 5432), "postgres catalog", ct);
            case DuckLakeCatalog.Quack:
                DuckLakeCatalog.TryParseQuackUri(config.GetString("uri")!, out var host, out var port);
                return DuckLakeProbe.TcpAsync(host, port, "quack catalog server", ct);
            default:
                return new(new ConnectionCheck(true, "not checked: motherduck has no offline probe; the first run authenticates"));
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DuckLakeSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DuckLakeSink(config));
}
