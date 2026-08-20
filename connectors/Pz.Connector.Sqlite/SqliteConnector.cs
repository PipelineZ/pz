using Pz.Connectors.Abstractions;

[assembly: PzConnector("sqlite", typeof(Pz.Connector.Sqlite.SqliteConnector))]

namespace Pz.Connector.Sqlite;

/// <summary>SQLite source + sink connector — native-path-only on the MySQL pattern, simplified.
/// DuckDB's own `sqlite`
/// extension is the ENTIRE data plane: reads are self-contained <c>sqlite_scan('path', 'table')</c>
/// native scans (no attach, no alias), writes are one rw attach + INSERT / CREATE OR REPLACE native
/// copies, and the connector ships with zero .NET SQLite driver — pure SQL-fragment generation
/// (<see cref="INativeOnlySource"/> + <see cref="INativeOnlySink"/>; engine.force_universal fails at
/// plan time with PZ0312). There is no server and no credential: the connection is a file path,
/// resolved against the CLI-injected <c>base_dir</c> (localfiles precedent), so
/// <see cref="CheckConnectionAsync"/> is a real local check (the 16-byte header magic) rather than
/// MySQL's blind TCP probe. `pz validate --connect`'s schema fetch works only for datasets with a
/// declared `columns:` contract. Registered under the logical name "sqlite". Connection
/// options: path required, nothing else. No `query:` datasets (upstream `sqlite_query` is unusable),
/// no merge writes, no cdc.</summary>
public sealed class SqliteConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    /// <summary>The first 16 bytes of every SQLite database file: "SQLite format 3\0".</summary>
    private static ReadOnlySpan<byte> HeaderMagic => "SQLite format 3\0"u8;

    public ConnectorInfo Info => new("sqlite", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["path"], "properties": { "path": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    /// <summary>No cross-field rules: `path` required-ness is enforced by
    /// <see cref="ConnectionConfigSchema"/>'s `required` list, and a path is an ordinary
    /// ''-escaped SQL string literal everywhere it appears — nothing to refuse offline.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    /// <summary>A real local check, not a probe: an existing file must start with the SQLite
    /// header magic (the extension itself attaches a non-database file "fine" and fails only at first
    /// query); a missing file is OK with an explicit will-be-created note (a write-first
    /// project is legitimate); a missing parent directory is a permanent failure (sqlite will not
    /// create directories).</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        if (config.GetString("path") is not { Length: > 0 })
        {
            return new ConnectionCheck(false, "permanent: sqlite connection requires 'path'");
        }

        var path = SqliteSql.ResolvePath(config);
        if (!File.Exists(path))
        {
            var directory = Path.GetDirectoryName(path);
            if (directory is { Length: > 0 } && !Directory.Exists(directory))
            {
                return new ConnectionCheck(false,
                    $"permanent: directory '{directory}' does not exist -- create it, or fix the connection's 'path'");
            }

            return new ConnectionCheck(true,
                $"'{path}' does not exist yet -- it will be created on first write; reads will fail until it exists");
        }

        var header = new byte[HeaderMagic.Length];
        await using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            var read = await stream.ReadAtLeastAsync(header, header.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
            if (read < header.Length || !header.AsSpan().SequenceEqual(HeaderMagic))
            {
                return new ConnectionCheck(false,
                    $"permanent: '{path}' is not a SQLite database file (header magic mismatch)");
            }
        }

        return new ConnectionCheck(true, "sqlite database file verified (header magic)");
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SqliteSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SqliteSink(config));
}
