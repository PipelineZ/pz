using Pz.Connectors.Abstractions;

[assembly: PzConnector("duckdb", typeof(Pz.Connector.DuckDb.DuckDbConnector))]

namespace Pz.Connector.DuckDb;

/// <summary>DuckDB database-file source + sink connector — native-path-only. The engine's own DuckDB
/// session ATTACHes the file and every read/write is a plain statement against that alias; the
/// connector ships zero drivers and does no data-plane I/O (<see cref="INativeOnlySource"/> +
/// <see cref="INativeOnlySink"/>; engine.force_universal fails at plan time with PZ0312). The
/// connection is a file path resolved against the CLI-injected <c>base_dir</c>, so
/// <see cref="CheckConnectionAsync"/> is a real local check (the header magic). `pz validate
/// --connect`'s schema fetch works only for datasets with a declared `columns:` contract.
/// Registered under the logical name "duckdb". One writer per file: an external process writing
/// the same file during a run fails with DuckDB's lock error.</summary>
public sealed class DuckDbConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    /// <summary>Every DuckDB database file carries "DUCK" at byte offset 8 (after the 8-byte checksum).</summary>
    private const int MagicOffset = 8;
    private static ReadOnlySpan<byte> HeaderMagic => "DUCK"u8;

    public ConnectorInfo Info => new("duckdb", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.Transactional |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["path"], "properties": { "path": { "type": "string" }, "base_dir": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    /// <summary>One cross-field rule: the file must not live under the project's <c>.pz/</c>, the
    /// engine's own staging/state area (attaching a run's staging database to itself has no
    /// legitimate use). Only checkable when the host injected <c>base_dir</c>.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        if (config.GetString("path") is not { Length: > 0 } || config.GetString("base_dir") is not { Length: > 0 } baseDir)
        {
            return new(ValidationResult.Success);
        }

        var resolved = DuckDbSql.ResolvePath(config);
        var pzDir = Path.GetFullPath(Path.Combine(baseDir, ".pz")) + Path.DirectorySeparatorChar;
        return resolved.StartsWith(pzDir, StringComparison.Ordinal)
            ? new(ValidationResult.Failed(
                "duckdb connection 'path' resolves inside the project's .pz/ directory, which is pz's own " +
                "staging and state area -- point it at a database file outside .pz/"))
            : new(ValidationResult.Success);
    }

    /// <summary>A real local check: an existing file must carry the DuckDB header magic; a missing
    /// file is OK with an explicit will-be-created note (a write-first project is legitimate); a
    /// missing parent directory is a permanent failure (ATTACH will not create directories).</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        if (config.GetString("path") is not { Length: > 0 })
        {
            return new ConnectionCheck(false, "permanent: duckdb connection requires 'path'");
        }

        var path = DuckDbSql.ResolvePath(config);
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

        var header = new byte[MagicOffset + HeaderMagic.Length];
        await using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            var read = await stream.ReadAtLeastAsync(header, header.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
            if (read < header.Length || !header.AsSpan(MagicOffset).SequenceEqual(HeaderMagic))
            {
                return new ConnectionCheck(false,
                    $"permanent: '{path}' is not a DuckDB database file (header magic mismatch)");
            }
        }

        return new ConnectionCheck(true, "duckdb database file verified (header magic)");
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DuckDbSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new DuckDbSink(config));
}
