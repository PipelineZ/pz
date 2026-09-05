using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.S3;

/// <summary>Native-only S3 sink: every output becomes a DuckDB COPY over httpfs with a
/// scoped CREATE SECRET. There is no universal write path in v0 — BeginWriteAsync always throws.</summary>
internal sealed class S3Sink(ConnectorConfig config) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        // The connection's `root:` says which lake -- "<bucket>" or "<bucket>/<prefix>".
        // An output may still name bucket/path itself; that is
        // composition at a different level, not a second declaration of the same thing.
        var (rootBucket, rootPrefix) = S3Sql.SplitRoot(config.GetString("root"));
        var bucket = spec.Options.TryGetValue("bucket", out var b) && b?.ToString() is { Length: > 0 } named
            ? named
            : rootBucket ?? throw new PzConnectorException(
                $"output '{spec.Output}': s3 needs a 'root' on the connection or a 'bucket' option",
                isTransient: false);
        var prefix = S3Sql.Join(
            rootBucket is null || (spec.Options.ContainsKey("bucket") && rootBucket != bucket) ? "" : rootPrefix,
            (spec.Options.TryGetValue("path", out var p) ? p?.ToString() : null)?.TrimEnd('/') ?? "");
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, null, "s3", context);
        FileFormatCatalog.EnsureWritable(format, "s3", context);
        FileFormatCatalog.EnsureRemoteWritable(format, "s3", context);
        var objectName = spec.Mode == "append"
            ? $"{spec.Output}-{Guid.NewGuid():N}.{format.Extension}"
            : $"{spec.Output}.{format.Extension}";
        var key = prefix.Length > 0 ? $"{prefix}/{objectName}" : objectName;
        copy = new NativeCopy(
            $"copy (select * from {{{{source}}}}) to 's3://{Esc(bucket)}/{Esc(key)}' ({FileFormatCatalog.CopyClause(format, spec.Options, context)})",
            [.. S3Sql.SetupStatements(config, S3Sql.SinkSecretName(spec.Sink)), .. FileFormatCatalog.SetupStatements(format)])
        {
            Mechanism = $"COPY TO s3 {format.Name}",
        };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException(
            $"output '{spec.Output}': s3 supports only the native COPY path in v0", isTransient: false);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private static string Esc(string value) => S3Sql.Esc(value);
}
