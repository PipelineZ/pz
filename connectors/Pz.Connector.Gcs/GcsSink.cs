using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs;

/// <summary>Gcs sink, two tiers selected by the connection's auth method: under hmac every
/// non-partitioned output is a native DuckDB COPY over httpfs with the scoped sink secret (the s3
/// shapes over <c>gs://</c>) and the universal tier is the clear refusal; under service_account/adc
/// there is no DuckDB secret at all, so native is declined and writes ride SDK-backed universal
/// write sessions. Partitioned (<c>partition_by</c>) outputs decline native under every method — a
/// single COPY ... TO cannot express a per-row-value fan-out (the azure rule).</summary>
internal sealed class GcsSink(ConnectorConfig config) : ISink
{
    private static readonly string[] ValidFormats = ["parquet", "csv", "json"];

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        if (!GcsAuth.IsHmac(config) || PartitionColumns.Read(spec.Options).Count > 0)
        {
            return false;
        }

        var (format, objectName) = ResolveObjectName(spec);
        var (bucket, prefix) = ResolvePrefix(spec);
        var key = prefix.Length > 0 ? $"{prefix}/{objectName}" : objectName;
        // json is DuckDB's newline-delimited (NDJSON) writer -- the same `format json` COPY shape the
        // localfiles/s3 native paths use.
        var formatClause = format switch
        {
            "parquet" => "format parquet",
            "csv" => "format csv, header",
            _ => "format json",
        };
        copy = new NativeCopy(
            $"copy (select * from {{{{source}}}}) to 'gs://{GcsSql.Esc(bucket)}/{GcsSql.Esc(key)}' ({formatClause})",
            GcsSql.SetupStatements(config, GcsSql.SinkSecretName(spec.Sink)))
        {
            Mechanism = $"COPY TO gcs {format}",
        };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        if (GcsAuth.IsHmac(config))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': gcs 'hmac' auth supports only the native COPY path " +
                "(remove engine.force_universal, or use 'service_account'/'adc' auth for SDK-backed writes)",
                isTransient: false);
        }

        throw new NotSupportedException("gcs universal write sessions are not implemented yet");
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>The validated format plus the final object name, per the replace/append naming
    /// convention shared with the other object-store sinks: "replace" is a stable name
    /// (<c>&lt;output&gt;.&lt;format&gt;</c>); "append" lands under a run-unique guid-suffixed name
    /// instead so repeated runs accumulate objects.</summary>
    private static (string Format, string ObjectName) ResolveObjectName(OutputSpec spec)
    {
        var format = spec.Options.TryGetValue("format", out var f) && f?.ToString() is { Length: > 0 } s
            ? s
            : throw new PzConnectorException($"output '{spec.Output}': gcs requires 'format'", isTransient: false);
        if (!ValidFormats.Contains(format))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': gcs 'format' must be one of 'parquet', 'csv', 'json' (got '{format}')",
                isTransient: false);
        }

        var objectName = string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? $"{spec.Output}-{Guid.NewGuid():N}.{format}"
            : $"{spec.Output}.{format}";
        return (format, objectName);
    }

    /// <summary>(bucket, key prefix) under the ratified `root:` composition — the source's rule,
    /// reused: an output naming its OWN bucket does not inherit the root's prefix.</summary>
    private (string Bucket, string Prefix) ResolvePrefix(OutputSpec spec)
    {
        var (rootBucket, rootPrefix) = GcsSql.SplitRoot(config.GetString("root"));
        var bucket = spec.Options.TryGetValue("bucket", out var b) && b?.ToString() is { Length: > 0 } named
            ? named
            : rootBucket ?? throw new PzConnectorException(
                $"output '{spec.Output}': gcs needs a 'root' on the connection or a 'bucket' option",
                isTransient: false);
        var prefix = GcsSql.Join(
            rootBucket is null || (spec.Options.ContainsKey("bucket") && rootBucket != bucket) ? "" : rootPrefix,
            (spec.Options.TryGetValue("path", out var p) ? p?.ToString() : null)?.Trim('/') ?? "");
        return (bucket, prefix);
    }
}
