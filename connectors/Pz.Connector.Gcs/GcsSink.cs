using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Cloud.Storage.V1;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Gcs;

/// <summary>Gcs sink, two tiers selected by the connection's auth method: under hmac every
/// non-partitioned output is a native DuckDB COPY over httpfs with the scoped sink secret (the s3
/// shapes over <c>gs://</c>) and the universal tier is the clear refusal; under service_account/adc
/// there is no DuckDB secret at all, so native is declined and writes ride SDK-backed universal
/// spool-then-atomic-upload write sessions (<see cref="GcsWriteSession"/>). Partitioned
/// (<c>partition_by</c>) outputs decline native under every method — a single COPY ... TO cannot
/// express a per-row-value fan-out (the azure rule).
///
/// GatedOperations: the engine calls <see cref="UseOperationGate"/> exactly once after this sink is
/// opened, before any <see cref="BeginWriteAsync"/> call; the stored gate threads into every session
/// so each discrete upload op routes through it. Native COPY is unaffected (out of .NET reach).
/// The client factory seam exists for the test suites (an emulator-backed or fake client); the
/// production path always resolves through <see cref="GcsAuth.CreateStorageClient"/>, lazily so a
/// native-only (hmac) sink never constructs one.</summary>
internal sealed class GcsSink(ConnectorConfig config, Func<StorageClient>? clientFactory = null)
    : ISink, IOperationGateAware
{
    private StorageClient? _client;
    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        if (!GcsAuth.IsHmac(config))
        {
            return false;
        }

        if (PartitionColumns.Read(spec.Options).Count > 0)
        {
            // Refused HERE, at plan time, because declining native would route this output to the
            // universal tier and the run would then die at execute time blaming
            // engine.force_universal -- a cause the user never set.
            throw new PzConnectorException(
                $"output '{spec.Output}': partition_by fan-out runs on the SDK write tier, which " +
                "'hmac' auth does not carry -- use 'service_account'/'adc' auth for this connection, " +
                "or remove partition_by", isTransient: false);
        }

        var (format, objectName) = ResolveObjectName(spec);
        var (bucket, prefix) = ResolvePrefix(spec);
        var key = prefix.Length > 0 ? $"{prefix}/{objectName}" : objectName;
        var context = $"output '{spec.Output}'";
        copy = new NativeCopy(
            $"copy (select * from {{{{source}}}}) to 'gs://{GcsSql.Esc(bucket)}/{GcsSql.Esc(key)}' ({FileFormatCatalog.CopyClause(format, spec.Options, context)})",
            [.. GcsSql.SetupStatements(config, GcsSql.SinkSecretName(spec.Sink)), .. FileFormatCatalog.SetupStatements(format)])
        {
            Mechanism = $"COPY TO gcs {format.Name}",
        };
        return true;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        if (GcsAuth.IsHmac(config))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': gcs 'hmac' auth supports only the native COPY path " +
                "(remove engine.force_universal, or use 'service_account'/'adc' auth for SDK-backed writes)",
                isTransient: false);
        }

        var (format, objectName) = ResolveObjectName(spec);
        FileFormatCatalog.EnsureUniversalTierSupported(format, spec.Options, "gcs", $"output '{spec.Output}'");
        var (bucket, prefix) = ResolvePrefix(spec);
        var client = _client ??= (clientFactory ?? (() => GcsAuth.CreateStorageClient(config)))();

        // Partitioned output routes rows into per-day folders; validate the partition column and build a
        // folder->inner-session fan-out session instead of a single-object session. Exactly one column:
        // this connector renders pz's calendar tokens from a timestamp column, and DagCompiler's PZ0219
        // has already refused a multi-column partition_by against a templated path. The folder template
        // is the full key prefix (root prefix + templated path), so each rendered folder lands at
        // <renderedFolder>/<objectName> -- sharing the exact object name the single-object path uses so
        // replace/append naming is identical per folder.
        if (PartitionColumns.Read(spec.Options) is [var partitionBy])
        {
            var partitionColIndex = ResolvePartitionColumn(spec.Output, schema, partitionBy);

            ValueTask<ISinkWriteSession> Open(string folder)
            {
                var trimmed = folder.Trim('/');
                var key = trimmed.Length > 0 ? $"{trimmed}/{objectName}" : objectName;
                return OpenSessionAsync(client, format, bucket, key, schema, ct);
            }

            return new GcsPartitionedWriteSession(Open, prefix, partitionColIndex, schema);
        }

        var singleKey = prefix.Length > 0 ? $"{prefix}/{objectName}" : objectName;
        return await OpenSessionAsync(client, format, bucket, singleKey, schema, ct).ConfigureAwait(false);
    }

    private async ValueTask<ISinkWriteSession> OpenSessionAsync(
        StorageClient client, FileFormat format, string bucket, string key, Schema schema, CancellationToken ct) =>
        format.Name switch
        {
            "parquet" => await GcsParquetWriteSession.CreateAsync(client, bucket, key, schema, ct, _gate)
                .ConfigureAwait(false),
            "csv" => GcsCsvWriteSession.Create(client, bucket, key, schema, _gate),
            "json" => GcsJsonWriteSession.Create(client, bucket, key, _gate),
            _ => throw new InvalidOperationException("unreachable: format already validated by EnsureUniversalTierSupported"),
        };

    /// <summary>Runtime fail-fast for the partition column: it must exist in the write schema AND be a
    /// timestamp/date Arrow type. Same shape as the azure/postgres pre-checks -- permanent, names the
    /// output and column, thrown before any resource is opened.</summary>
    private static int ResolvePartitionColumn(string output, Schema schema, string partitionBy)
    {
        var fields = schema.FieldsList;
        for (var i = 0; i < fields.Count; i++)
        {
            if (!string.Equals(fields[i].Name, partitionBy, StringComparison.Ordinal))
            {
                continue;
            }

            if (fields[i].DataType.TypeId is not (ArrowTypeId.Timestamp or ArrowTypeId.Date32))
            {
                throw new PzConnectorException(
                    $"output '{output}': partition_by column '{partitionBy}' is not a timestamp/date column " +
                    $"(got '{fields[i].DataType}')", isTransient: false);
            }

            return i;
        }

        throw new PzConnectorException(
            $"output '{output}': partition_by column '{partitionBy}' is not present in the write schema",
            isTransient: false);
    }

    public ValueTask DisposeAsync()
    {
        // The StorageClient wraps an HttpClient; the sink owns whichever client it materialized.
        _client?.Dispose();
        _client = null;
        return ValueTask.CompletedTask;
    }

    /// <summary>The validated format plus the final object name, per the replace/append naming
    /// convention shared with the other object-store sinks: "replace" is a stable name
    /// (<c>&lt;output&gt;.&lt;format&gt;</c>); "append" lands under a run-unique guid-suffixed name
    /// instead so repeated runs accumulate objects.</summary>
    private static (FileFormat Format, string ObjectName) ResolveObjectName(OutputSpec spec)
    {
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, null, "gcs", context);
        FileFormatCatalog.EnsureWritable(format, "gcs", context);
        var objectName = string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? $"{spec.Output}-{Guid.NewGuid():N}.{format.Extension}"
            : $"{spec.Output}.{format.Extension}";
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
