using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.AzureBlob;

/// <summary>Azure sink: native COPY over the DuckDB azure extension and a universal block-blob write
/// session (<see cref="AzureWriteSession"/>). Modes append/replace; merge is not a blob concept.
/// <see cref="ResolveFinalLocation"/> is the single source of truth for the final object name so
/// the native and universal paths always land at the exact same final blob for a given spec.
///
/// GatedOperations: the engine calls <see cref="UseOperationGate"/> exactly
/// once after this sink is opened, before any <see cref="BeginWriteAsync"/> call. The stored gate threads
/// into every <c>AzureXxxWriteSession.CreateAsync</c> call below -- including each per-folder inner
/// session opened lazily by <see cref="BeginPartitionedWrite"/>'s <c>Open</c> closure -- so every discrete
/// write-session op (open_write/commit_copy/delete_temp) routes through it. Native COPY is unaffected: it
/// never opens a session and is out of .NET reach either way.</summary>
internal sealed class AzureSink(ConnectorConfig config) : ISink, IOperationGateAware
{
    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        // Partitioned output fans one blob out per row-date folder -- a single native COPY ... TO cannot
        // express a per-row-value fan-out, so partitioned outputs must take the universal write-session
        // path: partitioned write is universal-tier only.
        if (PartitionColumns.Read(spec.Options).Count > 0)
        {
            copy = null;
            return false;
        }

        var (format, _, loc) = ResolveFinalLocation(spec);
        var context = $"output '{spec.Output}'";
        var secret = AzureAuth.CreateSecretSql(config, AzureAuth.SecretName(spec.Sink));

        copy = new NativeCopy(
            $"copy (select * from {{{{source}}}}) to '{AzureUrl.Escape(AzureUrl.Render(loc))}' ({FileFormatCatalog.CopyClause(format, spec.Options, context)})",
            ["install azure", "load azure", secret, .. FileFormatCatalog.SetupStatements(format)])
        {
            Mechanism = $"COPY TO azure {format.Name}",
        };
        return true;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var (format, objectName, loc) = ResolveFinalLocation(spec);
        var context = $"output '{spec.Output}'";
        FileFormatCatalog.EnsureUniversalTierSupported(format, spec.Options, "azureblob", context);
        var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, context);

        // Partitioned output routes rows into per-day folders; validate the partition column and build a
        // folder->inner-session fan-out session instead of a single-blob session.
        // Exactly one column: this connector renders pz's calendar tokens from a timestamp column, and
        // DagCompiler's PZ0219 has already refused a multi-column partition_by against a templated path.
        if (PartitionColumns.Read(spec.Options) is [var partitionBy])
        {
            return BeginPartitionedWrite(spec, schema, format, delimiter, objectName, loc, partitionBy, ct);
        }

        var container = AzureAuth.CreateBlobContainerClient(config, loc.Container);
        var finalBlobName = loc.Key;
        var tempBlobName = $"{finalBlobName}.pz-tmp-{Guid.NewGuid():N}";

        return format.Name switch
        {
            "parquet" => await AzureParquetWriteSession.CreateAsync(container, tempBlobName, finalBlobName, schema, ct, _gate)
                .ConfigureAwait(false),
            "csv" or "tsv" => await AzureCsvWriteSession.CreateAsync(
                    container, tempBlobName, finalBlobName, schema, delimiter, ct, _gate)
                .ConfigureAwait(false),
            "json" => await AzureJsonWriteSession.CreateAsync(container, tempBlobName, finalBlobName, schema, ct, _gate)
                .ConfigureAwait(false),
            _ => throw new UnreachableException("unreachable: format already validated by EnsureUniversalTierSupported"),
        };
    }

    /// <summary>Builds the fan-out write session for a <c>partition_by</c> output. Validates the partition
    /// column (present + timestamp/date-typed) BEFORE opening any resource -- this cannot be a compile-time
    /// check (PZ0219 only knows path tokens ⇔ partition_by, never the runtime schema), so it mirrors
    /// <c>PostgresSink.BeginWriteAsync</c>'s merge-key pre-check. The returned session opens no
    /// blob until the first row for a folder arrives (each inner session lands at
    /// <c>&lt;renderedFolder&gt;/&lt;objectName&gt;</c>, sharing the exact object name the single-blob path
    /// uses so replace/append naming is identical per folder).</summary>
    private ISinkWriteSession BeginPartitionedWrite(
        OutputSpec spec, Schema schema, FileFormat format, char delimiter, string objectName, AzureLocation loc,
        string partitionBy, CancellationToken ct)
    {
        var partitionColIndex = ResolvePartitionColumn(spec.Output, schema, partitionBy);

        var container = AzureAuth.CreateBlobContainerClient(config, loc.Container);
        var pathTemplate = Str(spec.Options, "path") ?? "";

        async ValueTask<ISinkWriteSession> Open(string folder)
        {
            var trimmed = folder.Trim('/');
            var finalBlobName = trimmed.Length > 0 ? $"{trimmed}/{objectName}" : objectName;
            var tempBlobName = $"{finalBlobName}.pz-tmp-{Guid.NewGuid():N}";

            return format.Name switch
            {
                "parquet" => await AzureParquetWriteSession.CreateAsync(container, tempBlobName, finalBlobName, schema, ct, _gate)
                    .ConfigureAwait(false),
                "csv" or "tsv" => await AzureCsvWriteSession.CreateAsync(
                        container, tempBlobName, finalBlobName, schema, delimiter, ct, _gate)
                    .ConfigureAwait(false),
                "json" => await AzureJsonWriteSession.CreateAsync(container, tempBlobName, finalBlobName, schema, ct, _gate)
                    .ConfigureAwait(false),
                _ => throw new UnreachableException("unreachable: format already validated by EnsureUniversalTierSupported"),
            };
        }

        return new AzurePartitionedWriteSession(Open, pathTemplate, partitionColIndex, schema);
    }

    /// <summary>Runtime fail-fast for the partition column: it must exist in the write schema AND be a
    /// timestamp/date Arrow type. Same shape as <c>PostgresSink.BeginWriteAsync</c>'s merge-key check --
    /// permanent, names the output and column, thrown before any resource is opened.</summary>
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

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>The final blob location (and validated format) a write would commit to, per the
    /// replace/append naming convention shared by <see cref="TryGetNativeCopy"/> and
    /// <see cref="BeginWriteAsync"/>: "replace" is a stable name (<c>&lt;output&gt;.&lt;ext&gt;</c>);
    /// "append" lands under a run-unique guid-suffixed name instead so repeated runs accumulate blobs.
    /// Side-effect-free (computes only) -- called from <see cref="TryGetNativeCopy"/>, which planning
    /// (ExecutionPlanner) also probes.</summary>
    private static (FileFormat Format, string ObjectName, AzureLocation Location) ResolveFinalLocation(OutputSpec spec)
    {
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, null, "azureblob", context);
        FileFormatCatalog.EnsureWritable(format, "azureblob", context);
        FileFormatCatalog.EnsureRemoteWritable(format, "azureblob", context);
        var objectName = string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? $"{spec.Output}-{Guid.NewGuid():N}.{format.Extension}"
            : $"{spec.Output}.{format.Extension}";
        var loc = AzureUrl.ParseSink(spec.Options, context, objectName);
        return (format, objectName, loc);
    }

    private static string? Str(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var v) ? v?.ToString() : null;
}
