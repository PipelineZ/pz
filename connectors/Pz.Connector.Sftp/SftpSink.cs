using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Sftp;

/// <summary>Sftp sink: universal-tier-only write dispatch (csv/json/parquet, via
/// <see cref="SftpWriteSessionBase"/>'s three format sessions), replace/append naming (transcribed
/// from <c>LocalFilesSink</c>'s convention: stable <c>&lt;output&gt;.&lt;ext&gt;</c> for replace,
/// guid-suffixed for append), and single-column <c>partition_by</c> fan-out
/// (<see cref="SftpPartitionedWriteSession"/>). SFTP has no native tier in either direction (see
/// <see cref="SftpConnector"/>'s doc comment), so <see cref="TryGetNativeCopy"/> always returns
/// false. <paramref name="connect"/> is the unit-test seam (production wires
/// <c>SftpClientFactory.Open</c>), mirroring <see cref="SftpSource"/>'s factory shape.
///
/// GatedOperations: the engine calls <see cref="UseOperationGate"/> exactly once after this sink is
/// opened, before any <see cref="BeginWriteAsync"/> call. The stored gate threads into every session
/// opened below -- the single-output session and each per-folder inner session a fan-out opens
/// lazily -- so every discrete write-session op (sftp.open_write/sftp.commit_rename/sftp.delete_temp)
/// routes through it.</summary>
internal sealed class SftpSink(SftpConnectionSettings settings, Func<SftpConnectionSettings, ISftpFileSystem> connect)
    : ISink, IOperationGateAware
{
    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        // sftp has no native tier in either direction, but the format must still be resolved here so an
        // invalid format or a native-only option (e.g. json layout: array) is refused at PLAN time
        // (PZ0361) rather than surfacing only when BeginWriteAsync runs at execute time.
        ResolveFormat(spec);
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var format = ResolveFormat(spec);
        var objectName = ResolveObjectName(spec, format);
        var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, $"output '{spec.Output}'");

        // Exactly one column: PathTemplate.Render (used both here and by SftpPartitionedWriteSession)
        // renders one timestamp per row into a folder, and DagCompiler's PZ0219 has already refused a
        // multi-column partition_by against a templated path.
        if (PartitionColumns.Read(spec.Options) is [var partitionBy])
        {
            return BeginPartitionedWrite(spec, schema, format, delimiter, objectName, partitionBy, ct);
        }

        var outputDir = SftpPaths.ResolveOutputDir(settings.Root, spec);
        var fs = connect(settings);
        try
        {
            CreateDirectories(fs, outputDir, spec.Output);
            var finalPath = $"{outputDir}/{objectName}";
            var tempPath = SftpWriteSessionBase.MakeTempPath(finalPath);
            return await OpenSessionAsync(fs, ownsFileSystem: true, tempPath, finalPath, format, delimiter, schema,
                $"output '{spec.Output}'", ct).ConfigureAwait(false);
        }
        catch
        {
            fs.Dispose();
            throw;
        }
    }

    /// <summary>Builds the fan-out write session for a <c>partition_by</c> output. Validates the
    /// partition column (present + timestamp/date-typed) BEFORE opening any resource -- transcribed
    /// from <c>AzureSink.ResolvePartitionColumn</c> -- so a bad <c>partition_by</c> fails without ever
    /// dialing the SSH connection. One <see cref="ISftpFileSystem"/> is opened here for the whole
    /// fan-out (a single serialized SSH channel shared by every inner session,
    /// <c>ownsFileSystem: false</c>); <see cref="SftpPartitionedWriteSession"/> disposes it once,
    /// after every inner session's own cleanup has run. Each inner session lands at
    /// <c>&lt;root&gt;/&lt;renderedFolder&gt;/&lt;objectName&gt;</c>, sharing the exact object name the
    /// single-output path uses so replace/append naming is identical per folder.</summary>
    private ISinkWriteSession BeginPartitionedWrite(
        OutputSpec spec, Schema schema, FileFormat format, char delimiter, string objectName, string partitionBy,
        CancellationToken ct)
    {
        var partitionColIndex = ResolvePartitionColumn(spec.Output, schema, partitionBy);

        var fs = connect(settings);
        var pathTemplate = Str(spec.Options, "path") ?? "";

        async ValueTask<ISinkWriteSession> Open(string folder)
        {
            var trimmed = folder.Trim('/');
            var dir = trimmed.Length > 0 ? SftpPaths.Join(settings.Root, trimmed) : settings.Root;
            var finalPath = string.IsNullOrEmpty(dir) ? objectName : $"{dir}/{objectName}";
            var tempPath = SftpWriteSessionBase.MakeTempPath(finalPath);

            if (!string.IsNullOrEmpty(dir))
            {
                CreateDirectories(fs, dir, spec.Output);
            }

            return await OpenSessionAsync(fs, ownsFileSystem: false, tempPath, finalPath, format, delimiter, schema,
                $"output '{spec.Output}'", ct).ConfigureAwait(false);
        }

        return new SftpPartitionedWriteSession(Open, fs, pathTemplate, partitionColIndex, schema);
    }

    /// <summary>Dispatches to the one format-specific session, gate threaded through. Format was
    /// already validated by <see cref="ResolveFormat"/> (writable, and universal-tier-supported), so
    /// the fallback branch is unreachable.</summary>
    private async ValueTask<ISinkWriteSession> OpenSessionAsync(
        ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath, FileFormat format, char delimiter,
        Schema schema, string context, CancellationToken ct) => format.Name switch
    {
        "parquet" => await SftpParquetWriteSession.CreateAsync(fs, ownsFileSystem, tempPath, finalPath, schema, _gate, context, ct)
            .ConfigureAwait(false),
        "csv" or "tsv" => await SftpCsvWriteSession.CreateAsync(
                fs, ownsFileSystem, tempPath, finalPath, schema, delimiter, _gate, context, ct)
            .ConfigureAwait(false),
        "json" => await SftpJsonWriteSession.CreateAsync(fs, ownsFileSystem, tempPath, finalPath, _gate, context, ct)
            .ConfigureAwait(false),
        _ => throw new UnreachableException("unreachable: format already validated by ResolveFormat"),
    };

    /// <summary>mkdir -p, classified through <see cref="SftpErrors.Map"/> like every other fs call --
    /// never gated (mkdir is not one of the three documented op labels; see
    /// <see cref="SftpWriteSessionBase"/>'s doc comment).</summary>
    private static void CreateDirectories(ISftpFileSystem fs, string dir, string output)
    {
        try
        {
            fs.CreateDirectories(dir);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw SftpErrors.Map(ex, $"output '{output}': mkdir '{dir}' failed");
        }
    }

    /// <summary>Runtime fail-fast for the partition column: it must exist in the write schema AND be a
    /// timestamp/date Arrow type. Lockstep copy of <c>AzureSink.ResolvePartitionColumn</c>
    /// (connectors/Pz.Connector.AzureBlob/AzureSink.cs) -- permanent, names the output and column,
    /// thrown before any resource is opened.</summary>
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

    /// <summary>Format resolution, through the shared catalog: default parquet, writable, and (sftp
    /// has no native tier) universal-tier-supported.</summary>
    private static FileFormat ResolveFormat(OutputSpec spec)
    {
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "parquet", "sftp", context);
        FileFormatCatalog.EnsureWritable(format, "sftp", context);
        FileFormatCatalog.EnsureUniversalTierSupported(format, spec.Options, "sftp", context);
        return format;
    }

    /// <summary>The final file name a write would commit to, per the replace/append naming
    /// convention transcribed from <c>LocalFilesSink</c>: "replace" is a stable name
    /// (<c>&lt;output&gt;.&lt;ext&gt;</c>); "append" lands under a run-unique guid-suffixed name
    /// instead so repeated runs accumulate files. Computed once per <see cref="BeginWriteAsync"/> call
    /// so every folder a partitioned fan-out opens shares the exact same object name.</summary>
    private static string ResolveObjectName(OutputSpec spec, FileFormat format) =>
        string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? $"{spec.Output}-{Guid.NewGuid():N}.{format.Extension}"
            : $"{spec.Output}.{format.Extension}";

    private static string? Str(IReadOnlyDictionary<string, object?> options, string key) =>
        options.TryGetValue(key, out var v) ? v?.ToString() : null;
}
