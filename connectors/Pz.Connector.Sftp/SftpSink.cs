using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Shared.Sinks;

namespace Pz.Connector.Sftp;

/// <summary>Sftp sink: universal-tier-only write dispatch (csv/json/parquet, via
/// <see cref="SftpWriteSessionBase"/>'s three format sessions), replace/append naming (the localfiles
/// convention: stable <c>&lt;output&gt;.&lt;ext&gt;</c> for replace,
/// guid-suffixed for append), and single-column <c>partition_by</c> fan-out
/// (<see cref="PartitionedWriteSession"/>). SFTP has no native tier in either direction (see
/// <see cref="SftpConnector"/>'s doc comment), so <see cref="TryGetNativeCopy"/> always returns
/// false. <paramref name="connect"/> is the unit-test seam (production wires
/// <c>SftpClientFactory.Open</c>), the same shape as <see cref="SftpSource"/>'s.
///
/// GatedOperations: the engine calls <see cref="UseOperationGate"/> exactly once after this sink is
/// opened, before any <see cref="BeginWriteAsync"/> call. The stored gate threads into every session
/// opened below -- the single-output session and each per-folder inner session a fan-out opens
/// lazily -- so every discrete write-session op (sftp.open_write/sftp.commit_rename/sftp.delete_temp)
/// routes through it.</summary>
internal sealed class SftpSink(SftpConnectionSettings settings, Func<SftpConnectionSettings, ISftpFileSystem> connect)
    : ISink, ISpoolingSink, IOperationGateAware, INoticeAware
{
    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    /// <summary>The engine calls this at most once per open (see the interface doc), so no dedup is
    /// needed here even though a partitioned fan-out write may open several physical connections.</summary>
    public void UseNotice(Action<string> notice)
    {
        if (settings.HostKeyFingerprint is null)
        {
            notice(SftpHostKeyNotice.Unpinned);
        }
    }

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        // DuckDB cannot write to an SFTP server; TryGetSpool below is how DuckDB's writers reach it.
        copy = null;
        return false;
    }

    /// <summary>Every output spools: DuckDB writes each file locally in the output's format (parquet with
    /// nested columns, xlsx, json <c>layout: array</c> included, since the excel writer's crash on a failed
    /// remote write cannot happen on a local file), and <see cref="CommitSpoolAsync"/> uploads it.
    /// Resolved here, so a bad format is refused at plan time (PZ0361).</summary>
    public bool TryGetSpool(OutputSpec spec, [NotNullWhen(true)] out SpoolPlan? plan)
    {
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "parquet", "sftp", context);
        FileFormatCatalog.EnsureWritable(format, "sftp", context);
        var partitionBy = PartitionColumns.Read(spec.Options) is [var column] ? column : null;
        plan = new SpoolPlan(FileFormatCatalog.CopyClause(format, spec.Options, context), format.Extension)
        {
            Mechanism = $"{format.Name} upload",
            SetupStatements = FileFormatCatalog.SetupStatements(format),
            NestedAsJson = FileFormatCatalog.NestedAsJson(format),
            PartitionColumn = partitionBy,
            PathTemplate = partitionBy is null ? null : Str(spec.Options, "path") ?? "",
        };
        return true;
    }

    /// <summary>Uploads each spooled file over one SSH connection, temp name then rename, into the folder the
    /// Arrow path would have used: the output directory, or <c>&lt;root&gt;/&lt;rendered folder&gt;</c> for a
    /// partitioned output, under the same replace/append object name.</summary>
    public async ValueTask<WriteResult> CommitSpoolAsync(OutputSpec spec, IReadOnlyList<SpooledFile> files, CancellationToken ct)
    {
        var context = $"output '{spec.Output}'";
        var format = FileFormatCatalog.Resolve(spec.Options, "parquet", "sftp", context);
        var objectName = ResolveObjectName(spec, format);
        var partitioned = PartitionColumns.Read(spec.Options).Count > 0;
        using var fs = connect(settings);
        long rows = 0;
        foreach (var file in files)
        {
            var trimmed = file.Folder.Trim('/');
            var dir = !partitioned ? SftpPaths.ResolveOutputDir(settings.Root, spec)
                : trimmed.Length > 0 ? SftpPaths.Join(settings.Root, trimmed) : settings.Root;
            if (!string.IsNullOrEmpty(dir))
            {
                CreateDirectories(fs, dir, spec.Output);
            }

            var finalPath = string.IsNullOrEmpty(dir) ? objectName : $"{dir}/{objectName}";
            await using var session = SftpFileWriteSession.Create(
                fs, SftpWriteSessionBase.MakeTempPath(finalPath), finalPath, _gate, context);
            await session.UploadAsync(file.LocalPath, file.Rows, ct).ConfigureAwait(false);
            rows += (await session.CommitAsync(ct).ConfigureAwait(false)).RowsWritten;
        }

        return new WriteResult(rows, files.Count);
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var format = ResolveFormat(spec);
        var objectName = ResolveObjectName(spec, format);
        var delimiter = FileFormatCatalog.Delimiter(format, spec.Options, $"output '{spec.Output}'");

        // Exactly one column: PathTemplate.Render (used both here and by PartitionedWriteSession)
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
    /// partition column (present + timestamp/date-typed) BEFORE opening any resource, so a bad
    /// <c>partition_by</c> fails without ever dialing the SSH connection. One
    /// <see cref="ISftpFileSystem"/> is opened here for the whole fan-out: a single serialized SSH channel
    /// shared by every inner session (<c>ownsFileSystem: false</c>), so folders are written one at a
    /// time and <see cref="PartitionedWriteSession"/> disposes the file system once, after every inner
    /// session's own cleanup has run. Each inner session lands at
    /// <c>&lt;root&gt;/&lt;renderedFolder&gt;/&lt;objectName&gt;</c>, sharing the exact object name the
    /// single-output path uses so replace/append naming is identical per folder.</summary>
    private ISinkWriteSession BeginPartitionedWrite(
        OutputSpec spec, Schema schema, FileFormat format, char delimiter, string objectName, string partitionBy,
        CancellationToken ct)
    {
        var partitionColIndex = PartitionedWriteSession.ResolvePartitionColumn(spec.Output, schema, partitionBy);

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

        return new PartitionedWriteSession(Open, pathTemplate, partitionColIndex, schema, maxConcurrentWrites: 1, sharedResource: fs);
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

    /// <summary>The final file name a write would commit to, per the localfiles replace/append naming
    /// convention: "replace" is a stable name
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
