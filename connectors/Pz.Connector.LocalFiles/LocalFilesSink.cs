using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.LocalFiles;

/// <summary>LocalFiles sink: parquet (Parquet.Net), a minimal RFC-4180 CSV writer, or NDJSON via the
/// shared toolkit codec, all committed via temp-write + atomic move. The native COPY path
/// (<see cref="TryGetNativeCopy"/>) goes through DuckDB's <c>COPY ... TO</c> — it is also the only
/// route for decimal128 parquet output, which the universal Parquet.Net path cannot write.</summary>
internal sealed class LocalFilesSink(string baseDir) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var context = $"output '{spec.Output}'";
        var format = ResolveFormat(spec);
        FileFormatCatalog.EnsureWritable(format, "localfiles", context);

        var finalPath = ResolveFinalPath(spec);
        var tempPath = Path.Combine(Path.GetDirectoryName(finalPath)!, $".pz-native-{Guid.NewGuid():N}{Path.GetExtension(finalPath)}");
        copy = new NativeCopy(
            $"copy (select * from {{{{source}}}}) to '{EscapeSqlLiteral(tempPath)}' ({FileFormatCatalog.CopyClause(format, spec.Options, context)})",
            FileFormatCatalog.SetupStatements(format))
        {
            Mechanism = $"COPY TO {format.Name}",
            Finalizations = [new FileMove(tempPath, finalPath)],
        };
        return true;
    }

    /// <summary>The final output path a write session would commit to, per the same replace/append
    /// naming convention <see cref="BeginWriteAsync"/> uses — shared so the native COPY path and the
    /// universal write path land at the exact same final name for a given spec. Computes the path only
    /// — never creates the directory: this is called from <see cref="TryGetNativeCopy"/>, which
    /// planning (ExecutionPlanner) also probes, and planning must be side-effect-free. The engine
    /// creates the directory at execution time, right before running the COPY (SinkWriteExecutor).</summary>
    private string ResolveFinalPath(OutputSpec spec)
    {
        var ext = ResolveFormat(spec).Extension;
        var outputDir = ResolveOutputDir(spec);
        var fileName = $"{spec.Output}.{ext}";
        return string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? Path.Combine(outputDir, $"{spec.Output}-{Guid.NewGuid():N}.{ext}")
            : Path.Combine(outputDir, fileName);
    }

    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var context = $"output '{spec.Output}'";
        var format = ResolveFormat(spec);
        FileFormatCatalog.EnsureUniversalTierSupported(format, spec.Options, "localfiles", context);
        var outputDir = ResolveOutputDir(spec);
        Directory.CreateDirectory(outputDir);

        var runGuid = Guid.NewGuid().ToString("N");
        var tempDir = Path.Combine(outputDir, $".pz-tmp-{runGuid}");
        Directory.CreateDirectory(tempDir);

        var fileName = $"{spec.Output}.{format.Extension}";
        var tempFilePath = Path.Combine(tempDir, fileName);

        // Commit protocol: "replace" overwrites the stable final name; "append" lands
        // under a run-unique suffixed name instead so repeated runs accumulate files.
        var finalFilePath = string.Equals(spec.Mode, "append", StringComparison.OrdinalIgnoreCase)
            ? Path.Combine(outputDir, $"{spec.Output}-{runGuid}.{format.Extension}")
            : Path.Combine(outputDir, fileName);

        try
        {
            return format.Name switch
            {
                "parquet" => await ParquetSinkWriteSession.CreateAsync(tempDir, tempFilePath, finalFilePath, schema, ct)
                    .ConfigureAwait(false),
                "csv" => new CsvSinkWriteSession(tempDir, tempFilePath, finalFilePath, schema),
                "json" => new NdjsonSinkWriteSession(tempDir, tempFilePath, finalFilePath),
                _ => throw new UnreachableException(),
            };
        }
        catch
        {
            TryDeleteDir(tempDir);
            throw;
        }
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>The entity names the directory this write lands in, and <c>path:</c> overrides that.
    /// An absolute <c>path:</c> ignores the connection's location entirely.</summary>
    private string ResolveOutputDir(OutputSpec spec)
    {
        var relative = spec.Options.TryGetValue("path", out var value) && value?.ToString() is { Length: > 0 } p
            ? p
            : spec.Output;

        return Path.IsPathRooted(relative) ? relative : Path.Combine(baseDir, relative);
    }

    internal static FileFormat ResolveFormat(OutputSpec spec) =>
        FileFormatCatalog.Resolve(spec.Options, "parquet", "localfiles", $"output '{spec.Output}'");

    internal static void TryDeleteDir(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
            {
                Directory.Delete(dir, recursive: true);
            }
        }
        catch
        {
            // Suppressed by design: never mask an earlier failure with cleanup fallout.
        }
    }

    private sealed class UnreachableException() : Exception("unreachable: format already validated");
}

internal enum LocalFileSessionState { Open, Committed, Aborted }

/// <summary>Shared temp-write + atomic-move commit protocol for every write format.
/// Commit: close the writer, <see cref="File.Move(string, string, bool)"/> into the final path
/// (overwrite), then delete the temp dir. Abort: close the writer, delete the temp dir without
/// moving anything. Mutually exclusive / at-most-once, mirroring the InMemory reference sink.</summary>
internal abstract class LocalFileWriteSessionBase(string tempDir, string tempFilePath, string finalFilePath) : ISinkWriteSession
{
    private LocalFileSessionState _state = LocalFileSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;

    protected string TempFilePath => tempFilePath;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        await WriteBatchCoreAsync(batch, ct).ConfigureAwait(false);
        _rowsWritten += batch.Length;
        _batchesWritten++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;

        await CloseWriterAsync().ConfigureAwait(false);
        File.Move(tempFilePath, finalFilePath, overwrite: true);
        LocalFilesSink.TryDeleteDir(tempDir);

        _state = LocalFileSessionState.Committed;
        return new WriteResult(_rowsWritten, _batchesWritten);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        await CloseWriterAsync().ConfigureAwait(false);
        LocalFilesSink.TryDeleteDir(tempDir);

        _state = LocalFileSessionState.Aborted;
    }

    public async ValueTask DisposeAsync()
    {
        if (_state != LocalFileSessionState.Open)
        {
            return;
        }

        if (_commitAttempted)
        {
            // Commit was attempted (and threw) — per Commit-xor-Abort this must NOT count as an
            // implicit abort. Just release local resources; the temp dir's fate is unknown (the
            // move may or may not have happened) so it is deliberately left alone.
            await CloseWriterAsync().ConfigureAwait(false);
            return;
        }

        await CloseWriterAsync().ConfigureAwait(false);
        LocalFilesSink.TryDeleteDir(tempDir);
        _state = LocalFileSessionState.Aborted;
    }

    protected abstract ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Flushes and releases the underlying writer/file handle. Idempotent — may be called
    /// more than once (Commit/Abort followed by Dispose).</summary>
    protected abstract ValueTask CloseWriterAsync();

    private void EnsureOpen(string action)
    {
        if (_state != LocalFileSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}

/// <summary>Parquet write session (Parquet.Net): one row group per received <see cref="RecordBatch"/>.
/// v0 type support: int32/int64/double/utf8/bool/date32/timestamp; decimal128 is a permanent failure
/// naming the column — the native COPY path is the decimal-capable route.</summary>
internal sealed class ParquetSinkWriteSession : LocalFileWriteSessionBase
{
    private readonly DataField[] _fields;
    private FileStream? _stream;
    private ParquetWriter? _writer;
    private bool _closed;

    private ParquetSinkWriteSession(string tempDir, string tempFilePath, string finalFilePath,
        DataField[] fields, FileStream stream, ParquetWriter writer)
        : base(tempDir, tempFilePath, finalFilePath)
    {
        _fields = fields;
        _stream = stream;
        _writer = writer;
    }

    internal static async Task<ParquetSinkWriteSession> CreateAsync(
        string tempDir, string tempFilePath, string finalFilePath, Schema arrowSchema, CancellationToken ct)
    {
        var fields = arrowSchema.FieldsList.Select(BuildDataField).ToArray();
        var parquetSchema = new ParquetSchema(fields);

        var stream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write);
        try
        {
            var writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: ct).ConfigureAwait(false);
            return new ParquetSinkWriteSession(tempDir, tempFilePath, finalFilePath, fields, stream, writer);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private static DataField BuildDataField(Apache.Arrow.Field field) => field.DataType switch
    {
        Int32Type => new DataField(field.Name, typeof(int), isNullable: true),
        Int64Type => new DataField(field.Name, typeof(long), isNullable: true),
        DoubleType => new DataField(field.Name, typeof(double), isNullable: true),
        BooleanType => new DataField(field.Name, typeof(bool), isNullable: true),
        StringType => new DataField(field.Name, typeof(string), isNullable: true),
        Date32Type => new DateTimeDataField(field.Name, DateTimeFormat.Date, isNullable: true),
        TimestampType => new DateTimeDataField(field.Name, DateTimeFormat.DateAndTime,
            isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true),
        Decimal128Type => throw new PzConnectorException(
            $"column '{field.Name}': the universal parquet write path does not support decimal columns; " +
            "the native COPY path writes them", isTransient: false),
        _ => throw new NotSupportedException(
            $"LocalFiles parquet sink v0 does not support Arrow type '{field.DataType}' for column '{field.Name}'"),
    };

    protected override async ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct)
    {
        using var rowGroup = _writer!.CreateRowGroup();
        for (var i = 0; i < _fields.Length; i++)
        {
            await WriteColumnAsync(rowGroup, _fields[i], batch.Column(i), ct).ConfigureAwait(false);
        }
    }

    private static Task WriteColumnAsync(ParquetRowGroupWriter rowGroup, DataField field, IArrowArray array, CancellationToken ct) =>
        array switch
        {
            Int32Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (int?)null : a.GetValue(i)), cancellationToken: ct),
            Int64Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (long?)null : a.GetValue(i)), cancellationToken: ct),
            DoubleArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (double?)null : a.GetValue(i)), cancellationToken: ct),
            BooleanArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (bool?)null : a.GetValue(i)), cancellationToken: ct),
            Date32Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetDateTime(i)!.Value.Date), cancellationToken: ct),
            TimestampArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetTimestamp(i)!.Value.UtcDateTime), cancellationToken: ct),
            StringArray a => rowGroup.WriteAsync(field, BuildStrings(a)),
            _ => throw new NotSupportedException(
                $"LocalFiles parquet sink v0 does not support array type '{array.GetType()}' for column '{field.Name}'"),
        };

    private static ReadOnlyMemory<T?> BuildNullable<T>(int length, Func<int, T?> selector) where T : struct
    {
        var values = new T?[length];
        for (var i = 0; i < length; i++)
        {
            values[i] = selector(i);
        }

        return values;
    }

    private static List<string?> BuildStrings(StringArray array)
    {
        var values = new List<string?>(array.Length);
        for (var i = 0; i < array.Length; i++)
        {
            values.Add(array.IsNull(i) ? null : array.GetString(i));
        }

        return values;
    }

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;

        if (_writer is not null)
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }

        if (_stream is not null)
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
            _stream = null;
        }
    }
}

/// <summary>Minimal RFC-4180 CSV writer: header row from the Arrow schema, one text line per row,
/// fields containing a comma/quote/newline are quoted with doubled internal quotes, LF line endings.
/// The encoding itself lives in the shared <see cref="CsvWriteCodec"/> (the same writer the azure sink
/// uses), so csv output stays byte-identical across connectors — as NDJSON does via
/// <see cref="NdjsonWriteCodec"/>. This session keeps only the temp-file/commit protocol.</summary>
internal sealed class CsvSinkWriteSession : LocalFileWriteSessionBase
{
    private CsvWriteCodec? _writer;
    private bool _closed;

    internal CsvSinkWriteSession(string tempDir, string tempFilePath, string finalFilePath, Schema schema)
        : base(tempDir, tempFilePath, finalFilePath) =>
        _writer = new CsvWriteCodec(
            // BufferSize 0 disables FileStream's own buffer: the codec already batches whole rows into
            // a 256 KiB pooled buffer, so a second copy through a 4 KiB one buys nothing.
            new FileStream(tempFilePath, new FileStreamOptions
            {
                Mode = FileMode.Create,
                Access = FileAccess.Write,
                BufferSize = 0,
            }),
            schema, "LocalFiles csv sink v0");

    protected override ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct) =>
        _writer!.WriteBatchAsync(batch, ct);

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;

        if (_writer is not null)
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }
    }
}

/// <summary>NDJSON write session: one JSON object per row, LF-framed
/// with a trailing newline, via the shared toolkit codec (<see cref="NdjsonWriteCodec"/> — the same
/// writer the azure and http sinks use), so output stays byte-identical across connectors. The Arrow
/// schema travels inside each <see cref="RecordBatch"/>, so unlike the parquet/csv sessions no schema
/// is captured up front.</summary>
internal sealed class NdjsonSinkWriteSession : LocalFileWriteSessionBase
{
    private FileStream? _stream;
    private bool _closed;

    internal NdjsonSinkWriteSession(string tempDir, string tempFilePath, string finalFilePath)
        : base(tempDir, tempFilePath, finalFilePath)
    {
        _stream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write);
    }

    protected override ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct) =>
        new(NdjsonWriteCodec.WriteAsync(batch, _stream!, ct));

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;

        if (_stream is not null)
        {
            await _stream.FlushAsync().ConfigureAwait(false);
            await _stream.DisposeAsync().ConfigureAwait(false);
            _stream = null;
        }
    }
}
