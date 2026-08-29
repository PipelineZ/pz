using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Sftp;

internal enum SftpWriteSessionState { Open, Committed, Aborted }

/// <summary>Temp-upload + rename-promote commit protocol over one <see cref="ISftpFileSystem"/>. The
/// session owns the fs when <c>ownsFileSystem</c> (single-output sessions); a partitioned fan-out
/// shares one fs across inner sessions and disposes it itself. Commit: close the format writer
/// (flushes the remote temp file), delete any existing final file, rename temp → final (SFTP rename
/// does not overwrite portably, hence delete-first), then best-effort sweep stale
/// '.pz-tmp-*-&lt;final&gt;' from dead attempts. Abort: close writer, best-effort delete the temp.
/// Rename is gated non-idempotent (a repeated rename after success would throw path-not-found and
/// could mask a landed commit); open/delete are gated idempotent.</summary>
internal abstract class SftpWriteSessionBase(
    ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath,
    IOperationGate? gate, string context) : ISinkWriteSession
{
    private SftpWriteSessionState _state = SftpWriteSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;

    protected string TempPath => tempPath;

    /// <summary>Routes one discrete write-session op through the engine-supplied gate when present; with
    /// no gate, calls straight through. Classification into PzConnectorException happens INSIDE
    /// <paramref name="op"/>, so the gate always sees fully-classified transient/permanent exceptions.
    /// idempotent is per-op: open/delete are repeat-safe before data flows or after it is gone; a
    /// repeated rename after a landed commit would throw path-not-found and could mask the commit, so
    /// commit_rename alone runs non-idempotent.</summary>
    private protected static Task<T> GatedAsync<T>(IOperationGate? gate, string opLabel, bool idempotent,
        Func<CancellationToken, Task<T>> op, CancellationToken ct)
        => gate is null ? op(ct) : gate.ExecuteAsync(opLabel, idempotent, op, ct);

    /// <summary>Opens the temp file's write stream, gated idempotent (create/truncate is repeat-safe
    /// before any data flows) and shared by every format session's <c>CreateAsync</c>.</summary>
    private protected static Task<Stream> OpenWriteAsync(
        ISftpFileSystem fs, string tempPath, IOperationGate? gate, string context, CancellationToken ct) =>
        GatedAsync(gate, "sftp.open_write", idempotent: true, _ =>
        {
            try
            {
                return Task.FromResult(fs.OpenWrite(tempPath));
            }
            catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
            {
                throw SftpErrors.Map(ex, $"{context}: open '{tempPath}' failed");
            }
        }, ct);

    /// <summary>Best-effort delete of the temp file — shared by Abort, the implicit
    /// dispose-without-commit abort, and a failed <c>CreateAsync</c>'s cleanup. Suppressed by design:
    /// never mask an earlier failure (or, on the implicit-abort path, nothing at all) with cleanup
    /// fallout.</summary>
    private protected static async Task DeleteTempBestEffortAsync(
        ISftpFileSystem fs, string tempPath, IOperationGate? gate, CancellationToken ct)
    {
        try
        {
            await GatedAsync(gate, "sftp.delete_temp", idempotent: true,
                _ =>
                {
                    fs.Delete(tempPath);
                    return Task.FromResult(true);
                }, ct).ConfigureAwait(false);
        }
        catch
        {
            // Suppressed by design: never mask an earlier failure with cleanup fallout.
        }
    }

    /// <summary>Cleans up a failed <c>CreateAsync</c>: disposes the half-open stream (if the failure
    /// happened after open) and best-effort deletes the temp file, then the caller rethrows the
    /// original failure unchanged — the Azure <c>AzureWriteSession</c> CreateAsync shape.</summary>
    private protected static async Task CleanupFailedCreateAsync(
        Stream? stream, ISftpFileSystem fs, string tempPath, IOperationGate? gate)
    {
        if (stream is not null)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }

        await DeleteTempBestEffortAsync(fs, tempPath, gate, CancellationToken.None).ConfigureAwait(false);
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        try
        {
            await WriteBatchCoreAsync(batch, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw SftpErrors.Map(ex, $"{context}: write to '{tempPath}' failed");
        }

        _rowsWritten += batch.Length;
        _batchesWritten++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;

        await CloseWriterAsync().ConfigureAwait(false);
        await GatedAsync(gate, "sftp.commit_rename", idempotent: false, _ =>
        {
            try
            {
                if (fs.FileExists(finalPath))
                {
                    fs.Delete(finalPath);
                }

                fs.Rename(tempPath, finalPath);
                return Task.FromResult(true);
            }
            catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
            {
                throw SftpErrors.Map(ex, $"{context}: commit rename to '{finalPath}' failed");
            }
        }, ct).ConfigureAwait(false);

        SweepStaleTemps();   // best-effort, suppressed failures — never mask a landed commit

        _state = SftpWriteSessionState.Committed;
        return new WriteResult(_rowsWritten, _batchesWritten);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        await CloseWriterAsync().ConfigureAwait(false);
        await DeleteTempBestEffortAsync(fs, tempPath, gate, ct).ConfigureAwait(false);

        _state = SftpWriteSessionState.Aborted;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_state != SftpWriteSessionState.Open)
            {
                return;
            }

            if (_commitAttempted)
            {
                // Commit was attempted (and threw) — per Commit-xor-Abort this must NOT count as an
                // implicit abort. Just release local resources; the temp file's fate is unknown (the
                // rename may or may not have happened) so it is deliberately left alone.
                await CloseWriterAsync().ConfigureAwait(false);
                return;
            }

            await CloseWriterAsync().ConfigureAwait(false);
            await DeleteTempBestEffortAsync(fs, tempPath, gate, CancellationToken.None).ConfigureAwait(false);
            _state = SftpWriteSessionState.Aborted;
        }
        finally
        {
            // Only single-output sessions own the fs; a partitioned fan-out (Task 9) shares one fs
            // across inner sessions and disposes it itself once, after every inner session is done.
            if (ownsFileSystem)
            {
                fs.Dispose();
            }
        }
    }

    protected abstract ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Flushes and releases the underlying writer/stream. Idempotent — may be called more than
    /// once (Commit/Abort followed by Dispose).</summary>
    protected abstract ValueTask CloseWriterAsync();

    /// <summary>Deletes sibling '.pz-tmp-*-&lt;finalFileName&gt;' files left behind by dead attempts (a
    /// prior run that opened a temp and never reached Commit/Abort — e.g. a killed process). Never
    /// gated: this is housekeeping, not a discrete remote op the engine needs to pace/retry against
    /// <see cref="IOperationGate"/>'s three recorded labels. Runs only after a landed rename, so any
    /// failure here — listing or an individual delete — must never surface.</summary>
    private void SweepStaleTemps()
    {
        try
        {
            var (dir, finalFileName) = SplitPath(finalPath);
            foreach (var candidate in fs.ListFiles(dir, recursive: false))
            {
                if (candidate == tempPath)
                {
                    continue;
                }

                var (_, name) = SplitPath(candidate);
                if (!name.StartsWith(".pz-tmp-", StringComparison.Ordinal) ||
                    !name.EndsWith($"-{finalFileName}", StringComparison.Ordinal))
                {
                    continue;
                }

                try
                {
                    fs.Delete(candidate);
                }
                catch
                {
                    // Suppressed by design, per candidate: one stale temp's delete failure must not
                    // stop the sweep of the others.
                }
            }
        }
        catch
        {
            // Suppressed by design: never mask a landed commit with cleanup fallout.
        }
    }

    private void EnsureOpen(string action)
    {
        if (_state != SftpWriteSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }

    /// <summary>Splits a '/'-separated remote path into its directory (empty string when the path has
    /// no directory component) and file name.</summary>
    private static (string Dir, string Name) SplitPath(string path)
    {
        var slash = path.LastIndexOf('/');
        return slash < 0 ? ("", path) : (path[..slash], path[(slash + 1)..]);
    }

    /// <summary>A temp path alongside <paramref name="finalPath"/> following the naming convention
    /// <see cref="SweepStaleTemps"/> recognizes: same directory, unique per call, ending in the final
    /// file's own name so a dead attempt's leftover is identifiable (and a different output's temp is
    /// not) purely from the file name.</summary>
    internal static string MakeTempPath(string finalPath)
    {
        var (dir, name) = SplitPath(finalPath);
        var tempName = $".pz-tmp-{Guid.NewGuid():N}-{name}";
        return dir.Length == 0 ? tempName : $"{dir}/{tempName}";
    }
}

/// <summary>Csv write session: a <see cref="CsvWriteCodec"/> opened once over the temp file's write
/// stream, header written at session creation, rows appended per received batch — the toolkit's shared
/// codec, the same writer LocalFiles/azure use, so csv output stays byte-identical across
/// connectors.</summary>
internal sealed class SftpCsvWriteSession : SftpWriteSessionBase
{
    private CsvWriteCodec? _writer;
    private bool _closed;

    private SftpCsvWriteSession(ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath,
        IOperationGate? gate, string context, CsvWriteCodec writer)
        : base(fs, ownsFileSystem, tempPath, finalPath, gate, context) => _writer = writer;

    internal static async Task<SftpCsvWriteSession> CreateAsync(
        ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath, Schema schema,
        IOperationGate? gate, string context, CancellationToken ct)
    {
        Stream? stream = null;
        try
        {
            stream = await OpenWriteAsync(fs, tempPath, gate, context, ct).ConfigureAwait(false);
            var writer = new CsvWriteCodec(stream, schema, context);
            return new SftpCsvWriteSession(fs, ownsFileSystem, tempPath, finalPath, gate, context, writer);
        }
        catch
        {
            await CleanupFailedCreateAsync(stream, fs, tempPath, gate).ConfigureAwait(false);
            throw;
        }
    }

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

/// <summary>NDJSON write session: one JSON object per row, LF-framed with a trailing newline, via the
/// shared toolkit codec (the same writer LocalFiles/azure/http use), so output stays byte-identical
/// across connectors. The Arrow schema travels inside each <see cref="RecordBatch"/>, so unlike the
/// csv/parquet sessions no schema is captured up front.</summary>
internal sealed class SftpJsonWriteSession : SftpWriteSessionBase
{
    private Stream? _stream;
    private bool _closed;

    private SftpJsonWriteSession(ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath,
        IOperationGate? gate, string context, Stream stream)
        : base(fs, ownsFileSystem, tempPath, finalPath, gate, context) => _stream = stream;

    internal static async Task<SftpJsonWriteSession> CreateAsync(
        ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath,
        IOperationGate? gate, string context, CancellationToken ct)
    {
        Stream? stream = null;
        try
        {
            stream = await OpenWriteAsync(fs, tempPath, gate, context, ct).ConfigureAwait(false);
            return new SftpJsonWriteSession(fs, ownsFileSystem, tempPath, finalPath, gate, context, stream);
        }
        catch
        {
            await CleanupFailedCreateAsync(stream, fs, tempPath, gate).ConfigureAwait(false);
            throw;
        }
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

/// <summary>Parquet write session (Parquet.Net): one row group per received <see cref="RecordBatch"/>.
/// v0 type support: int32/int64/double/utf8/bool/date32/timestamp; decimal128 is a permanent failure
/// naming the column. Unlike LocalFiles/Azure — where a native/COPY tier can still write decimal — sftp
/// has no native tier at all, so there is no fallback route to point at: the fix is a format change
/// (csv/json) or an upstream cast, not a different pz tier.</summary>
internal sealed class SftpParquetWriteSession : SftpWriteSessionBase
{
    private readonly DataField[] _fields;
    private Stream? _stream;
    private ParquetWriter? _writer;
    private bool _closed;

    private SftpParquetWriteSession(ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath,
        IOperationGate? gate, string context, DataField[] fields, Stream stream, ParquetWriter writer)
        : base(fs, ownsFileSystem, tempPath, finalPath, gate, context)
    {
        _fields = fields;
        _stream = stream;
        _writer = writer;
    }

    internal static async Task<SftpParquetWriteSession> CreateAsync(
        ISftpFileSystem fs, bool ownsFileSystem, string tempPath, string finalPath, Schema arrowSchema,
        IOperationGate? gate, string context, CancellationToken ct)
    {
        // Validated before any remote resource is touched, so a decimal column fails fast without
        // ever opening (and having to clean up) a temp file.
        var fields = arrowSchema.FieldsList.Select(BuildDataField).ToArray();
        var parquetSchema = new ParquetSchema(fields);

        Stream? stream = null;
        try
        {
            stream = await OpenWriteAsync(fs, tempPath, gate, context, ct).ConfigureAwait(false);
            var writer = await ParquetWriter.CreateAsync(parquetSchema, stream, cancellationToken: ct).ConfigureAwait(false);
            return new SftpParquetWriteSession(fs, ownsFileSystem, tempPath, finalPath, gate, context, fields, stream, writer);
        }
        catch
        {
            await CleanupFailedCreateAsync(stream, fs, tempPath, gate).ConfigureAwait(false);
            throw;
        }
    }

    // Lockstep copy of LocalFilesSink.ParquetSinkWriteSession.BuildDataField
    // (connectors/Pz.Connector.LocalFiles/LocalFilesSink.cs) except the decimal message: sftp has no
    // native COPY path to send decimal writers to, so it names a format/cast workaround instead. Keep
    // the two mappings in sync if the v0 type matrix changes.
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
            $"column '{field.Name}': the sftp sink cannot write decimal parquet columns — " +
            "use csv or json format instead, or cast the column upstream", isTransient: false),
        _ => throw new NotSupportedException(
            $"sftp parquet sink v0 does not support Arrow type '{field.DataType}' for column '{field.Name}'"),
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
                $"sftp parquet sink v0 does not support array type '{array.GetType()}' for column '{field.Name}'"),
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
