using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake sink: spool → PUT → COPY → one atomic commit statement. Batches spool to
/// gzip CSV files in a session temp dir (rolled at ~100 MB compressed); CommitAsync PUTs them to a
/// session-scoped temporary stage, COPYs into a temporary staging table, then executes exactly ONE
/// statement against the target (insert / insert overwrite / merge). Snowflake auto-commits DDL,
/// so multi-statement target transactions are never relied on -- only the single commit statement
/// ever touches the target, which is what makes Transactional an honest declaration and abort
/// (drop temp objects + delete spool files) DiscardsAll.</summary>
internal sealed class SnowflakeSink(string connectionString) : ISink
{
    private static readonly string[] SupportedModes = ["append", "replace", "merge"];

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        // Unreachable via pz run (PZ0228/PZ0324 refuse earlier); kept as ABI defense-in-depth.
        if (!SupportedModes.Contains(spec.Mode, StringComparer.Ordinal))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': snowflake sink supports only 'append'/'replace'/'merge' write " +
                $"modes (got '{spec.Mode}')", isTransient: false);
        }

        var isMerge = string.Equals(spec.Mode, "merge", StringComparison.Ordinal);
        if (isMerge)
        {
            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missingKeys = spec.Keys.Where(k => !fieldNames.Contains(k)).ToArray();
            if (missingKeys.Length > 0)
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': merge key column(s) [{string.Join(", ", missingKeys)}] are " +
                    "not present in the write schema", isTransient: false);
            }

            if (fieldNames.Contains(SfDdl.StagingSequenceColumn))
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': column name '{SfDdl.StagingSequenceColumn}' is reserved by " +
                    "the snowflake sink's merge staging -- hint: rename the column in the pipeline SQL",
                    isTransient: false);
            }
        }

        // The entity name is the target -- parsed offline (no connection needed), same discipline as
        // MsDdl.SplitEntity/PgDdl.SplitEntity, so a malformed output name fails before any side effect.
        var (sfSchema, table) = SfDdl.SplitEntity(spec.Output);

        // No connection is opened here: Snowflake's unit of load is a staged file, so the connection
        // is only useful at commit -- see the class doc comment and CommitAsync.
        var spoolDir = Path.Combine(Path.GetTempPath(), "pz-snowflake", Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(spoolDir);

        return new ValueTask<ISinkWriteSession>(
            new SnowflakeSinkWriteSession(connectionString, spec, schema, sfSchema, table, spoolDir));
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal enum SnowflakeSinkSessionState
{
    Open,
    Committed,
    Aborted,
    Disposed,
}

/// <summary>One output's write session. Batches spool to disk (see <see cref="WriteBatchAsync"/>)
/// with no connection open; <see cref="CommitAsync"/> is where the connection, the temp stage, the
/// temp staging table, and the single target statement all happen, in that order, each phase's
/// failure tagged (`stage upload` / `copy into staging` / `commit statement`) so a caller can tell a
/// transfer problem from a SQL-compilation one.</summary>
internal sealed class SnowflakeSinkWriteSession(
    string connectionString, OutputSpec spec, Schema schema, string sfSchema, string table, string spoolDir)
    : ISinkWriteSession
{
    /// <summary>Spool files roll once the CURRENT file's compressed size exceeds this -- "~100 MB
    /// compressed" per the sink's doc comment, checked after each batch rather than mid-write so one
    /// batch is never split across files.</summary>
    private const long RollThresholdBytes = 100L * 1024 * 1024;

    private readonly bool _isMerge = string.Equals(spec.Mode, "merge", StringComparison.Ordinal);
    private readonly List<string> _spoolFiles = [];

    private SnowflakeDbConnection? _connection;
    private string? _stageIdentifier;
    private string? _stagingTableIdentifier;
    private FileStream? _currentFile;
    private GZipStream? _currentGzip;
    private StreamWriter? _currentWriter;
    private int _fileIndex;

    /// <summary>Session-monotonic across every batch and every rolled spool file -- see
    /// <see cref="SfCsv.WriteBatch"/>'s doc comment for why this must be a real written CSV column
    /// rather than a target-side autoincrement. Meaningless (never read) outside merge mode.</summary>
    private long _nextSequence;

    private long _rowsWritten;
    private long _batchesWritten;
    private SnowflakeSinkSessionState _state = SnowflakeSinkSessionState.Open;
    private bool _commitAttempted;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        EnsureCurrentFile();

        // SfCsv.WriteBatch copies every value out of the (engine-owned, call-scoped) batch into the
        // writer's own managed buffers -- nothing from `batch` is retained past this call. For merge
        // mode only, it also appends the trailing session-monotonic sequence column.
        _nextSequence = SfCsv.WriteBatch(batch, _currentWriter!, _isMerge ? _nextSequence : null);
        await _currentWriter!.FlushAsync(ct).ConfigureAwait(false);

        _rowsWritten += batch.Length;
        _batchesWritten++;

        if (_currentFile!.Length > RollThresholdBytes)
        {
            await CloseCurrentFileAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;
        await CloseCurrentFileAsync().ConfigureAwait(false);

        var tag = Guid.NewGuid().ToString("n")[..8];
        var quotedSchema = SfDdl.Quote(sfSchema);
        // Schema-qualified: the sink's connection string sets a default database (`db=`) but no
        // default schema, so an unqualified temp stage/table would have no schema to resolve
        // against.
        var qualifiedStage = $"{quotedSchema}.{SfDdl.Quote($"pz_stage_{tag}")}";
        var qualifiedStaging = $"{quotedSchema}.{SfDdl.Quote($"pz_load_{tag}")}";

        _connection = new SnowflakeDbConnection { ConnectionString = connectionString };
        // Stashed before any DDL runs, so AbortAsync can name the right (schema-qualified) temp
        // objects if it is ever reached with a connection already open -- see its doc comment for why
        // that never happens via pz today.
        _stageIdentifier = qualifiedStage;
        _stagingTableIdentifier = qualifiedStaging;
        try
        {
            await _connection.OpenAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Wrap(ex, "commit statement");
        }

        // 1. Target table: create it if missing, or enforce schema_policy against what's already
        // there. See SfDdl.EnsureTargetAsync's doc comment for exactly what 'evolve' and
        // 'fail_on_change' mean here.
        try
        {
            await SfDdl.EnsureTargetAsync(_connection, spec.SchemaPolicy, sfSchema, table, schema, spec.Output, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Wrap(ex, "commit statement");
        }

        // 2. Session-scoped temp stage, schema-qualified (see above). Dies with the connection, so
        // AbortAsync/DisposeAsync need no explicit drop on the paths that never reach here.
        await ExecuteAsync(SfDdl.BuildCreateStageSql(qualifiedStage), "commit statement", ct).ConfigureAwait(false);

        // 3. One PUT per spool file.
        foreach (var file in _spoolFiles)
        {
            await ExecuteAsync(SfDdl.BuildPutSql(file, qualifiedStage), "stage upload", ct).ConfigureAwait(false);
        }

        // 4. Temp staging table, schema-qualified the same way.
        await ExecuteAsync(
            SfDdl.BuildCreateStagingTableSql(qualifiedStaging, schema, _isMerge), "commit statement", ct)
            .ConfigureAwait(false);

        // 5. COPY into staging.
        await ExecuteAsync(
            SfDdl.BuildCopyIntoStagingSql(qualifiedStaging, qualifiedStage, schema, _isMerge),
            "copy into staging", ct).ConfigureAwait(false);

        // 6. THE one statement that ever touches the target -- see the class doc comment.
        var targetSql = spec.Mode switch
        {
            "append" => SfDdl.BuildInsertSql(sfSchema, table, qualifiedStaging, schema),
            "replace" => SfDdl.BuildInsertOverwriteSql(sfSchema, table, qualifiedStaging, schema),
            "merge" => SfDdl.BuildMergeSql(sfSchema, table, qualifiedStaging, schema, spec.Keys),
            _ => throw new InvalidOperationException("unreachable: mode validated in BeginWriteAsync"),
        };
        await ExecuteAsync(targetSql, "commit statement", ct).ConfigureAwait(false);

        _state = SnowflakeSinkSessionState.Committed;
        return new WriteResult(_rowsWritten, _batchesWritten);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");
        _state = SnowflakeSinkSessionState.Aborted;
        await CloseCurrentFileAsync().ConfigureAwait(false);
        DeleteSpoolDir();

        // Only reachable if a future change opens the connection outside CommitAsync; today Abort is
        // always called before any connection exists (mutually exclusive with Commit, which is the
        // only place one is opened, and the ABI forbids Abort after a Commit attempt regardless of
        // outcome). Best-effort: session-temporary objects die with the session regardless, so a
        // failure here changes nothing observable.
        if (_connection is not null && _stagingTableIdentifier is not null && _stageIdentifier is not null)
        {
            await BestEffortDropAsync($"drop table if exists {_stagingTableIdentifier}", ct).ConfigureAwait(false);
            await BestEffortDropAsync($"drop stage if exists {_stageIdentifier}", ct).ConfigureAwait(false);
        }
    }

    private async Task BestEffortDropAsync(string sql, CancellationToken ct)
    {
        try
        {
            await using var command = _connection!.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort cleanup only -- never masks the real (abort) outcome.
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CloseCurrentFileAsync().ConfigureAwait(false);
        if (_connection is not null)
        {
            await _connection.DisposeAsync().ConfigureAwait(false);
        }

        DeleteSpoolDir();

        // Only move a still-Open session: Dispose after a successful Commit/Abort keeps that
        // (already-accurate) terminal state, so EnsureOpen's message still names the real reason.
        // A session disposed WITHOUT either -- an abnormal path -- must stop accepting calls too;
        // leaving it Open would let a later WriteBatchAsync run past resources this method just
        // disposed instead of failing with a clear, named InvalidOperationException.
        if (_state == SnowflakeSinkSessionState.Open)
        {
            _state = SnowflakeSinkSessionState.Disposed;
        }
    }

    private void EnsureCurrentFile()
    {
        if (_currentWriter is not null)
        {
            return;
        }

        var path = Path.Combine(spoolDir, $"part_{_fileIndex++:D5}.csv.gz");
        _currentFile = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        _currentGzip = new GZipStream(_currentFile, CompressionLevel.Fastest, leaveOpen: true);
        _currentWriter = new StreamWriter(_currentGzip, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false))
        {
            NewLine = "\n",
        };
        _spoolFiles.Add(path);
    }

    private async ValueTask CloseCurrentFileAsync()
    {
        if (_currentWriter is null)
        {
            return;
        }

        await _currentWriter.DisposeAsync().ConfigureAwait(false);
        await _currentGzip!.DisposeAsync().ConfigureAwait(false);
        await _currentFile!.DisposeAsync().ConfigureAwait(false);
        _currentWriter = null;
        _currentGzip = null;
        _currentFile = null;
    }

    private void DeleteSpoolDir()
    {
        try
        {
            if (Directory.Exists(spoolDir))
            {
                Directory.Delete(spoolDir, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort tmp cleanup only -- never masks the real outcome.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort tmp cleanup only -- never masks the real outcome.
        }
    }

    private async Task ExecuteAsync(string sql, string phase, CancellationToken ct)
    {
        try
        {
            await using var command = _connection!.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not PzConnectorException and not OperationCanceledException)
        {
            throw Wrap(ex, phase);
        }
    }

    private PzConnectorException Wrap(Exception ex, string phase) => new(
        $"output '{spec.Output}': {phase} failed: {ex.Message}", SfErrors.IsTransient(ex), innerException: ex);

    private void EnsureOpen(string action)
    {
        if (_state != SnowflakeSinkSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }

        if (_commitAttempted)
        {
            throw new InvalidOperationException($"cannot {action} a session whose commit was already attempted");
        }
    }
}
