using Apache.Arrow;
using Npgsql;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Postgres;

/// <summary>Postgres sink (append/replace/merge). Output options: <c>schema</c>
/// (default <c>public</c>), <c>table</c> (default = the output's name). Session shape: open
/// an <see cref="NpgsqlConnection"/>, begin one transaction, ensure the target exists (creating it from
/// the Arrow schema via <see cref="PgDdl"/>'s DDL map -- for merge, WITH a <c>unique(keys)</c> constraint
/// -- or enforcing <c>schema_policy</c> [and, for merge, verifying a pre-existing unique constraint on
/// keys] against an existing one), then create a connection-scoped <c>TEMP TABLE ... (LIKE target)</c>.
/// Writes stream via one lazily-opened <c>NpgsqlBinaryImporter</c> (COPY ... FROM STDIN BINARY) held
/// across every batch. Append COPYs DIRECTLY into the target (the temp indirection measured as double
/// the server-side write work -- COPY into temp plus the finalize INSERT each cost about as much as one
/// direct COPY; MVCC keeps concurrent readers on the old rows until COMMIT either way, and a rollback
/// discards the COPY identically). Replace and merge
/// still stage into a connection-scoped <c>TEMP TABLE ... (LIKE target)</c>: replace because
/// truncate-early-then-COPY-direct would take the ACCESS EXCLUSIVE lock at first write and BLOCK
/// concurrent readers for the whole load (the temp path preserves "readers see the old data until
/// COMMIT"), merge because the staged rows feed the dedup + ON CONFLICT statement. Commit completes
/// the importer, finalizes per mode (append: nothing; replace: TRUNCATE + INSERT ... SELECT from
/// temp; merge: INSERT ... SELECT ... ON CONFLICT (keys) DO UPDATE SET &lt;non-key columns&gt; =
/// excluded.&lt;non-key columns&gt;, or DO NOTHING when there are no non-key columns),
/// same transaction, then commits the transaction. Abort (or dispose without a commit attempt)
/// disposes the importer without completing it (aborting the COPY) and rolls the transaction back.
/// <see cref="PostgresSinkWriteSession"/> also implements <see
/// cref="IDeleteApplyingWriteSession"/> (<see cref="ConnectorCapabilities.ApplyDeletes"/>) -- hard/soft
/// delete-key application in the same transaction as the merge, applied after the upsert.</summary>
internal sealed class PostgresSink(string connectionString) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        // Universal path only in v0 -- no native COPY-based sink path exists yet for postgres.
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        // Unreachable via pz run (PZ0228/PZ0324 refuse earlier); kept as ABI defense-in-depth.
        if (spec.Mode is not ("append" or "replace" or "merge"))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': postgres sink supports only 'append'/'replace'/'merge' write " +
                $"modes (got '{spec.Mode}')",
                isTransient: false);
        }

        if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
        {
            // Fail fast, before any connection/resource is opened: a batch schema lacking a declared key
            // column can never be merged, and there is nothing cheaper to check first.
            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missingKeys = spec.Keys.Where(k => !fieldNames.Contains(k)).ToArray();
            if (missingKeys.Length > 0)
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': merge key column(s) [{string.Join(", ", missingKeys)}] are " +
                    "not present in the write schema",
                    isTransient: false);
            }
        }

        // The entity name is the target.
        var (pgSchema, table) = PgDdl.SplitEntity(spec.Output);

        var connection = new NpgsqlConnection(connectionString);
        NpgsqlTransaction? tx = null;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            tx = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

            await PgDdl.EnsureTargetAsync(
                    connection, tx, spec.SchemaPolicy, pgSchema, table, schema, spec.Output, spec.Mode, spec.Keys,
                    spec.OnDelete, ct)
                .ConfigureAwait(false);

            var quotedTarget = $"{PgDdl.Quote(pgSchema)}.{PgDdl.Quote(table)}";
            string? tempTable = null;
            if (!string.Equals(spec.Mode, "append", StringComparison.Ordinal))
            {
                // replace/merge stage into a temp table; append COPYs directly into the target --
                // see the class doc comment for why the split lands exactly here.
                tempTable = $"pz_tmp_{Guid.NewGuid().ToString("N")[..8]}";
                await using var create = new NpgsqlCommand(
                    $"create temp table {PgDdl.Quote(tempTable)} (like {quotedTarget})", connection, tx);
                await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            return new PostgresSinkWriteSession(
                connection, tx, quotedTarget, tempTable, schema, spec.Mode, spec.Keys, spec.OnDelete);
        }
        catch (NpgsqlException ex)
        {
            await CleanupAsync(tx, connection).ConfigureAwait(false);
            throw new PzConnectorException(
                $"output '{spec.Output}': postgres sink open failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
        catch
        {
            await CleanupAsync(tx, connection).ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask CleanupAsync(NpgsqlTransaction? tx, NpgsqlConnection connection)
    {
        if (tx is not null)
        {
            await tx.DisposeAsync().ConfigureAwait(false);
        }

        await connection.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => default;
}

internal enum PgSinkSessionState
{
    Open,
    Committed,
    Aborted,
}

/// <summary>One write session's <c>NpgsqlBinaryImporter</c> lifecycle -- COPYing into a
/// staged temp table for replace/merge, or directly into the target for append (class doc). Commit-
/// xor-abort, mirroring <see cref="Pz.Connectors.TestKit.Reference.InMemorySink"/>'s
/// <c>_commitAttempted</c> pattern: once <see cref="CommitAsync"/> has been entered, its true outcome is
/// unknown, so an implicit abort on dispose must never run afterward -- the transaction/connection are
/// still always disposed (which rolls back an uncommitted transaction), just without the explicit
/// <c>ROLLBACK</c> + importer-abort path this class otherwise takes for a genuine abort.</summary>
internal sealed class PostgresSinkWriteSession : IDeleteApplyingWriteSession
{
    private readonly NpgsqlConnection _connection;
    private readonly NpgsqlTransaction _tx;
    private readonly string _quotedTarget;
    private readonly string? _tempTable; // null = append's direct-COPY path (class doc comment)
    private readonly string _mode;
    private readonly string? _onDelete;
    private readonly string[] _quotedColumns;
    private readonly NpgsqlDbType[] _columnTypes;
    private readonly string[] _quotedKeyColumns;
    private readonly string[] _quotedNonKeyColumns;

    private NpgsqlBinaryImporter? _importer;
    private string? _deleteTempTable;
    private NpgsqlBinaryImporter? _deleteImporter;
    private NpgsqlDbType[]? _deleteKeyTypes;
    private PgSinkSessionState _state = PgSinkSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;

    public PostgresSinkWriteSession(
        NpgsqlConnection connection, NpgsqlTransaction tx, string quotedTarget, string? tempTable, Schema schema,
        string mode, IReadOnlyList<string> keys, string? onDelete)
    {
        _connection = connection;
        _tx = tx;
        _quotedTarget = quotedTarget;
        _tempTable = tempTable;
        _mode = mode;
        _onDelete = onDelete;
        _quotedColumns = schema.FieldsList.Select(f => PgDdl.Quote(f.Name)).ToArray();
        _columnTypes = schema.FieldsList.Select(PgDdl.NpgsqlTypeFor).ToArray();

        var keySet = new HashSet<string>(keys, StringComparer.Ordinal);
        _quotedKeyColumns = keys.Select(PgDdl.Quote).ToArray();
        _quotedNonKeyColumns = schema.FieldsList
            .Where(f => !keySet.Contains(f.Name))
            .Select(f => PgDdl.Quote(f.Name))
            .ToArray();
    }

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");

        try
        {
            var copyDestination = _tempTable is not null ? PgDdl.Quote(_tempTable) : _quotedTarget;
            _importer ??= await _connection.BeginBinaryImportAsync(
                    $"copy {copyDestination} ({string.Join(", ", _quotedColumns)}) from stdin (format binary)", ct)
                .ConfigureAwait(false);

            // Sync row loop: the importer buffers in memory and
            // only occasionally flushes to the socket, so per-cell awaits bought no responsiveness --
            // just millions of async state-machine transitions dominating sink CPU. Cancellation is
            // batch-granular, per the same batch-boundary contract DataReaderSource documents.
            ct.ThrowIfCancellationRequested();
            for (var row = 0; row < batch.Length; row++)
            {
                _importer.StartRow();
                for (var col = 0; col < _columnTypes.Length; col++)
                {
                    WriteCell(_importer, batch.Column(col), row, _columnTypes[col]);
                }
            }

            _rowsWritten += batch.Length;
            _batchesWritten++;
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException($"postgres sink write failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    /// <summary>Streams one delete-key batch into a lazily-created keys-only
    /// temp table, mirroring <see cref="WriteBatchAsync"/>'s COPY machinery (same
    /// <see cref="WriteCellAsync"/> cell writer, a fresh <see cref="NpgsqlBinaryImporter"/> scoped to
    /// this second temp table). Column order/quoting comes from <see cref="_quotedKeyColumns"/> (the
    /// output's declared merge keys); per-column <see cref="NpgsqlDbType"/>/DDL come from the ARRIVING
    /// batch's own schema (captured once, on the first call). Applied at <see cref="CommitAsync"/>,
    /// after the upsert finalizes -- see <see cref="BuildDeleteApplySql"/>.</summary>
    public async ValueTask ApplyDeleteKeysAsync(RecordBatch keyBatch, CancellationToken ct)
    {
        EnsureOpen("apply delete keys to");

        try
        {
            if (_deleteImporter is null)
            {
                // Npgsql allows only one COPY in progress per connection -- the upsert importer (still
                // open, held across every WriteBatchAsync call) must be flushed before this second COPY
                // can start. Safe ahead of CommitAsync's finalize: completing the COPY only lands rows
                // into the upsert temp table, not the real target -- the engine's drain protocol
                // guarantees every WriteBatchAsync for this session already happened by the time
                // ApplyDeleteKeysAsync is first called.
                if (_importer is not null)
                {
                    await _importer.CompleteAsync(ct).ConfigureAwait(false);
                    await _importer.DisposeAsync().ConfigureAwait(false);
                    _importer = null;
                }

                _deleteTempTable = $"pz_tmp_del_{Guid.NewGuid().ToString("N")[..8]}";
                var columnsDdl = string.Join(", ", _quotedKeyColumns.Zip(
                    keyBatch.Schema.FieldsList, (name, field) => $"{name} {PgDdl.PgTypeFor(field)}"));
                await using (var create = new NpgsqlCommand(
                    $"create temp table {PgDdl.Quote(_deleteTempTable)} ({columnsDdl})", _connection, _tx))
                {
                    await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                _deleteKeyTypes = keyBatch.Schema.FieldsList.Select(PgDdl.NpgsqlTypeFor).ToArray();
                _deleteImporter = await _connection.BeginBinaryImportAsync(
                        $"copy {PgDdl.Quote(_deleteTempTable)} ({string.Join(", ", _quotedKeyColumns)}) " +
                        "from stdin (format binary)", ct)
                    .ConfigureAwait(false);
            }

            // Same sync row loop as WriteBatchAsync (see the comment there); ct is batch-granular.
            ct.ThrowIfCancellationRequested();
            for (var row = 0; row < keyBatch.Length; row++)
            {
                _deleteImporter.StartRow();
                for (var col = 0; col < _deleteKeyTypes!.Length; col++)
                {
                    WriteCell(_deleteImporter, keyBatch.Column(col), row, _deleteKeyTypes[col]);
                }
            }
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"postgres sink delete-key apply failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    private static void WriteCell(NpgsqlBinaryImporter importer, IArrowArray array, int row, NpgsqlDbType type)
    {
        if (array.IsNull(row))
        {
            importer.WriteNull();
            return;
        }

        switch (array)
        {
            case Int32Array a:
                importer.Write(a.GetValue(row)!.Value, type);
                break;
            case Int64Array a:
                importer.Write(a.GetValue(row)!.Value, type);
                break;
            case DoubleArray a:
                importer.Write(a.GetValue(row)!.Value, type);
                break;
            case Decimal128Array a:
                importer.Write(a.GetValue(row)!.Value, type);
                break;
            case BooleanArray a:
                importer.Write(a.GetValue(row)!.Value, type);
                break;
            case Date32Array a:
                importer.Write(DateOnly.FromDateTime(a.GetDateTime(row)!.Value), type);
                break;
            case TimestampArray a:
                importer.Write(a.GetTimestamp(row)!.Value, type);
                break;
            case StringArray a:
                importer.Write(a.GetString(row), type);
                break;
            default:
                throw new NotSupportedException($"postgres sink v0 does not support array type '{array.GetType()}'");
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;

        try
        {
            if (_importer is not null)
            {
                await _importer.CompleteAsync(ct).ConfigureAwait(false);
                // Complete() flushes the COPY but does not itself release the connection's "current
                // operation" slot -- Dispose does. Without this, the finalize command below fails with
                // "the connection is already in state 'Copy'".
                await _importer.DisposeAsync().ConfigureAwait(false);
                _importer = null;
            }

            // Npgsql allows only one COPY in progress per connection -- the delete-key importer (if any
            // batch was ever applied) must also be flushed before EITHER finalize statement below can
            // run, even though the delete-apply SQL itself runs after the upsert finalize.
            if (_deleteImporter is not null)
            {
                await _deleteImporter.CompleteAsync(ct).ConfigureAwait(false);
                await _deleteImporter.DisposeAsync().ConfigureAwait(false);
                _deleteImporter = null;
            }

            // Append COPYed directly into the target (no temp table, nothing to finalize); replace and
            // merge (ON CONFLICT against the unique(keys) constraint PgDdl.EnsureTargetAsync guaranteed
            // exists before this session was even opened -- see BeginWriteAsync) drain their staged
            // temp here.
            if (_tempTable is not null)
            {
                var columnList = string.Join(", ", _quotedColumns);
                var quotedTemp = PgDdl.Quote(_tempTable);
                var finalizeSql = _mode switch
                {
                    "replace" =>
                        $"truncate table {_quotedTarget}; " +
                        $"insert into {_quotedTarget} ({columnList}) select {columnList} from {quotedTemp}",
                    "merge" => BuildMergeSql(columnList, quotedTemp),
                    _ => throw new PzConnectorException(
                        $"postgres sink: mode '{_mode}' has no temp-table finalize (append COPYs direct)",
                        isTransient: false),
                };

                await using var finalize = new NpgsqlCommand(finalizeSql, _connection, _tx);
                await finalize.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // Deletes apply AFTER the upsert, same transaction: upserts first, deletes second -- a
            // same-window key never appears in both sets, but a soft-then-later-reupsert across
            // sessions relies on the upsert's SET list clearing the flag, not on ordering here.
            if (_deleteTempTable is not null)
            {
                await using var apply = new NpgsqlCommand(BuildDeleteApplySql(), _connection, _tx);
                await apply.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await _tx.CommitAsync(ct).ConfigureAwait(false);
            _state = PgSinkSessionState.Committed;
            return new WriteResult(_rowsWritten, _batchesWritten);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException($"postgres sink commit failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    /// <summary><c>insert ... on conflict (&lt;keys&gt;) do update set &lt;nk&gt; =
    /// excluded.&lt;nk&gt;, ...</c> over every non-key column. A KEY-ONLY table (no non-key columns at
    /// all) degrades to <c>do nothing</c> -- there is nothing to SET, and an empty SET list is not valid
    /// SQL. All identifiers come from <see cref="PgDdl.Quote"/> (already applied to
    /// <see cref="_quotedKeyColumns"/>/<see cref="_quotedNonKeyColumns"/> at construction).
    /// <para>The SELECT feeding the insert is <c>distinct on (&lt;keys&gt;) ... order by &lt;keys&gt;,
    /// ctid desc</c> rather than a bare <c>select &lt;cols&gt; from &lt;tmp&gt;</c>: this whole temp table
    /// (every batch of the session) is inserted in ONE statement, and if two of its rows share a key,
    /// postgres raises "ON CONFLICT DO UPDATE command cannot affect row a second time" (a single INSERT may
    /// not touch the same target row twice). <c>ctid</c> ascends with COPY insertion order on this
    /// append-only temp table, so ordering by it descending and taking DISTINCT ON per key keeps exactly the
    /// LAST-inserted row for each duplicated key -- i.e. last-writer-wins, matching the merge contract
    /// <see cref="Pz.Connectors.TestKit.Reference.MergeRows.Build"/>'s <c>Absorb</c> already establishes for
    /// <c>InMemorySink</c>. On the common case of no duplicate keys within the session this is a no-op:
    /// DISTINCT ON over an already-unique key set returns every row unchanged.</para></summary>
    private string BuildMergeSql(string columnList, string quotedTemp)
    {
        var conflictTarget = string.Join(", ", _quotedKeyColumns);
        var selectSql = $"select distinct on ({conflictTarget}) {columnList} from {quotedTemp} " +
            $"order by {conflictTarget}, ctid desc";
        var insertSql = $"insert into {_quotedTarget} ({columnList}) {selectSql} on conflict ({conflictTarget})";

        var isSoftDelete = string.Equals(_onDelete, "soft", StringComparison.Ordinal);
        if (_quotedNonKeyColumns.Length == 0 && !isSoftDelete)
        {
            return $"{insertSql} do nothing";
        }

        var setParts = _quotedNonKeyColumns.Select(c => $"{c} = excluded.{c}").ToList();
        if (isSoftDelete)
        {
            // A key re-upserted after being soft-deleted (this session or a prior one) is live again --
            // clear the marker.
            setParts.Add($"{PgDdl.Quote(PgDdl.SoftDeleteColumn)} = null");
        }

        return $"{insertSql} do update set {string.Join(", ", setParts)}";
    }

    /// <summary>Builds the delete-apply statement run at commit, after the upsert (see
    /// <see cref="CommitAsync"/>): a hard delete removes matching target rows; a soft delete stamps
    /// <see cref="PgDdl.SoftDeleteColumn"/> instead. Both join the target to the delete-key temp table
    /// on every declared merge key -- duplicate key rows in the temp table (idempotent replay) match
    /// the same target row more than once, which postgres tolerates without error for both statement
    /// shapes (unlike an ON CONFLICT arbiter).</summary>
    private string BuildDeleteApplySql()
    {
        var quotedDelTmp = PgDdl.Quote(_deleteTempTable!);
        var joinCond = string.Join(" and ", _quotedKeyColumns.Select(c => $"t.{c} = d.{c}"));
        return _onDelete switch
        {
            "delete" => $"delete from {_quotedTarget} t using {quotedDelTmp} d where {joinCond}",
            "soft" => $"update {_quotedTarget} t set {PgDdl.Quote(PgDdl.SoftDeleteColumn)} = now() " +
                $"from {quotedDelTmp} d where {joinCond}",
            _ => throw new PzConnectorException(
                $"postgres sink: on_delete '{_onDelete}' has no delete-apply statement", isTransient: false),
        };
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        if (_importer is not null)
        {
            // Dispose WITHOUT Complete() -- aborts the COPY instead of flushing it.
            await _importer.DisposeAsync().ConfigureAwait(false);
            _importer = null;
        }

        if (_deleteImporter is not null)
        {
            await _deleteImporter.DisposeAsync().ConfigureAwait(false);
            _deleteImporter = null;
        }

        await _tx.RollbackAsync(ct).ConfigureAwait(false);
        _state = PgSinkSessionState.Aborted;
    }

    public async ValueTask DisposeAsync()
    {
        if (_state == PgSinkSessionState.Open)
        {
            if (_commitAttempted)
            {
                // Commit was attempted (and threw) -- per commit-xor-abort this must NOT count as an
                // implicit abort (Commit's true outcome is unknown). Just release the importer if it's
                // still open; the transaction is disposed unconditionally below, which rolls back an
                // uncommitted transaction automatically.
                if (_importer is not null)
                {
                    await _importer.DisposeAsync().ConfigureAwait(false);
                    _importer = null;
                }

                if (_deleteImporter is not null)
                {
                    await _deleteImporter.DisposeAsync().ConfigureAwait(false);
                    _deleteImporter = null;
                }
            }
            else
            {
                if (_importer is not null)
                {
                    await _importer.DisposeAsync().ConfigureAwait(false);
                    _importer = null;
                }

                if (_deleteImporter is not null)
                {
                    await _deleteImporter.DisposeAsync().ConfigureAwait(false);
                    _deleteImporter = null;
                }

                await _tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                _state = PgSinkSessionState.Aborted;
            }
        }

        await _tx.DisposeAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);
    }

    private void EnsureOpen(string action)
    {
        if (_state != PgSinkSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}
