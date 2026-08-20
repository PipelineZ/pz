using System.Globalization;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Postgres;

/// <summary>The single cdc partition for one Postgres dataset. Runs either the
/// first-run snapshot path (<see cref="DatasetSpec.PriorSyncState"/> null = first run or --full-refresh)
/// or the bounded pgoutput poll path (PriorSyncState non-null). Both emit the SAME change-row
/// contract (<c>_pz_op</c>/<c>_pz_lsn</c>/<c>_pz_changed_at</c> then the table columns) against the SAME
/// probed Arrow schema, so the engine ingests whichever path ran.
///
/// First-run flow: validate prerequisites on a regular connection (fail fast -- no row lands on an
/// unmet prerequisite), discover the change-key columns, resolve slot conflicts, then create a pgoutput
/// replication slot with an EXPORTED snapshot. The exported snapshot name is imported into a
/// <c>repeatable read</c> transaction on a second, regular connection (<c>SET TRANSACTION SNAPSHOT</c>),
/// and the table is read through that snapshot as plain SQL -- yielding the change-row contract
/// (<c>_pz_op='insert'</c>, all-zeros <c>_pz_lsn</c>, null <c>_pz_changed_at</c>, then the row columns).
/// The slot's consistent point becomes the sync-state token, so the poll resumes exactly where the
/// snapshot's transactional cut ended -- no gap, no overlap.
///
/// Poll flow: capture a bounded target (<c>pg_current_wal_insert_lsn()</c>) at read start, confirm the persisted
/// token at replication start ONCE (never during consume -- that is what keeps a replay reading the same
/// WAL), then stream pgoutput messages, emitting one change row per insert/update/delete and stopping at
/// the first commit whose LSN reaches the target. A caught-up-but-target-unreached stream is bounded by an
/// idle timer (see <see cref="IdleTimeout"/>).</summary>
internal sealed class PostgresCdcPartition(string connectionString, DatasetSpec spec)
    : IDatasetPartition, ISyncStatePartition, IChangeCapturePartition
{
    private static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromSeconds(5);

    private IReadOnlyList<string>? _keyColumns;
    private string? _syncCandidate;

    // Poll-path scratch, set once at the top of PollReadAsync and read per message: the relation this
    // dataset polls (a FOR ALL TABLES publication streams every table down the same slot, so every
    // relation-bearing message has to be matched against it) and the schema-field indices of the change
    // key columns (used to tell a REPLICA IDENTITY FULL key change from an ordinary update).
    private string _polledSchema = "";
    private string _polledTable = "";
    private int[] _keyFields = [];

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        spec.PriorSyncState is null ? SnapshotReadAsync(options, ct) : PollReadAsync(options, ct);

    private async IAsyncEnumerable<RecordBatch> SnapshotReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var (connection, reader) = await PrepareSnapshotAsync(ct).ConfigureAwait(false);

        // Manual-enumerator pattern (CS1626: no yield inside try/catch) -- mirrors PostgresPartition:
        // classify a mid-stream Npgsql fault as a PzConnectorException, dispose everything in finally.
        var enumerator = DataReaderSource.ReadBatchesAsync(reader, options.TargetBatchBytes, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                catch (NpgsqlException ex)
                {
                    throw new PzConnectorException(
                        $"postgres cdc snapshot read failed mid-stream: {ex.Message}", ex.IsTransient, innerException: ex);
                }

                if (!moved)
                {
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
            await reader.DisposeAsync().ConfigureAwait(false);
            await connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    public bool TryGetChangeKeyColumns(out IReadOnlyList<string>? keyColumns)
    {
        keyColumns = _keyColumns;
        return _keyColumns is { Count: > 0 };
    }

    public bool TryGetSyncStateCandidate(out string? candidate)
    {
        candidate = _syncCandidate;
        return _syncCandidate is not null;
    }

    // ---- poll path (PriorSyncState != null) ----

    private async IAsyncEnumerable<RecordBatch> PollReadAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var (schemaName, table) = PostgresCdc.SchemaAndTable(spec);
        var slotName = PostgresCdc.SlotName(spec);
        var publication = PostgresCdc.PublicationName(spec);
        var idleTimeout = IdleTimeout();

        // 1. Regular connection: capture the bounded target (WAL head at read start), probe the change-row
        //    schema (identical to the snapshot path's), and discover the key columns. Key discovery here
        //    (rather than from the pgoutput RelationMessage) means an empty-backlog poll -- which streams
        //    no RelationMessage -- still reports keys, and it reuses the snapshot path's exact derivation.
        Schema schema;
        NpgsqlLogSequenceNumber target;
        await using (var admin = new NpgsqlConnection(connectionString))
        {
            try
            {
                await admin.OpenAsync(ct).ConfigureAwait(false);
                target = await CurrentWalLsnAsync(admin, ct).ConfigureAwait(false);
                schema = await ProbeChangeSchemaAsync(admin, schemaName, table, ct).ConfigureAwait(false);
                _keyColumns = await PostgresCdc.DiscoverKeyColumnsAsync(admin, schemaName, table, ct).ConfigureAwait(false);
            }
            catch (NpgsqlException ex)
            {
                throw new PzConnectorException(
                    $"postgres cdc: poll setup failed for dataset '{spec.Dataset}': {ex.Message}",
                    ex.IsTransient, innerException: ex);
            }
        }

        var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes);
        // Map each table column name to its schema field index (fields 0..2 are the _pz_ header). pgoutput
        // tuples are decoded by column NAME, not position, so a relation column order that differs from the
        // probed `t.*` order still lands each value in the right column. A column-list publication is a
        // different story: a column it omits from the list never appears in the tuple at all, so it decodes
        // as null (EmitRowAsync's default), not the real value -- which is exactly why
        // ValidatePrerequisitesAsync refuses a publication with a column list up front.
        var columnIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        for (var i = 3; i < schema.FieldsList.Count; i++)
        {
            columnIndex[schema.FieldsList[i].Name] = i;
        }

        _polledSchema = schemaName;
        _polledTable = table;
        // Key columns that the probed schema actually carries. Empty when the table has neither a replica
        // identity index nor a primary key -- such a table cannot report a key change, so the FULL branch
        // below degrades to emitting the new row only.
        var keyFields = new List<int>();
        foreach (var name in _keyColumns ?? [])
        {
            if (columnIndex.TryGetValue(name, out var field))
            {
                keyFields.Add(field);
            }
        }

        _keyFields = [.. keyFields];

        await using var replication = new LogicalReplicationConnection(connectionString);
        try
        {
            await replication.Open(ct).ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            throw new PzConnectorException(
                $"postgres cdc: replication connection failed for dataset '{spec.Dataset}': {ex.Message}",
                ex.IsTransient, innerException: ex);
        }

        // Confirm the PERSISTED position and PIN it there for the whole window. Npgsql sends a standby
        // status update carrying LastFlushedLsn on WalReceiverStatusInterval (SendStatusUpdate itself is
        // illegal before the stream is active in Npgsql 10, so we drive it via the interval instead). By
        // presetting LastFlushed/LastApplied to the persisted token once and NEVER raising them while
        // consuming, every status update re-confirms exactly that token -- which both advances
        // confirmed_flush to it (releasing older WAL, even on an empty poll) and guarantees the flush
        // position never moves PAST it mid-window, so a replay reads the same WAL. A sub-second
        // interval makes the confirm land well within the idle window.
        var confirmed = PostgresCdc.ParseLsn(spec.PriorSyncState!);
        replication.LastAppliedLsn = confirmed;
        replication.LastFlushedLsn = confirmed;
        replication.WalReceiverStatusInterval = TimeSpan.FromMilliseconds(500);

        var slot = new PgOutputReplicationSlot(slotName);
        // binary: true -- pgoutput's default TEXT tuple format makes ReplicationValue.Get<T>() unsupported
        // for non-text columns (it reports the value as DataTypeName 'text'); binary format lets us decode
        // each value straight into the CLR type the Arrow column expects, so a poll row is byte-identical
        // to the snapshot path's for the same source row. Binary pgoutput is a PostgreSQL 14+ feature --
        // ValidatePrerequisitesAsync's server-version check is what keeps this from reaching an older server.
        var pgOptions = new PgOutputReplicationOptions(publication, PgOutputProtocolVersion.V1, binary: true);

        // A wall-clock idle timer bounds the tail of a caught-up-but-target-unreached stream (pgoutput only
        // surfaces publication traffic, so MoveNext would otherwise block). It fires between transactions --
        // intra-txn messages stream back-to-back and restart it -- so it never truncates a partial
        // transaction in practice. The cleaner stop, on a keepalive WAL position past the target, is not
        // available until Npgsql surfaces keepalive LSNs.
        using var consumeCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        using var idleTimer = new Timer(_ =>
        {
            try { consumeCts.Cancel(); }
            catch (ObjectDisposedException) { /* window already ended */ }
        });

        var state = new PollState();

        IAsyncEnumerator<PgOutputReplicationMessage> enumerator;
        try
        {
            enumerator = replication.StartReplication(slot, pgOptions, consumeCts.Token)
                .GetAsyncEnumerator(consumeCts.Token);
        }
        catch (PostgresException ex)
        {
            throw StartReplicationFailure(ex, publication);
        }

        try
        {
            while (true)
            {
                idleTimer.Change(idleTimeout, Timeout.InfiniteTimeSpan);

                RecordBatch? batch = null;
                var stop = false;
                var endOfWindow = false;
                try
                {
                    if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                    {
                        endOfWindow = true;
                    }
                    else
                    {
                        (batch, stop) = await ProcessMessageAsync(
                            enumerator.Current, builder, schema, columnIndex, state, target, consumeCts.Token)
                            .ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (consumeCts.IsCancellationRequested && !ct.IsCancellationRequested)
                {
                    // Idle timer (or our own stop-break) cancelled OUR linked cts -- normal end of window.
                    endOfWindow = true;
                }
                catch (PostgresException ex)
                {
                    throw StartReplicationFailure(ex, publication);
                }
                catch (NpgsqlException ex)
                {
                    throw new PzConnectorException(
                        $"postgres cdc: poll read failed mid-stream for dataset '{spec.Dataset}': {ex.Message}",
                        ex.IsTransient, innerException: ex);
                }

                if (batch is not null)
                {
                    yield return batch;
                }

                if (stop || endOfWindow)
                {
                    consumeCts.Cancel(); // stop the replication stream promptly before teardown
                    break;
                }
            }
        }
        finally
        {
            try { await enumerator.DisposeAsync().ConfigureAwait(false); }
            catch (OperationCanceledException) { /* teardown after our own cancel */ }
            catch (NpgsqlException) { /* best-effort stream teardown; the connection is disposed below */ }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }

        // Empty backlog -> LastCommitted null -> candidate stays absent (stored token unchanged). A
        // zero-change window that consumed empty commits still advances to the last committed LSN (harmless).
        _syncCandidate = state.LastCommitted is { } committed ? PostgresCdc.FormatLsn(committed) : null;
    }

    // Decodes one pgoutput message into at most one change row. Returns the batch the builder took (if it
    // filled) and whether the stop condition (commit LSN >= target) fired -- checked ONLY at commit
    // boundaries so a transaction is never split across the window edge.
    private async Task<(RecordBatch? Batch, bool Stop)> ProcessMessageAsync(
        PgOutputReplicationMessage message, ArrowBatchBuilder builder, Schema schema,
        IReadOnlyDictionary<string, int> columnIndex, PollState state, NpgsqlLogSequenceNumber target, CancellationToken ct)
    {
        // A publication may cover more tables than this dataset polls -- `FOR ALL TABLES` (explicitly
        // supported) streams EVERY table in the database down this one slot. Decoding is by column NAME,
        // so an unrelated table that happens to share column names would otherwise land its values in our
        // columns as plausible-looking change rows. Anything that is not our relation is not our business.
        if (RelationOf(message) is { } relation && !IsPolledRelation(relation))
        {
            return (null, false);
        }

        switch (message)
        {
            case BeginMessage begin:
                state.CurrentTxnLsn = begin.TransactionFinalLsn; // the transaction's commit LSN
                state.CurrentTimestamp = new DateTimeOffset(
                    DateTime.SpecifyKind(begin.TransactionCommitTimestamp, DateTimeKind.Utc));
                state.Seq = 0; // seq resets per transaction, increments per emitted row
                return (null, false);
            case InsertMessage insert:
                await EmitRowAsync("insert", insert.NewRow, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case IndexUpdateMessage indexUpdate:
                // REPLICA IDENTITY DEFAULT/USING INDEX: postgres attaches the old key ONLY when that key
                // changed, so this message type IS the key-change signal. The old key's delete has to be
                // emitted too -- a merge sink keyed on it would otherwise keep a ghost row under the
                // pre-change key forever, and --full-refresh cannot heal that (a merge never removes rows
                // the snapshot does not mention). Delete first, so last-event-per-key collapse sees the
                // delete as the OLD key's final event and the upsert as the NEW key's.
                await EmitRowAsync("delete", indexUpdate.Key, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                await EmitRowAsync("update", indexUpdate.NewRow, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case FullUpdateMessage fullUpdate:
                // REPLICA IDENTITY FULL ships the old row on EVERY update, so unlike the index case the
                // key has to be compared before a delete can be justified.
                await EmitFullUpdateAsync(fullUpdate, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case UpdateMessage update: // DefaultUpdateMessage -- key unchanged, new row only
                await EmitRowAsync("update", update.NewRow, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case KeyDeleteMessage keyDelete: // REPLICA IDENTITY DEFAULT/INDEX: key columns only
                await EmitRowAsync("delete", keyDelete.Key, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case FullDeleteMessage fullDelete: // REPLICA IDENTITY FULL: the whole old row
                await EmitRowAsync("delete", fullDelete.OldRow, builder, schema, columnIndex, state, ct).ConfigureAwait(false);
                break;
            case TruncateMessage truncate when CoversPolledRelation(truncate):
                throw TruncateNotRepresentable();
            case CommitMessage:
                state.LastCommitted = state.CurrentTxnLsn;
                return (null, (ulong)state.CurrentTxnLsn >= (ulong)target);
            default:
                // Relation/Origin/Type/another table's truncate/streaming-control -- nothing to emit.
                return (null, false);
        }

        // One message can emit two rows (a key change), so the batch may overshoot the byte target by a
        // single row before it is taken. TargetBatchBytes is a soft target, so that is harmless.
        builder.TryTakeBatch(out var batch);
        return (batch, false);
    }

    // The relation a message applies to, or null for messages that carry none (Begin/Commit/Relation/
    // Origin/Type/streaming-control). TruncateMessage carries a LIST of relations, so it is matched by
    // CoversPolledRelation instead.
    private static RelationMessage? RelationOf(PgOutputReplicationMessage message) => message switch
    {
        InsertMessage insert => insert.Relation,
        UpdateMessage update => update.Relation,
        DeleteMessage delete => delete.Relation,
        _ => null,
    };

    private bool IsPolledRelation(RelationMessage relation) =>
        string.Equals(relation.Namespace, _polledSchema, StringComparison.Ordinal)
        && string.Equals(relation.RelationName, _polledTable, StringComparison.Ordinal);

    private bool CoversPolledRelation(TruncateMessage truncate)
    {
        foreach (var relation in truncate.Relations)
        {
            if (IsPolledRelation(relation))
            {
                return true;
            }
        }

        return false;
    }

    // A truncate empties the table in one table-level event: pgoutput reports no per-row deletes for it, so
    // there is no set of change rows this window could emit that would leave the target matching the source.
    // Silently skipping it would report a green run over a target that still holds every dropped row.
    //
    // Note the remediation is NOT --full-refresh on its own: a re-snapshot feeds the merge an empty (or
    // shrunken) row set, and a merge never removes rows its input omits -- so the destination would keep
    // every truncated row and the run would go green over the same divergence. The destination has to be
    // emptied by hand first; that is a destructive write on the operator's own target, so pz names it
    // rather than doing it.
    private PzConnectorException TruncateNotRepresentable()
    {
        var (schema, table) = PostgresCdc.SchemaAndTable(spec);
        return new PzConnectorException(
            $"postgres cdc: source table {PgDdl.Quote(schema)}.{PgDdl.Quote(table)} was TRUNCATEd inside the " +
            $"polled change window for dataset '{spec.Dataset}'. A truncate removes every row as one " +
            "table-level event with no per-row deletes, so cdc cannot express it as change rows.\n" +
            "--full-refresh alone does NOT recover: a merge output never removes rows its input omits, so " +
            "the destination would keep every truncated row. Empty this dataset's destination table(s) " +
            "yourself, then re-snapshot:\n" +
            "  <delete or truncate the destination table>\n" +
            "  pz run --full-refresh",
            isTransient: false);
    }

    // REPLICA IDENTITY FULL: the old row arrives on every update, so a delete is only correct when the key
    // actually moved -- emitting one unconditionally would double the change volume and manufacture a
    // delete for every ordinary edit. Both tuples are enumerated in wire order (old, then new) before
    // either is appended: a partially consumed tuple corrupts the stream.
    private async Task EmitFullUpdateAsync(
        FullUpdateMessage update, ArrowBatchBuilder builder, Schema schema,
        IReadOnlyDictionary<string, int> columnIndex, PollState state, CancellationToken ct)
    {
        var oldRow = await DecodeTupleAsync(update.OldRow, schema, columnIndex, ct).ConfigureAwait(false);
        var newRow = await DecodeTupleAsync(update.NewRow, schema, columnIndex, ct).ConfigureAwait(false);

        var keyChanged = false;
        foreach (var field in _keyFields)
        {
            if (!Equals(oldRow[field], newRow[field]))
            {
                keyChanged = true;
                break;
            }
        }

        if (keyChanged)
        {
            AppendRow("delete", oldRow, builder, state);
        }

        AppendRow("update", newRow, builder, state);
    }

    // Emits one change row: the three _pz_ header fields then the table columns.
    private static async Task EmitRowAsync(
        string op, ReplicationTuple tuple, ArrowBatchBuilder builder, Schema schema,
        IReadOnlyDictionary<string, int> columnIndex, PollState state, CancellationToken ct) =>
        AppendRow(op, await DecodeTupleAsync(tuple, schema, columnIndex, ct).ConfigureAwait(false), builder, state);

    // Decodes one tuple into a row array sized for the whole change schema, each value landing in its
    // schema field looked up by column NAME (order-independent). The tuple is fully enumerated even for
    // skipped/null columns -- a partially consumed tuple corrupts the stream. Columns absent from the tuple
    // (e.g. a KeyDelete's non-key columns) stay null, as do the three _pz_ header slots AppendRow stamps.
    private static async Task<object?[]> DecodeTupleAsync(
        ReplicationTuple tuple, Schema schema, IReadOnlyDictionary<string, int> columnIndex, CancellationToken ct)
    {
        var row = new object?[schema.FieldsList.Count];
        await foreach (var value in tuple.WithCancellation(ct).ConfigureAwait(false))
        {
            if (value.GetFieldName() is { } name && columnIndex.TryGetValue(name, out var field))
            {
                row[field] = value.IsDBNull
                    ? null
                    : await DecodeAsync(value, schema.FieldsList[field].DataType.TypeId, ct).ConfigureAwait(false);
            }
        }

        return row;
    }

    // Stamps the _pz_ header onto an already-decoded row and appends it. Called in emission order, so the
    // seq suffix orders rows within their transaction exactly as they were emitted.
    private static void AppendRow(string op, object?[] row, ArrowBatchBuilder builder, PollState state)
    {
        row[0] = op;
        row[1] = $"{(ulong)state.CurrentTxnLsn:X16}-{state.Seq:D9}";
        state.Seq++;
        row[2] = state.CurrentTimestamp;
        builder.AppendRow(row);
    }

    // Decodes one replication value against the probed Arrow column type, using the CLR type the
    // DataReaderSource v0 matrix maps that Arrow type from -- so a poll row is byte-identical to the same
    // row read through the snapshot path.
    private static async Task<object?> DecodeAsync(ReplicationValue value, ArrowTypeId typeId, CancellationToken ct) =>
        typeId switch
        {
            ArrowTypeId.Int32 => await value.Get<int>(ct).ConfigureAwait(false),
            ArrowTypeId.Int64 => await value.Get<long>(ct).ConfigureAwait(false),
            ArrowTypeId.Double => await value.Get<double>(ct).ConfigureAwait(false),
            ArrowTypeId.Decimal128 => await value.Get<decimal>(ct).ConfigureAwait(false),
            ArrowTypeId.String => await value.Get<string>(ct).ConfigureAwait(false),
            ArrowTypeId.Boolean => await value.Get<bool>(ct).ConfigureAwait(false),
            ArrowTypeId.Date32 => await value.Get<DateOnly>(ct).ConfigureAwait(false),
            // Fallback: Get<DateTimeOffset> rejects `timestamp without time zone`, so read the CLR DateTime
            // and reinterpret it UTC -- exactly DataReaderSource.Normalize's trusted-UTC rule.
            ArrowTypeId.Timestamp => new DateTimeOffset(
                DateTime.SpecifyKind(await value.Get<DateTime>(ct).ConfigureAwait(false), DateTimeKind.Utc)),
            _ => throw new PzConnectorException(
                $"postgres cdc: unsupported change-stream column arrow type '{typeId}'", isTransient: false),
        };

    private static async Task<NpgsqlLogSequenceNumber> CurrentWalLsnAsync(NpgsqlConnection conn, CancellationToken ct)
    {
        // pg_current_wal_insert_lsn() (the WAL INSERT head), not pg_current_wal_lsn() (the flush pointer):
        // pgoutput reports a transaction's commit LSN as the END of its commit record, which can sit a few
        // bytes past the flush pointer for a just-committed transaction. Using the insert head as the
        // bounded target guarantees target >= every already-committed change, so the stop condition never
        // truncates the backlog one transaction short.
        await using var cmd = new NpgsqlCommand("select pg_current_wal_insert_lsn()", conn);
        return (NpgsqlLogSequenceNumber)(await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false))!;
    }

    // Probes the change-row shape exactly as PostgresSource.GetSchemaAsync does for the cdc branch: the
    // three _pz_ header columns prepended to the table's own columns.
    private static async Task<Schema> ProbeChangeSchemaAsync(
        NpgsqlConnection conn, string schema, string table, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            $"select * from ({PostgresCdc.SnapshotSelect(schema, table)}) q limit 0", conn);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return DataReaderSource.BuildArrowSchema(reader);
    }

    private PzConnectorException StartReplicationFailure(PostgresException ex, string publication)
    {
        // Belt-and-braces: the prerequisite check (publication coverage + the PG14+ server-version floor
        // pgoutput binary mode requires) makes this near-unreachable, but a publication dropped between the
        // prereq check and StartReplication -- or a server-version check that ran against a different
        // server -- surfaces here with the same copy-paste remediation.
        var (schema, table) = PostgresCdc.SchemaAndTable(spec);

        // A slot that is gone is a different failure from a publication that never covered the table, and
        // it has a different fix -- recreating a publication does nothing for it. This path only ever runs
        // with a stored token (the poll path is what PriorSyncState selects), so the slot's disappearance
        // also means every change it was holding WAL for is unrecoverable: say so, and name the two
        // commands that actually recover instead of a CREATE PUBLICATION that would not.
        if (string.Equals(ex.SqlState, PostgresErrorCodes.UndefinedObject, StringComparison.Ordinal))
        {
            return new PzConnectorException(
                $"postgres cdc: replication slot '{PostgresCdc.SlotName(spec)}' for dataset '{spec.Dataset}' " +
                $"no longer exists: {ex.MessageText}\n" +
                "pz still holds a sync token for this dataset, so changes since that token are gone with the " +
                "slot (it was dropped, or postgres invalidated it past max_slot_wal_keep_size) -- clear the " +
                "stored token and re-snapshot:\n" +
                $"pz cdc drop {spec.Source}.{spec.Dataset}\n" +
                "pz run --full-refresh",
                ex.IsTransient, innerException: ex);
        }

        return new PzConnectorException(
            $"postgres cdc: starting replication failed for dataset '{spec.Dataset}': {ex.MessageText}\n" +
            "likely cause: the publication is missing or does not cover the table, or the server is older " +
            "than PostgreSQL 14 (pgoutput binary mode requires 14+) -- ensure the publication exists and " +
            "covers the table:\n" +
            $"CREATE PUBLICATION {publication} FOR TABLE {PgDdl.Quote(schema)}.{PgDdl.Quote(table)};",
            ex.IsTransient, innerException: ex);
    }

    private TimeSpan IdleTimeout()
    {
        if (!spec.Options.TryGetValue("poll_idle_timeout", out var raw) || raw?.ToString() is not { Length: > 0 } text)
        {
            return DefaultIdleTimeout;
        }

        if (!TryParseDuration(text, out var value) || value <= TimeSpan.Zero)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'poll_idle_timeout' must be a positive duration " +
                $"(<integer><ms|s|m|h>, e.g. 5s), got '{text}'", isTransient: false);
        }

        return value;
    }

    // Connector-side duration parse (Pz.Core.DurationParser is host-side, out of the ABI's reach). Same
    // grammar subset: <non-negative integer><ms|s|m|h>, no whitespace/sign/fractions.
    private static bool TryParseDuration(string text, out TimeSpan value)
    {
        value = default;
        var unitStart = 0;
        while (unitStart < text.Length && char.IsAsciiDigit(text[unitStart]))
        {
            unitStart++;
        }

        if (unitStart == 0 ||
            !long.TryParse(text[..unitStart], NumberStyles.None, CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        try
        {
            switch (text[unitStart..])
            {
                case "ms": value = TimeSpan.FromMilliseconds(magnitude); return true;
                case "s": value = TimeSpan.FromSeconds(magnitude); return true;
                case "m": value = TimeSpan.FromMinutes(magnitude); return true;
                case "h": value = TimeSpan.FromHours(magnitude); return true;
                default: return false;
            }
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private sealed class PollState
    {
        public NpgsqlLogSequenceNumber CurrentTxnLsn { get; set; }

        public DateTimeOffset CurrentTimestamp { get; set; }

        public int Seq { get; set; }

        public NpgsqlLogSequenceNumber? LastCommitted { get; set; }
    }

    // Runs everything before the first snapshot yield: prerequisites (fail fast), key discovery, slot
    // lifecycle, slot creation with an exported snapshot, and importing that snapshot into a
    // repeatable-read transaction on the returned data connection. On return, the data connection holds an
    // open reader over the snapshot; _keyColumns and _syncCandidate are populated.
    // NOTE: the persistent slot is created here BEFORE the sync token is persisted (that happens only after
    // every downstream sink commits). If the snapshot fails mid-way, the run never persists a token, so the
    // NEXT run is still a first run -- and ResolveSlotConflictAsync's drop+recreate reclaims this orphaned
    // slot on that retry. The leak is therefore self-healing across retries, not a permanent orphan.
    private async Task<(NpgsqlConnection Connection, NpgsqlDataReader Reader)> PrepareSnapshotAsync(CancellationToken ct)
    {
        var (schema, table) = PostgresCdc.SchemaAndTable(spec);
        var slotName = PostgresCdc.SlotName(spec);

        // 1. Prerequisites, key discovery, slot conflict resolution on a regular connection. Fail-fast:
        //    an unmet prerequisite throws here, before any slot is created or any row lands.
        await using (var admin = new NpgsqlConnection(connectionString))
        {
            try
            {
                await admin.OpenAsync(ct).ConfigureAwait(false);
            }
            catch (NpgsqlException ex)
            {
                throw new PzConnectorException(
                    $"postgres cdc: connection failed: {ex.Message}", ex.IsTransient, innerException: ex);
            }

            var unmet = await PostgresCdc.ValidatePrerequisitesAsync(admin, spec, ct).ConfigureAwait(false);
            if (unmet.Count > 0)
            {
                throw new PzConnectorException(
                    $"postgres cdc: prerequisites not met for dataset '{spec.Dataset}' -- run:\n" +
                    string.Join("\n", unmet),
                    isTransient: false);
            }

            _keyColumns = await PostgresCdc.DiscoverKeyColumnsAsync(admin, schema, table, ct).ConfigureAwait(false);
            await PostgresCdc.ResolveSlotConflictAsync(admin, slotName, ct).ConfigureAwait(false);
        }

        // 2. Create the pgoutput slot with an exported snapshot, then import that snapshot into a
        //    repeatable-read transaction on the data connection. The replication connection MUST stay
        //    open until SET TRANSACTION SNAPSHOT has run (the snapshot is valid only while the exporting
        //    connection lives); after the import it is disposed.
        var replication = new LogicalReplicationConnection(connectionString);
        var connection = new NpgsqlConnection(connectionString);
        try
        {
            await replication.Open(ct).ConfigureAwait(false);
            PgOutputReplicationSlot slot;
            try
            {
                slot = await replication.CreatePgOutputReplicationSlot(
                    slotName, temporarySlot: false,
                    slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                    cancellationToken: ct).ConfigureAwait(false);
            }
            catch (PostgresException ex)
            {
                throw new PzConnectorException(
                    $"postgres cdc: replication slot '{slotName}' creation failed: {ex.MessageText}",
                    ex.IsTransient, innerException: ex);
            }

            _syncCandidate = PostgresCdc.FormatLsn(slot.ConsistentPoint);
            var snapshotName = slot.SnapshotName
                ?? throw new PzConnectorException(
                    $"postgres cdc: replication slot '{slotName}' did not export a snapshot name",
                    isTransient: false);

            await connection.OpenAsync(ct).ConfigureAwait(false);
            await ExecuteAsync(connection, "begin isolation level repeatable read", ct).ConfigureAwait(false);
            // snapshotName is a server-generated identifier (e.g. "00000003-0000001B-1"); quote-double
            // defensively anyway, per the file-wide injection-safety discipline.
            await ExecuteAsync(connection, $"set transaction snapshot '{snapshotName.Replace("'", "''")}'", ct)
                .ConfigureAwait(false);
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            await replication.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        // Snapshot imported into the data transaction -- the exporting replication connection is no
        // longer needed.
        await replication.DisposeAsync().ConfigureAwait(false);

        try
        {
            var command = new NpgsqlCommand(PostgresCdc.SnapshotSelect(schema, table), connection);
            var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            return (connection, reader);
        }
        catch (NpgsqlException ex)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw new PzConnectorException(
                $"postgres cdc snapshot read failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
