using Apache.Arrow;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>SQL Server sink: one connection + transaction per session so abort = rollback
/// for every mode. append bulk-loads the target DIRECTLY (no staging); replace TRUNCATEs (metadata-op,
/// transactional; DELETE fallback for FK-referenced or TRUNCATE-permission-less targets, decided by a
/// pre-check — see <see cref="ClearTargetAsync"/>) then bulk-loads the target; merge stages into a heap
/// #temp (with a trailing identity ordinal for last-writer-wins dedup), clusters it on the keys AFTER
/// the load, then one set-based MERGE WITH (HOLDLOCK) over the key-deduped staging rows. SqlBulkCopy is
/// created once per session, EnableStreaming, explicit column mappings by name; TABLOCK per the
/// 'tablock' output option (default true) on append/replace, always on for the session-private
/// #temp. <see cref="SqlServerSinkWriteSession"/> also implements <see
/// cref="IDeleteApplyingWriteSession"/> (<see cref="ConnectorCapabilities.ApplyDeletes"/>) -- hard/soft
/// delete-key application via a second SqlBulkCopy into a <c>#pz_del</c> temp table, applied in the
/// same transaction after the merge.</summary>
internal sealed class SqlServerSink(string connectionString) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        // Unreachable via pz run (PZ0228/PZ0324 refuse earlier); kept as ABI defense-in-depth.
        if (spec.Mode is not ("append" or "replace" or "merge"))
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': sqlserver sink supports only 'append'/'replace'/'merge' write " +
                $"modes (got '{spec.Mode}')",
                isTransient: false);
        }

        if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
        {
            var fieldNames = new HashSet<string>(schema.FieldsList.Select(f => f.Name), StringComparer.Ordinal);
            var missingKeys = spec.Keys.Where(k => !fieldNames.Contains(k)).ToArray();
            if (missingKeys.Length > 0)
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': merge key column(s) [{string.Join(", ", missingKeys)}] are " +
                    "not present in the write schema",
                    isTransient: false);
            }

            if (fieldNames.Contains(MsDdl.StagingSequenceColumn))
            {
                throw new PzConnectorException(
                    $"output '{spec.Output}': column name '{MsDdl.StagingSequenceColumn}' is reserved by " +
                    "the sqlserver sink's merge staging -- hint: rename the column in the pipeline SQL",
                    isTransient: false);
            }
        }

        // The entity name is the target.
        var (msSchema, table) = MsDdl.SplitEntity(spec.Output);
        var tablock = ParseTablock(spec);
        // Resolved offline (no connection needed) so a malformed `columns:` entry fails before any
        // network round-trip -- see MsEffectiveTypes' doc comment.
        var resolved = MsEffectiveTypes.Resolve(spec, schema);

        var connection = new SqlConnection(connectionString);
        SqlBulkCopy? bulk = null;
        SqlTransaction? tx = null;
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
            tx = (SqlTransaction)await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

            // Existing target's canonical columns, when the table pre-existed -- feeds StagingTypes
            // below so the merge staging #temp mirrors the target's actual sizes.
            var existingColumns = await MsDdl.EnsureTargetAsync(
                connection, tx, spec.SchemaPolicy, msSchema, table, schema, spec.Output, spec.Mode, spec.Keys,
                spec.OnDelete, resolved, ct)
                .ConfigureAwait(false);

            var quotedTarget = $"{MsDdl.Quote(msSchema)}.{MsDdl.Quote(table)}";
            string bulkDestination;
            string? stagingTable = null;

            if (string.Equals(spec.Mode, "merge", StringComparison.Ordinal))
            {
                stagingTable = $"#pz_tmp_{Guid.NewGuid().ToString("N")[..8]}";
                // Trailing identity ordinal = last-writer-wins tiebreaker (see MsDdl.StagingSequenceColumn);
                // SqlBulkCopy's explicit column mappings leave it unmapped, so it autofills in arrival order.
                await using var create = new SqlCommand(
                    $"create table {MsDdl.Quote(stagingTable)} ({MsDdl.BuildColumnListSql(schema, StagingTypes(schema, resolved, existingColumns))}, " +
                    $"{MsDdl.Quote(MsDdl.StagingSequenceColumn)} bigint identity(1,1))", connection, tx);
                await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                bulkDestination = MsDdl.Quote(stagingTable);
            }
            else
            {
                if (string.Equals(spec.Mode, "replace", StringComparison.Ordinal))
                {
                    await ClearTargetAsync(connection, tx, quotedTarget, ct).ConfigureAwait(false);
                }

                bulkDestination = quotedTarget;
            }

            var useTablock = stagingTable is not null || tablock; // #temp is session-private: always TABLOCK
            var options = SqlBulkCopyOptions.KeepNulls | (useTablock ? SqlBulkCopyOptions.TableLock : SqlBulkCopyOptions.Default);
            bulk = new SqlBulkCopy(connection, options, tx)
            {
                DestinationTableName = bulkDestination,
                EnableStreaming = true,
                BulkCopyTimeout = 0,
            };
            foreach (var field in schema.FieldsList)
            {
                bulk.ColumnMappings.Add(field.Name, field.Name);
            }

            return new SqlServerSinkWriteSession(connection, tx, bulk, quotedTarget, stagingTable, schema, spec);
        }
        catch (SqlException ex)
        {
            await CleanupAsync(bulk, tx, connection).ConfigureAwait(false);
            throw new PzConnectorException(
                $"output '{spec.Output}': sqlserver sink open failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
        catch
        {
            await CleanupAsync(bulk, tx, connection).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Sink output options are not schema-validated, so a malformed 'tablock'
    /// value reaches here unchecked -- parse it the same way <c>SqlServerSource.ParsePartitionCount</c>
    /// parses 'partitions': a bad value is a named, non-transient <see cref="PzConnectorException"/>,
    /// never a raw .NET exception surfacing to the user.</summary>
    private static bool ParseTablock(OutputSpec spec)
    {
        if (!spec.Options.TryGetValue("tablock", out var tl) || tl is null)
        {
            return true;
        }

        try
        {
            return Convert.ToBoolean(tl, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': 'tablock' must be a boolean, got '{tl}'", isTransient: false);
        }
    }

    /// <summary>TRUNCATE when possible (transactional metadata-op, O(1) log); DELETE for FK-referenced
    /// targets or missing ALTER permission — identical observable outcome, different speed/locks.
    /// The choice MUST be made by pre-checking, not by catching TRUNCATE's failure: error 4712
    /// (FK-referenced) DOOMS the enclosing transaction (xact_state() = -1; verified empirically — even a
    /// server-side TRY/CATCH cannot DELETE afterwards, error 3930), so a reactive fallback can never keep
    /// "DELETE in the same transaction" alive. sys.foreign_keys detects FK references;
    /// has_perms_by_name(..., 'ALTER') covers TRUNCATE's documented permission requirement (its NULL
    /// result — object invisible to the caller — conservatively picks DELETE).</summary>
    private static async Task ClearTargetAsync(
        SqlConnection connection, SqlTransaction tx, string quotedTarget, CancellationToken ct)
    {
        const string probeSql =
            "select case when exists (select 1 from sys.foreign_keys where referenced_object_id = object_id(@target)) " +
            "or isnull(has_perms_by_name(@target, 'OBJECT', 'ALTER'), 0) = 0 then 1 else 0 end";
        bool useDelete;
        await using (var probe = new SqlCommand(probeSql, connection, tx))
        {
            probe.Parameters.AddWithValue("@target", quotedTarget);
            useDelete = (int)(await probe.ExecuteScalarAsync(ct).ConfigureAwait(false))! == 1;
        }

        var clearSql = useDelete ? $"delete from {quotedTarget}" : $"truncate table {quotedTarget}";
        await using var clear = new SqlCommand(clearSql, connection, tx) { CommandTimeout = 0 };
        // Infinite timeout: TRUNCATE can wait on a Sch-M lock behind concurrent readers, and the
        // DELETE fallback is per-row logged over a potentially large target -- neither is bounded
        // by the default 30s command timeout.
        await clear.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async ValueTask CleanupAsync(SqlBulkCopy? bulk, SqlTransaction? tx, SqlConnection connection)
    {
        if (bulk is not null)
        {
            ((IDisposable)bulk).Dispose();
        }

        if (tx is not null)
        {
            await tx.DisposeAsync().ConfigureAwait(false);
        }

        await connection.DisposeAsync().ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => default;

    /// <summary>The merge staging #temp's string columns mirror the EFFECTIVE
    /// TARGET -- the existing table's actual (string-kind) types when it pre-existed, else the
    /// resolved types the create path just used -- so the bulk load into #temp never pays the
    /// nvarchar(max) LOB path against a sized target. Non-string columns keep the resolved default.
    /// A declared column is identical in both maps whenever fail_on_change passed, so declared
    /// trivially wins.</summary>
    internal static IReadOnlyDictionary<string, string> StagingTypes(
        Schema schema, MsResolvedTypes resolved, IReadOnlyDictionary<string, string>? existingColumns)
    {
        var types = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var field in schema.FieldsList)
        {
            types[field.Name] = existingColumns is not null
                && field.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.String
                && existingColumns.TryGetValue(field.Name, out var actual)
                && (actual.StartsWith("nvarchar(", StringComparison.OrdinalIgnoreCase)
                    || actual.StartsWith("varchar(", StringComparison.OrdinalIgnoreCase)
                    || actual.StartsWith("char(", StringComparison.OrdinalIgnoreCase)
                    || actual.StartsWith("nchar(", StringComparison.OrdinalIgnoreCase))
                ? actual
                : resolved.Types[field.Name];
        }

        return types;
    }

    /// <summary>SQL Server 2628/8152 ("string or binary data would be truncated") gets the
    /// sized-DDL remediation appended; every other bulk error keeps the plain wrap.</summary>
    internal static string BuildBulkWriteMessage(int sqlErrorNumber, string rawMessage, string output) =>
        sqlErrorNumber is 2628 or 8152
            ? $"output '{output}': sqlserver bulk write failed: {rawMessage} -- hint: a value exceeds " +
              "the target column's size; widen the column (alter table ... alter column) or declare " +
              "a larger type in the output's columns: option"
            : $"output '{output}': sqlserver bulk write failed: {rawMessage}";
}

internal enum MsSinkSessionState
{
    Open,
    Committed,
    Aborted,
}

internal sealed class SqlServerSinkWriteSession(
    SqlConnection connection, SqlTransaction tx, SqlBulkCopy bulk,
    string quotedTarget, string? stagingTable, Schema schema, OutputSpec spec) : IDeleteApplyingWriteSession
{
    private readonly string[] _quotedKeyColumns = spec.Keys.Select(MsDdl.Quote).ToArray();

    private MsSinkSessionState _state = MsSinkSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;
    private SqlBulkCopy? _deleteBulk;
    private string? _deleteStagingTable;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        try
        {
            using var reader = new ArrowBatchDataReader(batch);
            await bulk.WriteToServerAsync(reader, ct).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                SqlServerSink.BuildBulkWriteMessage(ex.Number, ex.Message, spec.Output), ex.IsTransient, innerException: ex);
        }
        catch (InvalidOperationException ex) when (ex.GetType() == typeof(InvalidOperationException))
        {
            // Exact-type match only: SqlBulkCopy throws plain InvalidOperationException for a staged
            // value incompatible with the declared/target column type. Subclasses like
            // ObjectDisposedException are session-state failures, not conversion errors, and propagate
            // raw.
            throw new PzConnectorException(
                $"output '{spec.Output}': sqlserver bulk write failed: {ex.Message} -- hint: a staged " +
                "value is incompatible with the declared/target column type; check the output's " +
                "columns: option", isTransient: false, innerException: ex);
        }

        _rowsWritten += batch.Length;
        _batchesWritten++;
    }

    /// <summary>Streams one delete-key batch into a lazily-created <c>#pz_del</c>
    /// temp table, reusing the session's row-writing machinery (a second <see cref="SqlBulkCopy"/>,
    /// same shape as <paramref name="bulk"/>) rather than inventing a new importer. Column set/types
    /// come from the arriving batch's own schema (captured once, on the first call) -- the engine
    /// guarantees this is exactly the output's declared merge keys, in declaration order. Applied at
    /// <see cref="CommitAsync"/>, after the merge finalizes -- see the delete-apply block there.</summary>
    public async ValueTask ApplyDeleteKeysAsync(RecordBatch keyBatch, CancellationToken ct)
    {
        EnsureOpen("apply delete keys to");
        try
        {
            if (_deleteBulk is null)
            {
                _deleteStagingTable = $"#pz_del_{Guid.NewGuid().ToString("N")[..8]}";
                // Delete-key columns are exactly the merge keys (doc comment above) -- outside the
                // sized-text-DDL path, so sized via DdlType.
                var keyTypes = keyBatch.Schema.FieldsList.ToDictionary(f => f.Name, f => MsDdl.DdlType(f));
                await using (var create = new SqlCommand(
                    $"create table {MsDdl.Quote(_deleteStagingTable)} ({MsDdl.BuildColumnListSql(keyBatch.Schema, keyTypes)})",
                    connection, tx))
                {
                    await create.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                _deleteBulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock, tx)
                {
                    DestinationTableName = MsDdl.Quote(_deleteStagingTable),
                    EnableStreaming = true,
                    BulkCopyTimeout = 0,
                };
                foreach (var field in keyBatch.Schema.FieldsList)
                {
                    _deleteBulk.ColumnMappings.Add(field.Name, field.Name);
                }
            }

            using var reader = new ArrowBatchDataReader(keyBatch);
            await _deleteBulk.WriteToServerAsync(reader, ct).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': sqlserver delete-key apply failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;
        try
        {
            if (stagingTable is not null)
            {
                // Heap-then-index: one set-based sort after the load beats per-row clustered-index
                // maintenance during it, and only then the single MERGE.
                var quotedStaging = MsDdl.Quote(stagingTable);
                // Trailing __pz_seq keeps the index covering the dedup window's full sort order
                // (partition by keys, order by __pz_seq desc), so the row_number dedup is sort-free.
                var keyList = string.Join(", ", spec.Keys.Select(MsDdl.Quote)
                    .Append(MsDdl.Quote(MsDdl.StagingSequenceColumn)));
                await using (var index = new SqlCommand(
                    $"create clustered index [cx_keys] on {quotedStaging} ({keyList})", connection, tx)
                {
                    CommandTimeout = 0, // one big sort over the staged rows -- not bounded by the default 30s
                })
                {
                    await index.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                await using var merge = new SqlCommand(
                    MsDdl.BuildMergeSql(quotedTarget, quotedStaging, schema, spec.Keys, spec.OnDelete), connection, tx);
                merge.CommandTimeout = 0;
                await merge.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // Deletes apply AFTER the upsert, same transaction: upserts first, deletes second -- a
            // same-window key never appears in both sets, but a soft-then-later-reupsert across
            // sessions relies on the merge's SET list clearing the flag, not on ordering here.
            // Plain DELETE/UPDATE ... FROM ... JOIN, not MERGE: a
            // duplicate key in #pz_del (idempotent replay) would make MERGE's WHEN MATCHED raise
            // "cannot affect row a second time" (error 8672); a join-based statement instead just
            // touches the target row once (or an unspecified-but-single one of the matches), no error.
            if (_deleteStagingTable is not null)
            {
                if (_deleteBulk is not null)
                {
                    ((IDisposable)_deleteBulk).Dispose();
                    _deleteBulk = null;
                }

                var quotedDelTmp = MsDdl.Quote(_deleteStagingTable);
                var joinCond = string.Join(" and ", _quotedKeyColumns.Select(c => $"t.{c} = d.{c}"));
                var deleteSql = spec.OnDelete switch
                {
                    "delete" => $"delete t from {quotedTarget} as t inner join {quotedDelTmp} as d on {joinCond};",
                    "soft" => $"update t set {MsDdl.Quote(MsDdl.SoftDeleteColumn)} = sysutcdatetime() " +
                        $"from {quotedTarget} as t inner join {quotedDelTmp} as d on {joinCond};",
                    _ => throw new PzConnectorException(
                        $"output '{spec.Output}': on_delete '{spec.OnDelete}' has no delete-apply statement",
                        isTransient: false),
                };
                await using var apply = new SqlCommand(deleteSql, connection, tx) { CommandTimeout = 0 };
                await apply.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
            _state = MsSinkSessionState.Committed;
            return new WriteResult(_rowsWritten, _batchesWritten);
        }
        catch (SqlException ex)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': sqlserver commit failed: {ex.Message}", ex.IsTransient, innerException: ex);
        }
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");
        _state = MsSinkSessionState.Aborted;
        await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_state == MsSinkSessionState.Open && !_commitAttempted)
        {
            _state = MsSinkSessionState.Aborted;
            try
            {
                await tx.RollbackAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is InvalidOperationException or SqlException)
            {
                // Rollback on an already-broken connection: disposal below still releases everything.
            }
        }

        ((IDisposable)bulk).Dispose();
        if (_deleteBulk is not null)
        {
            ((IDisposable)_deleteBulk).Dispose();
            _deleteBulk = null;
        }

        await tx.DisposeAsync().ConfigureAwait(false);
        await connection.DisposeAsync().ConfigureAwait(false);
    }

    private void EnsureOpen(string action)
    {
        if (_state != MsSinkSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }

        if (_commitAttempted)
        {
            throw new InvalidOperationException($"cannot {action} a session whose commit was already attempted");
        }
    }
}
