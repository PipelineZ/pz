using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Memory;
using DuckDB.NET.Data;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.DuckDb;

/// <summary>
/// A disk-backed DuckDB session. DuckDB.NET's ADO.NET surface is synchronous under the
/// hood, so the async members here are implemented as sync-over-async via <see cref="Task.Run(Action)"/>,
/// offloading the blocking ADO.NET call to a thread-pool thread rather than blocking the caller.
/// </summary>
public sealed class DuckSession : IDuckSession
{
    private readonly DuckDBConnection _connection;

    // A single duckdb_connection is NOT safe for concurrent statement execution from multiple threads —
    // DuckDB's own C API contract is "used by one thread at a time" per connection. RunOrchestrator
    // dispatches nodes up to RunOptions.MaxConcurrency (engine.threads) concurrently, and every node
    // executor shares this ONE DuckSession/_connection, so two SourceLoad/SinkWrite nodes ingesting or
    // querying at the same time race directly on the native connection's pending-query state —
    // surfacing as a nondeterministic "duckdb_appender_close failed: Attempting to execute an
    // unsuccessful or closed pending query result" (PZ0501), a genuine node failure. This gate
    // serializes every operation that touches `_connection`/`NativeConnection` end-to-end (including the
    // full streaming duration of ingest/egress, since a live pending result is per-connection state that
    // must not be interleaved with any other statement) so concurrent nodes queue instead of corrupting
    // shared state. Correctness, not performance: it trades away genuine DB-level overlap between
    // concurrent nodes for the connection's exclusive-use contract — a known perf caveat. Recovering
    // that overlap would take a connection per concurrent node, which DuckDB supports against the same
    // attached database.
    private readonly SemaphoreSlim _gate = new(1, 1);

    private DuckSession(DuckDBConnection connection)
    {
        _connection = connection;
    }

    /// <summary>Fixed catalog alias every session attaches <paramref name="databasePath"/> under (see
    /// <see cref="Open"/>) — never a name callers would also choose for a schema.</summary>
    private const string CatalogAlias = "pz";

    public static DuckSession Open(string databasePath, DuckOptions? options = null)
    {
        // Connecting directly to `databasePath` (`Data Source={databasePath}`) makes DuckDB name the
        // default catalog after the file's basename — e.g. "staging" for ".../staging.duckdb", which is
        // exactly the physical filename the engine layer's RunPaths convention always uses. Callers then
        // create and reference a schema also literally named "staging" (the engine's staging-table
        // convention). Once a catalog and a schema share a name, any two-part "schema.table" DDL
        // reference (CREATE TABLE/VIEW) becomes genuinely ambiguous to DuckDB's binder — it cannot tell
        // whether "staging" names the catalog or the schema (reads are unaffected; only DDL hits this).
        // Opening `:memory:` as the ADO connection and ATTACHing the real file under a fixed alias that
        // no caller-chosen schema name can collide with, then switching to it, sidesteps the collision
        // entirely while still persisting every statement to the same physical file.
        var connection = new DuckDBConnection("Data Source=:memory:");
        try
        {
            connection.Open();

            var session = new DuckSession(connection);
            session.AttachDatabase(databasePath);
            session.ApplyOptions(options);
            return session;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private void AttachDatabase(string databasePath)
    {
        ExecuteSync($"ATTACH {QuoteLiteral(databasePath)} AS {CatalogAlias}");
        ExecuteSync($"USE {CatalogAlias}");
    }

    private void ApplyOptions(DuckOptions? options)
    {
        if (options is null)
        {
            return;
        }

        if (options.MemoryLimit is not null)
        {
            ExecuteSync($"SET memory_limit = {QuoteLiteral(options.MemoryLimit)}");
        }

        if (options.Threads is not null)
        {
            ExecuteSync($"SET threads = {options.Threads.Value.ToString(CultureInfo.InvariantCulture)}");
        }

        if (options.TempDirectory is not null)
        {
            ExecuteSync($"SET temp_directory = {QuoteLiteral(options.TempDirectory)}");
        }
    }

    private static string QuoteLiteral(string value) => "'" + value.Replace("'", "''") + "'";

    private void ExecuteSync(string sql, CancellationToken ct = default)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = sql;
        Interruptible(command, ct, static c => c.ExecuteNonQuery());
    }

    /// <summary>Runs a statement so that cancelling <paramref name="ct"/> interrupts it INSIDE DuckDB.
    /// The calls here are synchronous and never look at a token, so without this a cancelled node — a
    /// node timeout, Ctrl-C — waits for a runaway query to finish on its own, holding the gate the
    /// whole time. The caller holds the gate, so the statement being interrupted is always its own;
    /// DuckDB clears the interrupt flag when the next statement starts, so a cancel that lands just
    /// after this one finished cannot leak into another node's statement. DuckDB reports the interrupt
    /// as an ordinary error; with the token cancelled it is surfaced as the cancellation it is.</summary>
    private static T Interruptible<T>(DuckDBCommand command, CancellationToken ct, Func<DuckDBCommand, T> run)
    {
        using var registration = ct.UnsafeRegister(static state => ((DuckDBCommand)state!).Cancel(), command);
        try
        {
            return run(command);
        }
        catch (DuckDBException) when (ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ct);
        }
    }

    public Task ExecuteAsync(string sql, CancellationToken ct = default)
    {
        return Task.Run(
            () =>
            {
                _gate.Wait(ct);
                try
                {
                    ExecuteSync(sql, ct);
                }
                finally
                {
                    _gate.Release();
                }
            },
            ct);
    }

    public Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default)
    {
        return Task.Run(
            () =>
            {
                _gate.Wait(ct);
                try
                {
                    using var command = _connection.CreateCommand();
                    command.CommandText = sql;
                    var result = Interruptible(command, ct, static c => c.ExecuteScalar());
                    return (T)Convert.ChangeType(result!, typeof(T), CultureInfo.InvariantCulture);
                }
                finally
                {
                    _gate.Release();
                }
            },
            ct);
    }

    public ValueTask DisposeAsync()
    {
        _connection.Dispose();
        _gate.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Test-only hook invoked (on the ingest background thread) after each Arrow batch has been
    /// fully consumed — appended and disposed. An instance member (not static) so parallel test classes
    /// exercising ingest concurrently don't race on a shared hook.</summary>
    internal Action? OnBatchConsumedForTests { get; set; }

    public Task<long> IngestArrowAsync(
        string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct = default)
    {
        return Task.Run(() => IngestArrowGatedAsync(targetTable, schema, batches, ct), ct);
    }

    /// <summary>Gate held for the whole ingest (see <see cref="_gate"/>'s doc comment) — including while
    /// awaiting the next upstream batch — since a partially-appended table is per-connection pending
    /// state that must not interleave with any other statement on <see cref="_connection"/>.</summary>
    private async Task<long> IngestArrowGatedAsync(
        string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await IngestArrowCoreAsync(targetTable, schema, batches, ct).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<long> IngestArrowCoreAsync(
        string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        ExecuteSync(ArrowInterop.BuildCreateTableSql(targetTable, schema));

        // From here on, the target table exists. A failed or cancelled ingest must leave nothing behind,
        // so any exception (including OperationCanceledException) that escapes the ingest below is
        // followed by a best-effort DROP TABLE before propagating — see DropTableBestEffort. A
        // successful return is the only way the table survives.
        try
        {
            return await IngestBatchesAsync(targetTable, schema, batches, ct).ConfigureAwait(false);
        }
        catch
        {
            DropTableBestEffort(targetTable);
            throw;
        }
    }

    private async Task<long> IngestBatchesAsync(
        string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        var connectionHandle = _connection.NativeConnection.DangerousGetHandle();
        using var writer = ArrowInterop.ArrowIngestWriter.Create(connectionHandle, targetTable, schema);

        var rows = 0L;
        await foreach (var batch in batches.WithCancellation(ct).ConfigureAwait(false))
        {
            try
            {
                ct.ThrowIfCancellationRequested();
                writer.AppendBatch(batch);
                rows += batch.Length;
                OnBatchConsumedForTests?.Invoke();
            }
            finally
            {
                batch.Dispose();
            }
        }

        writer.Complete();
        return rows;

        // `writer` is disposed (via the `using` above) as part of stack unwinding before any exception
        // reaches IngestArrowCoreAsync's catch, so the appender/converted-schema are always released
        // before the DROP TABLE below runs.
    }

    /// <summary>Best-effort cleanup after a failed or cancelled ingest: drops the table the failed ingest
    /// created so nothing half-written survives. Failures here are swallowed — the original ingest
    /// exception must reach the caller untouched even if the drop itself fails (e.g. the connection is no
    /// longer usable).</summary>
    private void DropTableBestEffort(string targetTable)
    {
        try
        {
            ExecuteSync($"DROP TABLE IF EXISTS {ArrowInterop.QuoteQualified(targetTable)}");
        }
        catch
        {
            // Suppressed by design: never mask the original ingest failure with a cleanup failure.
        }
    }

    public Task CreateEmptyTableAsync(string targetTable, Schema schema, CancellationToken ct = default)
    {
        return Task.Run(
            () =>
            {
                _gate.Wait(ct);
                try
                {
                    ExecuteSync(ArrowInterop.BuildCreateTableSql(targetTable, schema));
                }
                finally
                {
                    _gate.Release();
                }
            },
            ct);
    }

    public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, Schema? schema = null,
        CancellationToken ct = default)
    {
        // Deliberately NOT passing `ct` as Task.Run's own cancellation token: if `ct` is already
        // cancelled when this is called, Task.Run(Action, CancellationToken) never invokes the
        // delegate at all — which would skip the `finally { batch.Dispose(); }` below and leak the
        // engine-owned pooled Arrow batch (this method's contract, and IDuckSession's doc comment,
        // require the batch to always be disposed, success or failure). Cancellation responsiveness
        // is preserved by `_gate.Wait(ct)` and the explicit `ThrowIfCancellationRequested()` inside
        // the delegate, which the (always-running) finally then unwinds through.
        return Task.Run(
            () =>
            {
                try
                {
                    _gate.Wait(ct);
                    try
                    {
                        ct.ThrowIfCancellationRequested();
                        var connectionHandle = _connection.NativeConnection.DangerousGetHandle();
                        // A fresh appender per call: appender state is per-connection pending state
                        // (see _gate's doc comment) and must not span gate releases. Close errors
                        // (duckdb_appender_close) surface from Complete() inside this same call.
                        using var writer = ArrowInterop.ArrowIngestWriter.Create(connectionHandle, targetTable, schema ?? batch.Schema);
                        writer.AppendBatch(batch);
                        writer.Complete();
                        return (long)batch.Length;
                    }
                    finally
                    {
                        _gate.Release();
                    }
                }
                finally
                {
                    batch.Dispose();
                }
            });
    }

    public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default)
    {
        return Task.Run(
            () =>
            {
                _gate.Wait(ct);
                try
                {
                    ExecuteSync("BEGIN TRANSACTION");
                    try
                    {
                        foreach (var sql in statements)
                        {
                            ct.ThrowIfCancellationRequested();
                            ExecuteSync(sql, ct);
                        }

                        ExecuteSync("COMMIT");
                    }
                    catch
                    {
                        try
                        {
                            ExecuteSync("ROLLBACK");
                        }
                        catch
                        {
                            // Best-effort by design: never mask the original transaction failure.
                        }

                        throw;
                    }
                }
                finally
                {
                    _gate.Release();
                }
            },
            ct);
    }

    /// <summary>Binds/plans <paramref name="sql"/> without fetching any rows (<c>limit 0</c>) and derives
    /// its result <see cref="Schema"/> via <see cref="DuckDBCommand.ExecuteArrowStream"/> +
    /// <see cref="ArrowInterop.NormalizeNativeArrowSchema"/>. Used only by
    /// <see cref="GetResultSchemaAsync"/>. Deliberately NOT <c>ExecuteReader()</c> +
    /// <c>DuckDBDataReader.GetSchemaTable()</c>: that lookup silently reports precision=0/scale=0 for a
    /// DECIMAL column in ANY zero-row result -- not specific to the `limit 0` wrap; DuckDB.NET's
    /// <c>GetSchemaTable()</c> reads a DECIMAL column's precision/scale off the first fetched data
    /// chunk's own vector rather than off the query's binder-known result schema, and a zero-row result
    /// has no data chunk to read it from. DuckDB itself reports the correct precision/scale for this
    /// exact query shape (confirmed via `DESCRIBE`), so the defect is DuckDB.NET-side.
    /// `ExecuteArrowStream()` (DuckDB.NET.Data.Full >= 1.5.5) sidesteps it entirely: its Schema is built
    /// from `duckdb_column_logical_type` -- the same binder-known result metadata `DESCRIBE` reads --
    /// before any chunk is fetched, so it is correct regardless of row count. See
    /// <see cref="ArrowInterop.NormalizeNativeArrowSchema"/> for the v0-type-matrix validation and the
    /// TIMESTAMP-timezone normalization this needs. Caller must hold <see cref="_gate"/>.</summary>
    private Schema PeekSchema(string sql)
    {
        using var command = _connection.CreateCommand();
        command.CommandText = $"select * from ({sql}) q limit 0";
        using var stream = command.ExecuteArrowStream();
        return ArrowInterop.NormalizeNativeArrowSchema(stream.Schema);
    }

    public Task<Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default)
    {
        return Task.Run(
            () =>
            {
                _gate.Wait(ct);
                try
                {
                    return PeekSchema(sql);
                }
                finally
                {
                    _gate.Release();
                }
            },
            ct);
    }

    /// <summary>Test-only hook invoked (on the egress producer thread) each time a batch is written to the
    /// channel, with the cumulative number of rows read from the DuckDB reader so far (including the batch
    /// just written). An instance member (not static) so parallel test classes exercising egress
    /// concurrently don't race on a shared hook. Mirrors <see cref="OnBatchConsumedForTests"/> on the
    /// ingest side; exists to let tests observe that the first batch reaches the channel well before the
    /// whole result has been read — proof the producer streams rather than buffers the full result.</summary>
    internal Action<long>? OnEgressBatchProducedForTests { get; set; }

    /// <summary>Allocator every egress output batch's buffers are rented from. Settable so a test can
    /// observe exactly the memory one session's egress rents and returns, on an allocator no other
    /// concurrently running test shares.</summary>
    internal MemoryAllocator EgressAllocator { get; set; } = PooledNativeAllocator.Shared;

    public IAsyncEnumerable<RecordBatch> QueryArrowAsync(
        string sql, int targetBatchBytes = 32 * 1024 * 1024, CancellationToken ct = default)
    {
        return QueryArrowCoreAsync(sql, targetBatchBytes, ct);
    }

    private async IAsyncEnumerable<RecordBatch> QueryArrowCoreAsync(
        string sql, int targetBatchBytes, [EnumeratorCancellation] CancellationToken ct)
    {
        // Bounded(1): the producer thread below may build at most one batch ahead of what the consumer
        // has taken, so this is genuine streaming (never buffers the full result) — the same guarantee
        // IngestArrowAsync's `await foreach` gives on the ingest side.
        var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(1)
        {
            SingleReader = true,
            SingleWriter = true,
        });

        // Cancelling this token (below, in `finally`) is how the producer thread is told to stop even when
        // enumeration ends for a reason other than the caller's `ct` firing (a normal exhausted stream, an
        // exception surfaced through the channel itself, or the consumer simply not finishing the
        // `await foreach`, e.g. via `break`) — otherwise a producer blocked on `channel.Writer.WriteAsync`
        // past a `break` would never learn nobody is reading anymore.
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var producer = Task.Run(
            () => ProduceArrowBatchesAsync(sql, targetBatchBytes, channel.Writer, linkedCts.Token),
            CancellationToken.None);

        try
        {
            await foreach (var batch in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return batch;
            }
        }
        finally
        {
            linkedCts.Cancel();
            try
            {
                // Joins the producer so its `DuckDBCommand`/`DuckDBDataReader` are disposed before this
                // method returns control to the caller (important if the caller disposes the session right
                // after, e.g. cancellation tests). Any exception here was already the reason
                // `channel.Reader.ReadAllAsync` above threw (query/mapping failure propagates through the
                // channel's completion), or is a benign `OperationCanceledException` from `linkedCts` on an
                // early-exit path with no external cancellation — either way it must not replace whatever
                // already escaped the `try` block above.
                await producer.ConfigureAwait(false);
            }
            catch
            {
                // Intentionally swallowed — see comment above.
            }

            // The producer has finished, so nothing is written after this. A batch still buffered in the
            // channel was never handed to the consumer and is still engine-owned: release it here.
            while (channel.Reader.TryRead(out var unread))
            {
                unread.Dispose();
            }
        }
    }

    /// <summary>Runs on a thread-pool thread (via <see cref="Task.Run(Func{Task})"/> in
    /// <see cref="QueryArrowCoreAsync"/>). Drives DuckDB.NET's
    /// <see cref="DuckDBCommand.ExecuteArrowBatchesAsync"/> (bulk, columnar — DuckDB's own Arrow C Data
    /// Interface export, one native call per ~2048-row data chunk, no per-cell P/Invoke) rather than a
    /// row-by-row <c>ExecuteReader</c>/<c>Read</c> loop. DuckDB's own chunk size does not respect
    /// <paramref name="targetBatchBytes"/> (byte-targeted adaptive sizing, chosen over fixed row counts),
    /// so chunks are coalesced by <see cref="ArrowChunkCoalescer"/>, which concatenates whole columns:
    /// one buffer copy per column per output batch, no per-cell decode/re-encode, and every value type
    /// DuckDB exports carried bit-for-bit (a DECIMAL(38) beyond <see cref="decimal"/>'s range included).
    /// Output buffers come from <see cref="EgressAllocator"/> (pooled native memory), never from DuckDB's
    /// chunk memory, so a chunk's lifetime ends at the concatenation that consumes its last row and an
    /// output batch is engine-owned independently of the DuckDB result.
    /// <see cref="ArrowBatchBuilder"/>'s constructor, run once over the first chunk's schema, is what
    /// enforces the v0 type matrix (its <c>NotSupportedException</c> names the column); the builder is
    /// otherwise unused. <see cref="DuckDBCommand.UseStreamingMode"/> is load-bearing here — without it
    /// DuckDB.NET materializes the whole result before yielding anything, silently reintroducing full
    /// buffering.
    ///
    /// The schema is taken from the FIRST incoming chunk's own <see cref="RecordBatch.Schema"/> — NOT via
    /// <see cref="PeekSchema"/> (<see cref="GetResultSchemaAsync"/>'s limit-0 peek), because deriving from
    /// the first real chunk avoids planning/binding <paramref name="sql"/> twice.
    ///
    /// Ownership: each DuckDB chunk passes to the coalescer as soon as it arrives, and the coalescer is
    /// disposed in <c>finally</c> (releasing any chunk still held) before the command is. A batch whose
    /// channel write fails is disposed here, never handed on.
    ///
    /// Checks <paramref name="ct"/> once per chunk (in addition to DuckDB.NET's own check per fetch). Any
    /// exception (including <see cref="OperationCanceledException"/>) is captured and handed to
    /// <see cref="ChannelWriter{T}.Complete(Exception?)"/> so it surfaces to the consumer through the
    /// channel rather than becoming an unobserved task exception.</summary>
    private async Task ProduceArrowBatchesAsync(
        string sql, int targetBatchBytes, ChannelWriter<RecordBatch> writer, CancellationToken ct)
    {
        Exception? error = null;
        var gateAcquired = false;
        try
        {
            // Gate held for the whole egress stream (see `_gate`'s doc comment): a live pending Arrow
            // batch stream is per-connection state that must not interleave with any other statement on
            // `_connection`, so this holds the gate from before ExecuteArrowBatchesAsync() until the
            // stream is fully drained below, not just around individual native calls.
            await _gate.WaitAsync(ct).ConfigureAwait(false);
            gateAcquired = true;

            using var command = _connection.CreateCommand();
            command.CommandText = sql;
            command.UseStreamingMode = true;

            ArrowChunkCoalescer? coalescer = null;
            try
            {
                var rowsRead = 0L;

                await foreach (var duckBatch in command.ExecuteArrowBatchesAsync(ct).ConfigureAwait(false))
                {
                    if (coalescer is null)
                    {
                        // First chunk fixes the schema for the whole stream (ADO.NET/Arrow-stream
                        // convention — DuckDB never changes a result's column types mid-stream). An
                        // unsupported column type fails fast here, in ArrowBatchBuilder's own
                        // constructor, before any row is processed.
                        try
                        {
                            _ = new ArrowBatchBuilder(duckBatch.Schema, targetBatchBytes);
                        }
                        catch
                        {
                            duckBatch.Dispose();
                            throw;
                        }

                        coalescer = new ArrowChunkCoalescer(
                            duckBatch.Schema, targetBatchBytes, BatchOptions.Default.MaxRowsPerBatch, EgressAllocator);
                    }

                    rowsRead += duckBatch.Length;
                    var due = coalescer.Add(duckBatch); // owns the chunk from here on
                    ct.ThrowIfCancellationRequested();

                    foreach (var batch in due)
                    {
                        await WriteOwnedAsync(writer, batch, ct).ConfigureAwait(false);
                        OnEgressBatchProducedForTests?.Invoke(rowsRead);
                    }
                }

                var final = coalescer?.Flush();
                if (final is not null)
                {
                    await WriteOwnedAsync(writer, final, ct).ConfigureAwait(false);
                    OnEgressBatchProducedForTests?.Invoke(rowsRead);
                }
            }
            finally
            {
                coalescer?.Dispose();
            }
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            if (gateAcquired)
            {
                _gate.Release();
            }

            writer.Complete(error);
        }
    }

    /// <summary>Hands <paramref name="batch"/> to the consumer, or disposes it when the write does not
    /// happen (cancellation, or a channel already completed), since it then still belongs to the
    /// producer.</summary>
    private static async ValueTask WriteOwnedAsync(
        ChannelWriter<RecordBatch> writer, RecordBatch batch, CancellationToken ct)
    {
        try
        {
            await writer.WriteAsync(batch, ct).ConfigureAwait(false);
        }
        catch
        {
            batch.Dispose();
            throw;
        }
    }
}

public sealed record DuckOptions(string? MemoryLimit = null, int? Threads = null, string? TempDirectory = null);
