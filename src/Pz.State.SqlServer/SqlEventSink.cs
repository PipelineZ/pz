using System.Buffers;
using System.Data;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Pz.Diagnostics.Events;

namespace Pz.State.SqlServer;

/// <summary>Persists the run-event stream into
/// <c>{schema}.run_events</c>, batched and bounded so it can never stall the engine.
///
/// **Not an <c>IEventRenderer</c>.** That interface lives in <c>Pz.Cli.Rendering</c>; this project
/// references only <c>Pz.Core</c>/<c>Pz.Engine</c>, so implementing it here would invert the
/// strictly-downward layering rule. This is instead a plain sink -- <see cref="Write"/> plus
/// <see cref="IAsyncDisposable"/> -- that the composition site inside <c>Pz.Cli</c> wraps in a
/// one-line <c>IEventRenderer</c> adapter. It is also the right fit semantically:
/// <c>IEventRenderer</c>'s own contract calls rendering "presentation-only and best-effort", which a
/// durable event sink is precisely not.
///
/// **Never blocks, never throws.** <see cref="Write"/> stamps a per-run monotonic <c>seq</c> (an
/// <see cref="Interlocked.Increment(ref long)"/> starting at 0 -- ordering is a contract: `at` has only
/// millisecond precision, so ties are real, and a batched async writer cannot rely on insert order),
/// serializes the event, and enqueues onto an in-memory buffer; a single background task drains it and
/// bulk-inserts via <see cref="SqlBulkCopy"/>. This mirrors <see cref="RunEventBus.Publish"/>'s own
/// never-block/never-throw contract, which the pump feeding this sink depends on holding all the way
/// through.
///
/// **Bounded and lossy on purpose.** The buffer is capped at <see cref="MaxBuffered"/>; once full,
/// <see cref="Write"/> drops the incoming event and counts it rather than blocking the caller or growing
/// without limit. The background task flushes every <see cref="FlushEvents"/> events or
/// <see cref="FlushMs"/> milliseconds, whichever comes first. <see cref="DisposeAsync"/> flushes whatever
/// remains and writes the accumulated dropped count into <c>{schema}.runs.events_dropped</c> -- a
/// silently truncated stream is exactly the failure this guards against.
///
/// **Bounded even against a dead store.** <see cref="MaxConsecutiveFailures"/> consecutive flush
/// failures trip a one-way circuit for the rest of this sink's life: every later batch is counted
/// dropped without another connect attempt, instead of each one paying its own connect timeout in turn.
/// <see cref="DisposeAsync"/> additionally bounds its own total wait at <see cref="DisposeDeadlineMs"/>
/// (via the constructor's <see cref="TimeProvider"/>) as a backstop for a store that is merely slow, not
/// outright failing. Together these are what keep a run's shutdown -- Ctrl-C included -- from hanging
/// for minutes behind a broken or unreachable event store.
///
/// **Payload shape.** <c>event</c>/<c>payload</c> mirror <c>Pz.Cli.Rendering.JsonRenderer</c>'s
/// snake_case event names and camelCase per-event fields, so a consumer reading both the stdout NDJSON
/// stream and this table never has to learn two shapes. Both call the same
/// <see cref="Pz.Diagnostics.Events.RunEventFields"/>, which lives in the one BCL-only project that
/// owns <see cref="RunEvent"/> and that both <c>Pz.Cli</c> and this project already reach -- so the
/// two shapes cannot drift apart the next time a <see cref="RunEvent"/> type or field is added.</summary>
public sealed class SqlEventSink : IAsyncDisposable
{
    public const int FlushEvents = 500;
    public const int FlushMs = 2000;
    public const int MaxBuffered = 10_000;

    /// <summary>Consecutive <see cref="FlushBatchAsync"/> failures (each one a fresh connect attempt
    /// that pays the driver's own connect timeout) before this sink stops trying for the rest of its
    /// lifetime and counts everything still buffered -- and everything written afterward -- as dropped
    /// without attempting to connect. A store that is genuinely down would otherwise make every
    /// remaining batch pay its own connect timeout in turn, which is exactly what made
    /// <see cref="DisposeAsync"/> able to hang for minutes with a full buffer.</summary>
    public const int MaxConsecutiveFailures = 3;

    /// <summary>The most <see cref="DisposeAsync"/> will wait for the drain task, driven by the
    /// constructor's <see cref="TimeProvider"/> so a test can prove the deadline fires without a real
    /// wall-clock wait. A backstop behind <see cref="MaxConsecutiveFailures"/>: that circuit breaker is
    /// what keeps a genuinely-down store fast, but a store that is merely slow (not failing outright)
    /// could otherwise still make dispose run long. Ctrl-C at the end of a run must not hang for
    /// minutes either way.</summary>
    public const int DisposeDeadlineMs = 30_000;

    private readonly SqlStateConnection _connection;
    private readonly string _runId;
    private readonly TimeProvider _time;
    private readonly int _disposeDeadlineMs;
    private readonly Channel<QueuedEvent> _channel = Channel.CreateUnbounded<QueuedEvent>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    private readonly TaskCompletionSource? _startGate;
    private readonly Task _drainTask;

    private long _seq = -1;
    private int _pending;
    private long _dropped;
    private int _consecutiveFailures;
    private volatile bool _circuitOpen;
    // Set when DisposeAsync gives up on the drain task: dispose has then counted everything still
    // pending as dropped, and the drain task -- still running, nothing can stop it -- must not
    // count the same events again when it gets to them.
    private volatile bool _abandoned;
    private long _connectAttempts;

    /// <summary>Real <see cref="SqlBulkCopy"/> flush attempts this sink instance has made -- i.e.
    /// excluding batches the open circuit dropped without trying. Test-only seam: proves the circuit
    /// breaker stops attempting new connections after <see cref="MaxConsecutiveFailures"/>, independent
    /// of timing (a fast-failing "unreachable host" in one environment could otherwise make a
    /// stopwatch-only assertion pass without the breaker ever tripping).</summary>
    internal long ConnectAttemptsForTests => Interlocked.Read(ref _connectAttempts);

    public SqlEventSink(SqlStateConnection connection, string runId, TimeProvider time)
        : this(connection, runId, time, startGate: null, disposeDeadlineMsOverride: null)
    {
    }

    private SqlEventSink(SqlStateConnection connection, string runId, TimeProvider time,
        TaskCompletionSource? startGate, int? disposeDeadlineMsOverride)
    {
        _connection = connection;
        _runId = runId;
        _time = time;
        _startGate = startGate;
        _disposeDeadlineMs = disposeDeadlineMsOverride ?? DisposeDeadlineMs;
        _drainTask = Task.Run(DrainLoopAsync);
    }

    /// <summary>Test-only seam (mirrors <see cref="RunEventBus.PendingCountForTests"/>): the background
    /// drain task never even starts reading until <see cref="ReleaseWriterAndDisposeForTests"/> releases
    /// it, so the overflow test can prove <see cref="Write"/> drops rather than blocks by construction
    /// instead of racing a timing window. <paramref name="disposeDeadlineMsOverride"/> lets the dispose-
    /// deadline test bound a REAL wait to a few hundred milliseconds instead of
    /// <see cref="DisposeDeadlineMs"/>'s production value -- deterministic because the gate above
    /// guarantees the drain task cannot finish first, not because of a mocked clock.</summary>
    internal static SqlEventSink WithWriterGatedForTests(SqlStateConnection connection, string runId, TimeProvider time,
        int? disposeDeadlineMsOverride = null) =>
        new(connection, runId, time, startGate: new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
            disposeDeadlineMsOverride);

    /// <summary>The number of events dropped so far -- either from overflowing <see cref="MaxBuffered"/>,
    /// hitting an unrecognized event type, a batch that failed to persist, or a <see cref="Write"/> that
    /// raced a completed <see cref="DisposeAsync"/>. Public so a
    /// composition site can read the final count without depending on the dispose-time
    /// <c>events_dropped</c> UPDATE, which no-ops if no <c>pz.runs</c> row exists yet for this run.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    internal async Task ReleaseWriterAndDisposeForTests()
    {
        _startGate?.TrySetResult();
        await DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Never blocks, never throws -- see the class doc. Stamps <c>seq</c>, serializes, and
    /// enqueues; all I/O happens later, on the background drain task.</summary>
    public void Write(RunEvent evt)
    {
        var seq = Interlocked.Increment(ref _seq);
        var pending = Interlocked.Increment(ref _pending);
        if (pending > MaxBuffered)
        {
            Interlocked.Decrement(ref _pending);
            Interlocked.Increment(ref _dropped);
            return;
        }

        string eventName;
        string payload;
        try
        {
            eventName = RunEventFields.EventName(evt);
            payload = SerializePayload(evt);
        }
        catch (ArgumentOutOfRangeException)
        {
            // An event type outside the closed set this mapping knows (spec drift, not expected in
            // practice) must not make Write throw: RunEventBus.Publish's never-throw contract has to
            // hold through this pump exactly as it does through JsonRenderer's.
            Interlocked.Decrement(ref _pending);
            Interlocked.Increment(ref _dropped);
            return;
        }

        if (!_channel.Writer.TryWrite(new QueuedEvent(seq, evt.At, eventName, payload)))
        {
            // A Write that races a completed DisposeAsync (the channel writer is closed once
            // DisposeAsync starts) -- not a documented calling pattern, but every other loss path
            // above counts its drop, so this one must too rather than silently losing the event
            // uncounted.
            Interlocked.Decrement(ref _pending);
            Interlocked.Increment(ref _dropped);
        }
    }

    /// <summary>Flushes whatever remains and writes the dropped count into
    /// <c>{schema}.runs.events_dropped</c>. Best-effort like the rest of this sink: a store that is
    /// unreachable at dispose time leaves the row as-is rather than throwing out of a run's finalize
    /// phase.
    ///
    /// Bounded by <see cref="_disposeDeadlineMs"/> (<see cref="DisposeDeadlineMs"/> in production): past
    /// the deadline the drain task is abandoned, not cancelled -- <see cref="SqlBulkCopy"/> has no
    /// cooperative cancellation seam here, so correctness only requires that THIS method returns.
    /// Whatever the abandoned task later finds still buffered simply never persists; every event still
    /// counted pending at that point has not reached the server, so it is exactly as dropped as one
    /// <see cref="FlushBatchAsync"/> already gave up on.</summary>
    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();

        try
        {
            var deadline = Task.Delay(TimeSpan.FromMilliseconds(_disposeDeadlineMs), _time);
            var winner = await Task.WhenAny(_drainTask, deadline).ConfigureAwait(false);
            if (winner == deadline)
            {
                _circuitOpen = true; // WriteDroppedCountAsync below must not pay its own connect timeout
                if (!_abandoned)
                {
                    _abandoned = true;
                    Interlocked.Add(ref _dropped, Interlocked.Exchange(ref _pending, 0));
                }
            }
            else
            {
                await _drainTask.ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // Best-effort (class doc): FlushBatchAsync already swallows every persistence failure
            // it can see, so this is a last-resort guard -- an unexpected fault anywhere else in the
            // drain loop still must not escape a run's finalize phase.
        }

        await WriteDroppedCountAsync().ConfigureAwait(false);
    }

    private async Task DrainLoopAsync()
    {
        if (_startGate is { } gate)
        {
            await gate.Task.ConfigureAwait(false);
        }

        var reader = _channel.Reader;
        var batch = new List<QueuedEvent>(FlushEvents);
        var flushDeadline = _time.GetUtcNow() + TimeSpan.FromMilliseconds(FlushMs);

        while (true)
        {
            bool readable;
            if (batch.Count == 0)
            {
                readable = await reader.WaitToReadAsync().ConfigureAwait(false);
            }
            else
            {
                var remaining = flushDeadline - _time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                {
                    await FlushBatchAsync(batch).ConfigureAwait(false);
                    batch.Clear();
                    continue;
                }

                var waitToRead = reader.WaitToReadAsync().AsTask();
                var delay = Task.Delay(remaining, _time);
                var winner = await Task.WhenAny(waitToRead, delay).ConfigureAwait(false);
                if (winner == delay)
                {
                    await FlushBatchAsync(batch).ConfigureAwait(false);
                    batch.Clear();
                    continue;
                }

                readable = await waitToRead.ConfigureAwait(false);
            }

            if (!readable)
            {
                break; // writer completed and the channel is drained
            }

            while (batch.Count < FlushEvents && reader.TryRead(out var item))
            {
                if (batch.Count == 0)
                {
                    flushDeadline = _time.GetUtcNow() + TimeSpan.FromMilliseconds(FlushMs);
                }

                batch.Add(item);
            }

            if (batch.Count >= FlushEvents)
            {
                await FlushBatchAsync(batch).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await FlushBatchAsync(batch).ConfigureAwait(false);
        }
    }

    private async Task FlushBatchAsync(List<QueuedEvent> batch)
    {
        if (batch.Count == 0)
        {
            return;
        }

        if (_circuitOpen)
        {
            // MaxConsecutiveFailures batches have already failed in a row -- every remaining batch
            // would pay the same connect timeout again for no benefit (class doc). Count it dropped
            // without even trying to open a connection.
            if (!_abandoned)
            {
                Interlocked.Add(ref _dropped, batch.Count);
                Interlocked.Add(ref _pending, -batch.Count);
            }

            return;
        }

        Interlocked.Increment(ref _connectAttempts);
        try
        {
            using var table = BuildTable(batch);
            using var sqlConnection = _connection.Open();
            using var bulk = new SqlBulkCopy(sqlConnection)
            {
                DestinationTableName = $"{QuoteIdentifier(_connection.Schema)}.{QuoteIdentifier("run_events")}",
            };
            bulk.ColumnMappings.Add("run_id", "run_id");
            bulk.ColumnMappings.Add("seq", "seq");
            bulk.ColumnMappings.Add("at", "at");
            bulk.ColumnMappings.Add("event", "event");
            bulk.ColumnMappings.Add("payload", "payload");
            await bulk.WriteToServerAsync(table).ConfigureAwait(false);
            _consecutiveFailures = 0;
        }
        catch (Exception)
        {
            // Persisting the event stream is best-effort (class doc): ANY failure here -- a transient
            // outage, a SqlBulkCopy edge case, anything -- must not crash the drain loop. Deliberately a
            // catch-all rather than a closed allowlist: an allowlist only delivers the best-effort
            // promise this class exists to make if it happens to stay complete, and betting on that is
            // exactly the kind of thing that quietly stops holding. The whole batch counts as
            // dropped -- there is no partial-success signal from SqlBulkCopy worth chasing here.
            if (!_abandoned)
            {
                Interlocked.Add(ref _dropped, batch.Count);
            }

            if (++_consecutiveFailures >= MaxConsecutiveFailures)
            {
                _circuitOpen = true;
            }
        }
        finally
        {
            Interlocked.Add(ref _pending, -batch.Count);
        }
    }

    private DataTable BuildTable(List<QueuedEvent> batch)
    {
        var table = new DataTable();
        table.Columns.Add("run_id", typeof(string));
        table.Columns.Add("seq", typeof(long));
        table.Columns.Add("at", typeof(DateTime));
        table.Columns.Add("event", typeof(string));
        table.Columns.Add("payload", typeof(string));

        foreach (var item in batch)
        {
            table.Rows.Add(_runId, item.Seq, item.At.UtcDateTime, item.EventName, item.Payload);
        }

        return table;
    }

    private async Task WriteDroppedCountAsync()
    {
        if (_circuitOpen)
        {
            // FlushBatchAsync's circuit breaker (or DisposeAsync's own deadline) already gave up on
            // this store -- one more connect attempt here would pay the same timeout for no benefit,
            // and DisposeAsync is supposed to return promptly (class doc: "Ctrl-C must not hang").
            return;
        }

        var dropped = Interlocked.Read(ref _dropped);

        try
        {
            using var sqlConnection = _connection.Open();
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'UPDATE ' + QUOTENAME(@schema) + " +
                "N'.runs SET events_dropped = @dropped WHERE run_id = @run_id'; " +
                "EXEC sp_executesql @sql, N'@dropped INT, @run_id NVARCHAR(64)', " +
                "@dropped = @dropped, @run_id = @run_id;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", _connection.Schema);
            command.Parameters.AddWithValue("@dropped", (int)Math.Min(dropped, int.MaxValue));
            command.Parameters.AddWithValue("@run_id", _runId);
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Best-effort (class doc): a store unreachable at dispose time -- or any other failure here
            // -- leaves events_dropped as-is rather than throwing out of a run's finalize phase. A
            // catch-all for the same reason as FlushBatchAsync's catch above.
        }
    }

    /// <summary>Client-side bracket-quoting for <see cref="SqlBulkCopy.DestinationTableName"/>, which --
    /// unlike every other identifier this project embeds (schema name via server-side <c>QUOTENAME</c>
    /// inside dynamic SQL) -- has no server-side quoting function to delegate to: <see cref="SqlBulkCopy"/>
    /// takes a plain string. Doubling an embedded ']' mirrors exactly what <c>QUOTENAME</c> does for the
    /// default bracket delimiter, so an operator-supplied schema name containing ']' still cannot break
    /// out of the identifier position.</summary>
    private static string QuoteIdentifier(string name) => "[" + name.Replace("]", "]]") + "]";

    private readonly record struct QueuedEvent(long Seq, DateTimeOffset At, string EventName, string Payload);

    /// <summary>The event-name mapping and per-event field writer live in
    /// <see cref="RunEventFields"/> -- a single source of truth both this sink and
    /// <c>Pz.Cli.Rendering.JsonRenderer</c> call, so persisted rows and stdout NDJSON cannot drift
    /// apart.</summary>
    private static string SerializePayload(RunEvent evt)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var json = new Utf8JsonWriter(buffer, new JsonWriterOptions { Indented = false }))
        {
            json.WriteStartObject();
            RunEventFields.WriteFields(json, evt);
            json.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }
}
