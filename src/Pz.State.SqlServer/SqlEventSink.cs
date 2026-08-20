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

    private readonly SqlStateConnection _connection;
    private readonly string _runId;
    private readonly TimeProvider _time;
    private readonly Channel<QueuedEvent> _channel = Channel.CreateUnbounded<QueuedEvent>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

    private readonly TaskCompletionSource? _startGate;
    private readonly Task _drainTask;

    private long _seq = -1;
    private int _pending;
    private long _dropped;

    public SqlEventSink(SqlStateConnection connection, string runId, TimeProvider time)
        : this(connection, runId, time, startGate: null)
    {
    }

    private SqlEventSink(SqlStateConnection connection, string runId, TimeProvider time, TaskCompletionSource? startGate)
    {
        _connection = connection;
        _runId = runId;
        _time = time;
        _startGate = startGate;
        _drainTask = Task.Run(DrainLoopAsync);
    }

    /// <summary>Test-only seam (mirrors <see cref="RunEventBus.PendingCountForTests"/>): the background
    /// drain task never even starts reading until <see cref="ReleaseWriterAndDisposeForTests"/> releases
    /// it, so the overflow test can prove <see cref="Write"/> drops rather than blocks by construction
    /// instead of racing a timing window.</summary>
    internal static SqlEventSink WithWriterGatedForTests(SqlStateConnection connection, string runId, TimeProvider time) =>
        new(connection, runId, time, startGate: new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

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
    /// phase.</summary>
    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();

        try
        {
            await _drainTask.ConfigureAwait(false);
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
        }
        catch (Exception)
        {
            // Persisting the event stream is best-effort (class doc): ANY failure here -- a transient
            // outage, a SqlBulkCopy edge case, anything -- must not crash the drain loop. Deliberately a
            // catch-all rather than a closed allowlist: an allowlist only delivers the best-effort
            // promise this class exists to make if it happens to stay complete, and betting on that is
            // exactly the kind of thing that quietly stops holding. The whole batch counts as
            // dropped -- there is no partial-success signal from SqlBulkCopy worth chasing here.
            Interlocked.Add(ref _dropped, batch.Count);
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
