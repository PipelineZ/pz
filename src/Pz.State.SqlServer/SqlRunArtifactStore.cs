using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.State.SqlServer;

/// <summary>The SQL Server implementation of
/// <see cref="IRunArtifactStore"/>, backed by <c>{schema}.runs</c>/<c>run_nodes</c>/<c>run_events</c>
/// (see <see cref="SqlStateSchema"/>).
///
/// <see cref="WriteSnapshot"/> narrows the local backend's "rewrite the whole document" to "upsert the
/// run header plus every node in <paramref name="completed"/>" -- the per-node cadence
/// (called after EVERY node completion) is what makes this affordable over a network. First-class
/// columns hold exactly what `pz retry`/`pz state show` filter on (status, kind, rows_moved,
/// duration_ms, error_code/message, provenance, the watermark triple); the additive-optional ABI
/// payloads (<see cref="NodeResult.Timings"/>, <see cref="NodeResult.Ops"/>,
/// <see cref="NodeResult.Partitions"/>, <see cref="NodeResult.Delivery"/>, <see cref="NodeResult.Cdc"/>,
/// <see cref="NodeResult.Observed"/>) go together into one JSON <c>payload</c> column, so a future
/// additive ABI field costs no migration.
///
/// **Collation decision (deliberate, not a default):** unlike <c>state.scope</c>/<c>state_key</c>
/// (arbitrary user-controlled text, where SQL Server's default case-insensitive collation silently
/// folded "A" and "a" together), <c>run_id</c> and <c>node_id</c> stay on the schema's default
/// collation. Both value domains are fixed-shape and case-unambiguous: a run id is
/// <c>yyyyMMddTHHmmssfff'Z-'xxxx</c> (<see cref="Pz.Cli"/>'s <c>RunCommand.ExecuteRun</c>) -- digits and
/// two always-uppercase literals ('T', 'Z') at fixed positions, plus a lowercase-only hex suffix; a node
/// id is <see cref="Pz.Core.Dag.NodeId.Compute"/>'s lowercase hex digest. Neither domain contains a
/// character with a differently-cased counterpart that could also appear in the same domain, so no two
/// distinct legitimate values can compare equal, and ordering matches
/// <see cref="StringComparer.Ordinal"/> exactly (digits and fixed literals sort identically under every
/// SQL Server collation; the differentiating character between any two distinct ids is always a digit or
/// a lowercase hex digit, never a case-foldable letter pair). A collation change here would be
/// unrequested complexity fixing a defect that cannot occur for these two domains.</summary>
public sealed class SqlRunArtifactStore(SqlStateConnection connection, string projectName) : IRunArtifactStore
{
    /// <summary>Serializes concurrent <see cref="WriteSnapshot"/> calls for the SAME run id within this
    /// store instance -- mirrors <c>LocalRunArtifactStore</c>'s per-run <c>RunResultsWriter</c> lock
    /// (that class's doc comment): two <c>NodeCompleted</c> callbacks racing on the same run must never
    /// interleave their upserts of the <c>runs</c> header row.</summary>
    private readonly Dictionary<string, Lock> _runLocks = new(StringComparer.Ordinal);

    private readonly Lock _runLocksGate = new();

    public void WriteSnapshot(string runId, string startedAtIso, IReadOnlyList<NodeResult> completed, string status,
        long? eventsDropped = null)
    {
        lock (LockFor(runId))
        {
            using var sqlConnection = connection.Open();
            try
            {
                using var transaction = sqlConnection.BeginTransaction();
                try
                {
                    UpsertRunHeader(sqlConnection, transaction, runId, startedAtIso, status, eventsDropped);
                    foreach (var node in completed)
                    {
                        UpsertNode(sqlConnection, transaction, runId, node);
                    }

                    transaction.Commit();
                }
                catch
                {
                    transaction.Rollback();
                    throw;
                }
            }
            catch (Exception ex) when (ex is SqlException or InvalidOperationException)
            {
                throw connection.Unavailable(ex);
            }
        }
    }

    public PriorRun? ReadLatest() => ReadAllNewestFirst().FirstOrDefault();

    /// <summary>Lazily enumerated (RunResultsReader.cs's class doc, mirrored here): the id list is one
    /// cheap index-only scan of <c>runs</c>, but each run's header + nodes are only read as enumeration
    /// reaches them -- so <see cref="ReadLatest"/> (<c>FirstOrDefault</c>) costs one run's worth of
    /// reading in the common case, never every stored run.</summary>
    public IEnumerable<PriorRun> ReadAllNewestFirst()
    {
        using var sqlConnection = connection.Open();
        List<string> runIds;
        try
        {
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT run_id FROM ' + QUOTENAME(@schema) + " +
                "N'.runs ORDER BY run_id DESC'; " +
                "EXEC sp_executesql @sql;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", connection.Schema);

            runIds = [];
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                runIds.Add(reader.GetString(0));
            }
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }

        foreach (var runId in runIds)
        {
            PriorRun run;
            try
            {
                run = ReadRun(sqlConnection, runId);
            }
            catch (UnreadableRunException)
            {
                // Skip-the-unreadable (RunResultsReader.cs's class doc): a run whose stored data cannot
                // be parsed falls through to the next older one rather than failing `pz retry` outright.
                continue;
            }

            yield return run;
        }
    }

    public IReadOnlyList<RunCandidate> ListCandidates()
    {
        using var sqlConnection = connection.Open();
        try
        {
            using var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT run_id FROM ' + QUOTENAME(@schema) + N'.runs'; " +
                "EXEC sp_executesql @sql;",
                sqlConnection);
            command.Parameters.AddWithValue("@schema", connection.Schema);

            var candidates = new List<RunCandidate>();
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                // A remote candidate never carries staging (DeleteStaging is local-only because
                // staging never leaves the machine) or a live-run lock (that concept is
                // filesystem-only, RunDirLock) -- both always false/zero here.
                candidates.Add(new RunCandidate(reader.GetString(0), HasStaging: false, StagingBytes: 0, TotalBytes: 0, IsLive: false));
            }

            return candidates;
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    /// <summary>Idempotent: deleting an absent run is a no-op (each DELETE simply affects zero rows),
    /// never an error. Spans all three tables in one transaction -- the schema declares no foreign
    /// keys, so nothing cascades on its own.</summary>
    public void Delete(string runId)
    {
        using var sqlConnection = connection.Open();
        try
        {
            using var transaction = sqlConnection.BeginTransaction();
            try
            {
                DeleteFrom(sqlConnection, transaction, "run_events", runId);
                DeleteFrom(sqlConnection, transaction, "run_nodes", runId);
                DeleteFrom(sqlConnection, transaction, "runs", runId);
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    private Lock LockFor(string runId)
    {
        lock (_runLocksGate)
        {
            if (!_runLocks.TryGetValue(runId, out var runLock))
            {
                runLock = new Lock();
                _runLocks[runId] = runLock;
            }

            return runLock;
        }
    }

    /// <summary><paramref name="eventsDropped"/> folds the run's
    /// persisted-event drop count into this upsert instead of relying on <c>SqlEventSink</c>'s own
    /// dispose-time UPDATE, which silently no-ops when no <c>runs</c> row exists yet for this run id --
    /// an unenforced ordering invariant between two independently-constructed components. Null (every
    /// per-node snapshot, and the terminal snapshot taken before the event sink is disposed) leaves the
    /// column as-is via <c>COALESCE</c>, so only the composition site's one post-dispose call ever
    /// actually changes it; a fresh INSERT with no known count yet defaults to 0.
    /// <c>SqlEventSink.DisposeAsync</c>'s own UPDATE stays in place as a backstop for a run whose
    /// artifacts are local-only (<c>state.artifacts: false</c>) but events are remote, which never calls
    /// this method at all.</summary>
    private void UpsertRunHeader(SqlConnection sqlConnection, SqlTransaction transaction, string runId,
        string startedAtIso, string status, long? eventsDropped)
    {
        var startedAt = ParseIso(startedAtIso);
        var finishedAt = IsTerminal(status) ? DateTime.UtcNow : (DateTime?)null;

        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'" +
            "UPDATE ' + QUOTENAME(@schema) + N'.runs SET status = @status, finished_at = @finished_at, " +
            "events_dropped = COALESCE(@events_dropped, events_dropped) " +
            "WHERE run_id = @run_id; " +
            "IF @@ROWCOUNT = 0 " +
            "INSERT INTO ' + QUOTENAME(@schema) + N'.runs " +
            "(run_id, project, status, started_at, finished_at, events_dropped) " +
            "VALUES (@run_id, @project, @status, @started_at, @finished_at, COALESCE(@events_dropped, 0));'; " +
            "EXEC sp_executesql @sql, " +
            "N'@run_id NVARCHAR(64), @project NVARCHAR(256), @status NVARCHAR(32), " +
            "@started_at DATETIME2, @finished_at DATETIME2, @events_dropped INT', " +
            "@run_id = @run_id, @project = @project, @status = @status, " +
            "@started_at = @started_at, @finished_at = @finished_at, @events_dropped = @events_dropped;",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);
        command.Parameters.AddWithValue("@project", projectName);
        command.Parameters.AddWithValue("@status", status);
        command.Parameters.AddWithValue("@started_at", startedAt);
        command.Parameters.AddWithValue("@finished_at", (object?)finishedAt ?? DBNull.Value);
        command.Parameters.AddWithValue("@events_dropped",
            eventsDropped is { } dropped ? (int)Math.Min(dropped, int.MaxValue) : DBNull.Value);
        command.ExecuteNonQuery();
    }

    private void UpsertNode(SqlConnection sqlConnection, SqlTransaction transaction, string runId, NodeResult node)
    {
        var provenance = node.Provenance is { } p ? ProvenanceName(p) : null;
        var payload = SerializePayload(node);

        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'" +
            "UPDATE ' + QUOTENAME(@schema) + N'.run_nodes SET name = @name, kind = @kind, status = @status, " +
            "rows_moved = @rows_moved, duration_ms = @duration_ms, error_code = @error_code, " +
            "error_message = @error_message, provenance = @provenance, " +
            "watermark_cursor = @watermark_cursor, watermark_type = @watermark_type, " +
            "watermark_value = @watermark_value, payload = @payload " +
            "WHERE run_id = @run_id AND node_id = @node_id; " +
            "IF @@ROWCOUNT = 0 " +
            "INSERT INTO ' + QUOTENAME(@schema) + N'.run_nodes " +
            "(run_id, node_id, name, kind, status, rows_moved, duration_ms, error_code, error_message, " +
            "provenance, watermark_cursor, watermark_type, watermark_value, payload) " +
            "VALUES (@run_id, @node_id, @name, @kind, @status, @rows_moved, @duration_ms, @error_code, " +
            "@error_message, @provenance, @watermark_cursor, @watermark_type, @watermark_value, @payload);'; " +
            "EXEC sp_executesql @sql, " +
            "N'@run_id NVARCHAR(64), @node_id NVARCHAR(128), @name NVARCHAR(512), @kind NVARCHAR(32), " +
            "@status NVARCHAR(32), @rows_moved BIGINT, @duration_ms BIGINT, @error_code NVARCHAR(16), " +
            "@error_message NVARCHAR(MAX), @provenance NVARCHAR(32), @watermark_cursor NVARCHAR(256), " +
            "@watermark_type NVARCHAR(64), @watermark_value NVARCHAR(256), @payload NVARCHAR(MAX)', " +
            "@run_id = @run_id, @node_id = @node_id, @name = @name, @kind = @kind, @status = @status, " +
            "@rows_moved = @rows_moved, @duration_ms = @duration_ms, @error_code = @error_code, " +
            "@error_message = @error_message, @provenance = @provenance, " +
            "@watermark_cursor = @watermark_cursor, @watermark_type = @watermark_type, " +
            "@watermark_value = @watermark_value, @payload = @payload;",
            sqlConnection, transaction);

        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);
        command.Parameters.AddWithValue("@node_id", node.Id.Value);
        command.Parameters.AddWithValue("@name", node.Name);
        command.Parameters.AddWithValue("@kind", node.Kind.ToString());
        command.Parameters.AddWithValue("@status", NodeStatusName(node.Status));
        command.Parameters.AddWithValue("@rows_moved", node.RowsMoved);
        command.Parameters.AddWithValue("@duration_ms", (long)node.Duration.TotalMilliseconds);
        command.Parameters.AddWithValue("@error_code", (object?)node.Error?.Code ?? DBNull.Value);
        command.Parameters.Add("@error_message", SqlDbType.NVarChar, -1).Value = (object?)node.Error?.Message ?? DBNull.Value;
        command.Parameters.AddWithValue("@provenance", (object?)provenance ?? DBNull.Value);
        command.Parameters.AddWithValue("@watermark_cursor", (object?)node.WatermarkCandidate?.Cursor ?? DBNull.Value);
        command.Parameters.AddWithValue("@watermark_type", (object?)node.WatermarkCandidate?.TypeName ?? DBNull.Value);
        command.Parameters.AddWithValue("@watermark_value", (object?)node.WatermarkCandidate?.Value ?? DBNull.Value);
        command.Parameters.Add("@payload", SqlDbType.NVarChar, -1).Value = (object?)payload ?? DBNull.Value;
        command.ExecuteNonQuery();
    }

    /// <summary>Reads one run's header + nodes. Throws <see cref="UnreadableRunException"/> (caught by
    /// <see cref="ReadAllNewestFirst"/>) when the stored data cannot be parsed -- currently only an
    /// unparseable <c>payload</c> column (SQL Server enforces every other column's type, so there is
    /// nothing else here that CAN be malformed short of that).</summary>
    private PriorRun ReadRun(SqlConnection sqlConnection, string runId)
    {
        try
        {
            string status;
            using (var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT status FROM ' + QUOTENAME(@schema) + " +
                "N'.runs WHERE run_id = @run_id'; " +
                "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
                sqlConnection))
            {
                command.Parameters.AddWithValue("@schema", connection.Schema);
                command.Parameters.AddWithValue("@run_id", runId);
                if (command.ExecuteScalar() is not string headerStatus)
                {
                    // The header vanished between the id scan and this read (e.g. a concurrent
                    // Delete) -- treated the same as any other unreadable run rather than throwing.
                    throw new UnreadableRunException();
                }

                status = headerStatus;
            }

            var nodes = new List<PriorNode>();
            using (var command = new SqlCommand(
                "DECLARE @sql NVARCHAR(MAX) = N'SELECT node_id, name, status, kind, rows_moved, " +
                "watermark_cursor, watermark_type, watermark_value, payload FROM ' + QUOTENAME(@schema) + " +
                "N'.run_nodes WHERE run_id = @run_id'; " +
                "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
                sqlConnection))
            {
                command.Parameters.AddWithValue("@schema", connection.Schema);
                command.Parameters.AddWithValue("@run_id", runId);
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var payload = reader.IsDBNull(8) ? null : reader.GetString(8);
                    ValidatePayload(payload);

                    var cursor = reader.IsDBNull(5) ? null : reader.GetString(5);
                    var type = reader.IsDBNull(6) ? null : reader.GetString(6);
                    var value = reader.IsDBNull(7) ? null : reader.GetString(7);

                    nodes.Add(new PriorNode(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetInt64(4),
                        cursor is null || type is null || value is null ? null : new PriorWatermark(cursor, type, value),
                        ParseObservedSchema(payload)));
                }
            }

            return new PriorRun(runId, status, nodes);
        }
        catch (UnreadableRunException)
        {
            throw;
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException)
        {
            throw connection.Unavailable(ex);
        }
    }

    private void DeleteFrom(SqlConnection sqlConnection, SqlTransaction transaction, string table, string runId)
    {
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'DELETE FROM ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + " +
            "N' WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection, transaction);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.AddWithValue("@run_id", runId);
        command.ExecuteNonQuery();
    }

    /// <summary>Marker for "this row's stored data cannot be parsed" (currently: a corrupt
    /// <c>payload</c> column) -- distinct from <see cref="SqlException"/>/<see cref="InvalidOperationException"/>
    /// so a genuine connectivity failure is never mistaken for skip-the-unreadable.</summary>
    private sealed class UnreadableRunException : Exception;

    /// <summary>Parses the <c>observed_schema</c> object out of a node's <c>payload</c> column, mirroring
    /// <see cref="RunResultsReader"/>'s <c>TryParse</c> local-store logic -- <see cref="PriorNode.Observed"/>
    /// must be populated for a SQL-backed run too, or <c>pz schema accept</c> is a no-op under
    /// <c>state: {backend: sqlserver}</c>.
    /// Omit-when-absent: null payload, no <c>observed_schema</c> property, or a shape missing
    /// <c>hintsHash</c>/<c>columns</c> all yield null rather than throwing -- <see cref="ValidatePayload"/>
    /// is what already guards against a payload column that isn't valid JSON at all. Internal (mirroring
    /// <see cref="SqlEventSink"/>'s <c>ForTests</c> seam via this assembly's <c>InternalsVisibleTo</c>) so
    /// the SerializePayload/ParseObservedSchema round-trip can be proven without Docker.</summary>
    internal static ObservedSchema? ParseObservedSchema(string? payload)
    {
        if (payload is null)
        {
            return null;
        }

        using var document = JsonDocument.Parse(payload);
        if (!document.RootElement.TryGetProperty("observed_schema", out var obsElement) ||
            obsElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var hintsHash = obsElement.TryGetProperty("hintsHash", out var hh) ? hh.GetString() : null;
        if (hintsHash is null || !obsElement.TryGetProperty("columns", out var colsElement) ||
            colsElement.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var columns = new List<SchemaColumn>();
        foreach (var col in colsElement.EnumerateArray())
        {
            var name = col.TryGetProperty("name", out var n) ? n.GetString() : null;
            var type = col.TryGetProperty("type", out var t) ? t.GetString() : null;
            if (name is not null && type is not null)
            {
                columns.Add(new SchemaColumn(name, type));
            }
        }

        return new ObservedSchema(columns, hintsHash);
    }

    private static void ValidatePayload(string? payload)
    {
        if (payload is null)
        {
            return;
        }

        try
        {
            using var document = JsonDocument.Parse(payload);
        }
        catch (JsonException)
        {
            throw new UnreadableRunException();
        }
    }

    /// <summary>The additive-optional ABI payloads, serialized together -- same
    /// field shapes/names as <c>RunResultsWriter.WriteNode</c>'s JSON, so a future reader can share
    /// parsing logic. Null when every one of them is null, so a node untouched by any of those
    /// features writes no payload at all. Internal for the same no-Docker test seam as
    /// <see cref="ParseObservedSchema"/>.</summary>
    internal static string? SerializePayload(NodeResult node)
    {
        if (node.Timings is null && node.Ops is null && node.Partitions is null &&
            node.Delivery is null && node.Cdc is null && node.Observed is null)
        {
            return null;
        }

        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();

            if (node.Timings is { } timings)
            {
                writer.WriteStartObject("timings");
                writer.WriteNumber("producerStallMs", (long)timings.ProducerStall.TotalMilliseconds);
                writer.WriteNumber("consumerStallMs", (long)timings.ConsumerStall.TotalMilliseconds);
                writer.WriteEndObject();
            }

            if (node.Ops is { } ops)
            {
                writer.WriteStartObject("ops");
                writer.WriteNumber("executed", ops.Executed);
                writer.WriteNumber("retried", ops.Retried);
                writer.WriteNumber("throttle_wait_ms", ops.ThrottleWaitMs);
                writer.WriteEndObject();
            }

            if (node.Partitions is { } partitions)
            {
                writer.WriteStartObject("partitions");
                writer.WriteNumber("total", partitions.Total);
                writer.WriteNumber("completed", partitions.Completed);
                writer.WriteNumber("reused", partitions.Reused);
                writer.WriteNumber("resumed", partitions.Resumed);
                writer.WriteEndObject();
            }

            if (node.Delivery is { } delivery)
            {
                writer.WriteStartObject("delivery");
                writer.WriteString("abort_semantics", delivery.AbortSemantics);
                writer.WriteNumber("rows_visible", delivery.RowsVisible);
                writer.WriteNumber("resumed_rows", delivery.ResumedRows);
                writer.WriteEndObject();
            }

            if (node.Cdc is { } cdc)
            {
                writer.WriteStartObject("cdc");
                writer.WriteNumber("inserts", cdc.Inserts);
                writer.WriteNumber("updates", cdc.Updates);
                writer.WriteNumber("deletes", cdc.Deletes);
                if (node.SyncStateCandidate is { } position)
                {
                    writer.WriteString("position", position.Token);
                }
                else
                {
                    writer.WriteNull("position");
                }

                writer.WriteEndObject();
            }

            if (node.Observed is { } observed)
            {
                writer.WriteStartObject("observed_schema");
                writer.WriteStartArray("columns");
                foreach (var col in observed.Columns)
                {
                    writer.WriteStartObject();
                    writer.WriteString("name", col.Name);
                    writer.WriteString("type", col.Type);
                    writer.WriteEndObject();
                }

                writer.WriteEndArray();
                writer.WriteString("hintsHash", observed.HintsHash);
                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static DateTime ParseIso(string iso) =>
        DateTimeOffset.Parse(iso, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal).UtcDateTime;

    /// <summary>Mirrors <c>RunResultsWriter.WriteSnapshot</c>'s contract: every status but "running" is
    /// terminal (RunResultsReader.cs's <see cref="PriorRun"/> doc lists "running" as the one
    /// non-terminal value a crashed run's last snapshot can be left holding).</summary>
    private static bool IsTerminal(string status) => status != "running";

    private static string NodeStatusName(NodeStatus status) => status switch
    {
        NodeStatus.Success => "success",
        NodeStatus.Failed => "failed",
        NodeStatus.Skipped => "skipped",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };

    private static string ProvenanceName(NodeProvenance provenance) => provenance switch
    {
        NodeProvenance.Reused => "reused",
        NodeProvenance.CarriedForward => "carried_forward",
        _ => throw new ArgumentOutOfRangeException(nameof(provenance), provenance, "unknown provenance"),
    };
}
