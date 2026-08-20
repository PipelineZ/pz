using System.Runtime.CompilerServices;
using System.Text;
using Pz.Cli.Rendering;
using Pz.Diagnostics.Events;

namespace Pz.Cli.Tests.Rendering;

/// <summary>Golden NDJSON coverage: a scripted
/// RunStarted -&gt; 2x(NodeStarted/Progress/Completed) -&gt; a bare partition-stats NodeCompleted
/// -&gt; a failed delivery-stats NodeCompleted -&gt; a cdc-stats NodeCompleted
/// -&gt; RunCompleted -&gt; RetentionSwept (the
/// stream's true last event) sequence with fixed timestamps, byte-compared against
/// <c>Golden/events.ndjson</c>.
/// `PZ_UPDATE_GOLDEN=1` regenerates the golden file in the source tree (same convention as
/// <c>Pz.Core.Tests.Artifacts.GoldenCompileTests</c>).</summary>
public class JsonRendererTests
{
    private static string SourceTreeDir([CallerFilePath] string thisFile = "") =>
        Path.GetDirectoryName(Path.GetDirectoryName(thisFile))!; // tests/Pz.Cli.Tests

    private static RunEvent[] ScriptedSequence()
    {
        const string runId = "20260704T100000000Z-ab12";
        var t0 = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        DateTimeOffset At(int offsetMs) => t0.AddMilliseconds(offsetMs);

        return
        [
            new RunStartedEvent(At(0), runId, "hello_pz", 2),
            new NodeStartedEvent(At(10), runId, "node-a", "SourceLoad", "src_crm__customers"),
            new NodeProgressEvent(At(20), runId, "node-a", "src_crm__customers", 2, 128, 1),
            new NodeCompletedEvent(At(30), runId, "node-a", "SourceLoad", "src_crm__customers", "success", 3, 31,
                null, null, null),
            new NodeStartedEvent(At(40), runId, "node-b", "SinkWrite", "lake.orders_curated"),
            new NodeProgressEvent(At(50), runId, "node-b", "lake.orders_curated", 3, 256, 1),
            new NodeCompletedEvent(At(60), runId, "node-b", "SinkWrite", "lake.orders_curated", "failed", 0, 5,
                "PZ0501", "boom", new NodeTimingsPayload(2, 3)),
            new NodeCompletedEvent(At(65), runId, "node-c", "SourceLoad", "src_files__partitioned_events",
                "success", 12, 45, null, null, null, Partitions: new PartitionStatsPayload(4, 4, 1, 2)),
            new NodeCompletedEvent(At(70), runId, "node-d", "SinkWrite", "api.orders_out",
                "failed", 0, 7, "PZ0501", "boom", null,
                Delivery: new DeliveryPayload("none", 40, 0)),
            // `rows` is the post-collapse canonical count,
            // not the raw 30+8+2=40 change-window total -- CdcPayload's per-op counts are raw window
            // counts (unchanged semantics), but NodeCompleted's `rows` reports what actually lands in
            // the canonical table. Scenario: 30 inserts mint 30 distinct keys, the 8 updates touch keys
            // already among those 30 (no new keys), and the 2 deletes remove 2 of them -- 30 - 2 = 28
            // rows survive collapse.
            new NodeCompletedEvent(At(75), runId, "node-e", "SourceLoad", "pg.orders_cdc",
                "success", 28, 12, null, null, null,
                Cdc: new CdcPayload(30, 8, 2, "0/1A2B3C4")),
            new RunCompletedEvent(At(75), runId, "completed_with_failures", 2, 1, 0, 75),
            // The stream's last event, published by RunCommand after
            // RunCompleted. Counts only -- no swept run id, no path.
            new RetentionSweptEvent(At(120), runId, RunsSwept: 3, BytesFreed: 1_288_490_188, Failures: 0),
        ];
    }

    private static byte[] Render(RunEvent[] events)
    {
        var buffer = new StringWriter { NewLine = "\n" };
        var renderer = new JsonRenderer(buffer);
        foreach (var evt in events)
        {
            renderer.Render(evt);
        }

        return Encoding.UTF8.GetBytes(buffer.ToString());
    }

    [Fact]
    public void Scripted_sequence_matches_golden()
    {
        var actual = Render(ScriptedSequence());
        var goldenSourcePath = Path.Combine(SourceTreeDir(), "Golden", "events.ndjson");

        if (Environment.GetEnvironmentVariable("PZ_UPDATE_GOLDEN") == "1")
        {
            File.WriteAllBytes(goldenSourcePath, actual);
            return; // golden refreshed; commit it
        }

        var goldenPath = Path.Combine(AppContext.BaseDirectory, "Golden", "events.ndjson");
        var expected = File.ReadAllBytes(goldenPath);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Every_line_is_valid_json_with_stable_envelope_fields()
    {
        var actual = Encoding.UTF8.GetString(Render(ScriptedSequence()));
        var lines = actual.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(12, lines.Length);

        foreach (var line in lines)
        {
            using var doc = System.Text.Json.JsonDocument.Parse(line);
            var root = doc.RootElement;
            Assert.True(root.TryGetProperty("event", out _));
            Assert.True(root.TryGetProperty("at", out _));
            Assert.True(root.TryGetProperty("runId", out _));
        }

        Assert.Equal("run_started", System.Text.Json.JsonDocument.Parse(lines[0]).RootElement.GetProperty("event").GetString());
        Assert.Equal("retention_swept", System.Text.Json.JsonDocument.Parse(lines[^1]).RootElement.GetProperty("event").GetString());
    }

    [Fact]
    public void Node_completed_with_timings_serializes_nested_object()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SinkWrite", "lake.orders", "failed", 0, 5,
            "PZ0501", "boom", new NodeTimingsPayload(2, 3));

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var timings = doc.RootElement.GetProperty("timings");
        Assert.Equal(2, timings.GetProperty("producerStallMs").GetInt64());
        Assert.Equal(3, timings.GetProperty("consumerStallMs").GetInt64());
    }

    /// <summary>Asserts the `breaker_state_changed` NDJSON line's event name and every field
    /// name/value.</summary>
    [Fact]
    public void Breaker_state_changed_serializes_event_name_and_fields()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new BreakerStateChangedEvent(at, runId, "conn:pg_prod", "closed", "open",
            "5 consecutive transient failures", 120_000);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var root = doc.RootElement;
        Assert.Equal("breaker_state_changed", root.GetProperty("event").GetString());
        Assert.Equal("conn:pg_prod", root.GetProperty("instance").GetString());
        Assert.Equal("closed", root.GetProperty("oldState").GetString());
        Assert.Equal("open", root.GetProperty("newState").GetString());
        Assert.Equal("5 consecutive transient failures", root.GetProperty("trigger").GetString());
        Assert.Equal(120_000, root.GetProperty("coolDownMs").GetInt64());
    }

    /// <summary>One NDJSON line for `source_drift_detected` with nested `changes[]`/`observed[]`
    /// arrays -- asserted via parsed JSON.</summary>
    [Fact]
    public void Source_drift_detected_serializes_event_name_and_fields()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new SourceDriftDetectedEvent(at, runId, "node-a", "pg_prod", "orders", "warn",
            [new DriftChangePayload("retyped", "amount", "BIGINT", "VARCHAR")],
            [new SchemaColumnPayload("amount", "VARCHAR")], "abc123");

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var root = doc.RootElement;
        Assert.Equal("source_drift_detected", root.GetProperty("event").GetString());
        Assert.Equal("node-a", root.GetProperty("nodeId").GetString());
        Assert.Equal("pg_prod", root.GetProperty("connection").GetString());
        Assert.Equal("orders", root.GetProperty("entity").GetString());
        Assert.Equal("warn", root.GetProperty("policy").GetString());

        var changes = root.GetProperty("changes");
        Assert.Equal(1, changes.GetArrayLength());
        var change = changes[0];
        Assert.Equal("retyped", change.GetProperty("kind").GetString());
        Assert.Equal("amount", change.GetProperty("column").GetString());
        Assert.Equal("BIGINT", change.GetProperty("from").GetString());
        Assert.Equal("VARCHAR", change.GetProperty("to").GetString());

        var observed = root.GetProperty("observed");
        Assert.Equal(1, observed.GetArrayLength());
        Assert.Equal("amount", observed[0].GetProperty("name").GetString());
        Assert.Equal("VARCHAR", observed[0].GetProperty("type").GetString());

        Assert.Equal("abc123", root.GetProperty("hintsHash").GetString());
    }

    /// <summary>One NDJSON line for `merge_key_duplicates_detected` --
    /// key column names and counts only, never row values.</summary>
    [Fact]
    public void Merge_key_duplicates_detected_serializes_event_name_and_fields()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new MergeKeyDuplicatesDetectedEvent(at, runId, "node-a", "events_tgt", ["id", "region"], 2, 3);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var root = doc.RootElement;
        Assert.Equal("merge_key_duplicates_detected", root.GetProperty("event").GetString());
        Assert.Equal("node-a", root.GetProperty("nodeId").GetString());
        Assert.Equal("events_tgt", root.GetProperty("output").GetString());
        var keys = root.GetProperty("keys");
        Assert.Equal(2, keys.GetArrayLength());
        Assert.Equal("id", keys[0].GetString());
        Assert.Equal("region", keys[1].GetString());
        Assert.Equal(2, root.GetProperty("duplicateGroups").GetInt64());
        Assert.Equal(3, root.GetProperty("extraRows").GetInt64());
    }

    [Fact]
    public void Lossy_integer_inference_event_maps_connection_entity_and_columns()
    {
        var at = new DateTimeOffset(2026, 8, 18, 10, 0, 0, TimeSpan.Zero);
        var evt = new LossyIntegerInferenceDetectedEvent(at, "run-1", "node-a", "crm", "orders", ["id", "sku"]);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var root = doc.RootElement;
        Assert.Equal("lossy_integer_inference_detected", root.GetProperty("event").GetString());
        Assert.Equal("node-a", root.GetProperty("nodeId").GetString());
        Assert.Equal("crm", root.GetProperty("connection").GetString());
        Assert.Equal("orders", root.GetProperty("entity").GetString());
        var columns = root.GetProperty("columns");
        Assert.Equal(2, columns.GetArrayLength());
        Assert.Equal("id", columns[0].GetString());
        Assert.Equal("sku", columns[1].GetString());
    }

    [Fact]
    public void Ambiguous_date_inference_event_maps_columns_and_format()
    {
        var at = new DateTimeOffset(2026, 8, 18, 10, 0, 0, TimeSpan.Zero);
        var evt = new AmbiguousDateInferenceDetectedEvent(at, "run-1", "node-a", "crm", "orders", ["when"], "%d/%m/%Y");

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var root = doc.RootElement;
        Assert.Equal("ambiguous_date_inference_detected", root.GetProperty("event").GetString());
        Assert.Equal("node-a", root.GetProperty("nodeId").GetString());
        Assert.Equal("crm", root.GetProperty("connection").GetString());
        Assert.Equal("orders", root.GetProperty("entity").GetString());
        Assert.Equal("when", root.GetProperty("columns")[0].GetString());
        Assert.Equal("%d/%m/%Y", root.GetProperty("format").GetString());
    }

    [Fact]
    public void Node_completed_with_null_timings_serializes_json_null()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        Assert.Equal(System.Text.Json.JsonValueKind.Null, doc.RootElement.GetProperty("timings").ValueKind);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, doc.RootElement.GetProperty("errorCode").ValueKind);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, doc.RootElement.GetProperty("errorMessage").ValueKind);
    }

    /// <summary>`provenance` is additive-optional (append-only contract,
    /// same discipline as `timings`) — present with the exact wire value when the node was reused or
    /// carried forward, and entirely absent (not `null`) for a normally-executed node so every
    /// pre-existing golden line stays byte-identical.</summary>
    [Fact]
    public void Node_completed_with_provenance_serializes_the_field()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null, "reused");

        var actual = Encoding.UTF8.GetString(Render([evt]));
        Assert.Contains("\"provenance\":\"reused\"", actual);
    }

    [Fact]
    public void Node_completed_without_provenance_omits_the_field_entirely()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        Assert.False(doc.RootElement.TryGetProperty("provenance", out _));
    }

    [Fact]
    public void Node_completed_with_ops_serializes_the_block()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null, null, new OpStatsPayload(7, 2, 350));

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        var ops = doc.RootElement.GetProperty("ops");
        Assert.Equal(7, ops.GetProperty("executed").GetInt64());
        Assert.Equal(2, ops.GetProperty("retried").GetInt64());
        Assert.Equal(350, ops.GetProperty("throttleWaitMs").GetInt64());
    }

    [Fact]
    public void Node_completed_without_ops_omits_the_block()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        using var doc = System.Text.Json.JsonDocument.Parse(actual.TrimEnd('\n'));
        Assert.False(doc.RootElement.TryGetProperty("ops", out _));
    }

    [Fact]
    public void Node_completed_with_partitions_serializes_the_block()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null, Partitions: new PartitionStatsPayload(4, 4, 1, 2));

        var actual = Encoding.UTF8.GetString(Render([evt]));
        Assert.Contains("\"partitions\":{\"total\":4,\"completed\":4,\"reused\":1,\"resumed\":2}", actual);
    }

    [Fact]
    public void Node_completed_without_partitions_omits_the_block()
    {
        const string runId = "run-1";
        var at = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var evt = new NodeCompletedEvent(at, runId, "node-a", "SourceLoad", "src", "success", 3, 5,
            null, null, null);

        var actual = Encoding.UTF8.GetString(Render([evt]));
        Assert.DoesNotContain("\"partitions\"", actual);
    }

    /// <summary>The golden proves the bytes but not the
    /// secrecy rule -- a swept run's id or a filesystem path in the event stream would leak project
    /// layout.</summary>
    [Fact]
    public void Retention_swept_carries_counts_only()
    {
        var golden = File.ReadAllText(Path.Combine(SourceTreeDir(), "Golden", "events.ndjson"));
        var line = golden.Split('\n').Single(l => l.Contains("retention_swept", StringComparison.Ordinal));

        using var doc = System.Text.Json.JsonDocument.Parse(line);
        Assert.Equal(3, doc.RootElement.GetProperty("runsSwept").GetInt32());
        Assert.Equal(1_288_490_188, doc.RootElement.GetProperty("bytesFreed").GetInt64());
        Assert.Equal(0, doc.RootElement.GetProperty("failures").GetInt32());

        Assert.DoesNotContain(".pz", line, StringComparison.Ordinal);
    }

    [Fact]
    public void Output_is_LF_terminated_not_CRLF()
    {
        var actual = Encoding.UTF8.GetString(Render(ScriptedSequence()));
        Assert.DoesNotContain("\r\n", actual);
        Assert.EndsWith("\n", actual);
    }
}
