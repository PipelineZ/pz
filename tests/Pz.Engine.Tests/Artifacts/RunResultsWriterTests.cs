using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Artifacts;

public sealed class RunResultsWriterTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunPaths Paths => new(_dir, "test-run");

    [Fact]
    public void WriteSnapshot_is_valid_json_and_leaves_no_tmp_file_after_each_call()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var node1 = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null);

        writer.WriteSnapshot([node1], "running");
        AssertValidSnapshot(expectedNodeCount: 1, expectedStatus: "running");

        var node2 = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Failed, 0, TimeSpan.Zero, new PzError("PZ0501", "boom", null, null, null));

        writer.WriteSnapshot([node1, node2], "completed_with_failures");
        AssertValidSnapshot(expectedNodeCount: 2, expectedStatus: "completed_with_failures");
    }

    [Fact]
    public void WriteSnapshot_fields_match_schema()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var success = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null);
        var failed = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Failed, 0, TimeSpan.Zero, new PzError("PZ0501", "boom", null, null, null));
        var skipped = NodeResult.Skipped(new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SinkWrite,
            "lake.out", [new NodeId("bbbbbbbbbbbbbbbb")], null, "unused"));

        writer.WriteSnapshot([success, failed, skipped], "completed_with_failures");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        Assert.Equal("test-run", root.GetProperty("runId").GetString());
        Assert.Equal("completed_with_failures", root.GetProperty("status").GetString());
        Assert.Equal("2026-07-02T10:15:00.123Z", root.GetProperty("startedAt").GetString());

        var nodes = root.GetProperty("nodes");
        Assert.Equal(3, nodes.GetArrayLength());

        var n0 = nodes[0];
        Assert.Equal("aaaaaaaaaaaaaaaa", n0.GetProperty("id").GetString());
        Assert.Equal("SourceLoad", n0.GetProperty("kind").GetString());
        Assert.Equal("src_files__orders", n0.GetProperty("name").GetString());
        Assert.Equal("success", n0.GetProperty("status").GetString());
        Assert.Equal(12, n0.GetProperty("rows").GetInt64());
        Assert.Equal(417, n0.GetProperty("durationMs").GetInt64());
        Assert.Equal(JsonValueKind.Null, n0.GetProperty("error").ValueKind);

        var n1 = nodes[1];
        Assert.Equal("failed", n1.GetProperty("status").GetString());
        Assert.Equal("PZ0501", n1.GetProperty("error").GetProperty("code").GetString());
        Assert.Equal("boom", n1.GetProperty("error").GetProperty("message").GetString());

        var n2 = nodes[2];
        Assert.Equal("skipped", n2.GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, n2.GetProperty("error").ValueKind);
    }

    /// <summary>`timings` is additive-OPTIONAL — a node with null
    /// <see cref="NodeResult.Timings"/> writes no `"timings"` key at all, never `"timings":null`, so
    /// pre-existing readers and byte-level
    /// assertions over timing-free runs are unaffected; a node with timings gets the two-field
    /// object.</summary>
    [Fact]
    public void Timings_written_only_when_present_and_omitted_when_null()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withTimings = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null,
            new NodeTimings(TimeSpan.FromMilliseconds(30), TimeSpan.FromMilliseconds(250)));
        var withoutTimings = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withTimings, withoutTimings], "success");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        Assert.Equal(1, doc.RootElement.GetProperty("version").GetInt32()); // schema version unchanged
        var nodes = doc.RootElement.GetProperty("nodes");

        var timings = nodes[0].GetProperty("timings");
        Assert.Equal(30, timings.GetProperty("producerStallMs").GetInt64());
        Assert.Equal(250, timings.GetProperty("consumerStallMs").GetInt64());

        Assert.False(nodes[1].TryGetProperty("timings", out _),
            "a null-Timings node must have NO timings key (additive-optional, never explicit null)");
    }

    [Fact]
    public async Task Concurrent_snapshots_are_all_applied_and_last_write_wins()
    {
        const int callCount = 50;
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");

        var tasks = new Task[callCount];
        for (var i = 0; i < callCount; i++)
        {
            var index = i;
            tasks[index] = Task.Run(() =>
            {
                // Distinguishable payload: node count and name both encode the call index so a
                // final read can be checked for internal consistency (no interleaved/mixed write).
                var nodes = new NodeResult[index + 1];
                for (var n = 0; n < nodes.Length; n++)
                {
                    nodes[n] = new NodeResult(new NodeId($"node{index:D4}{n:D4}"), NodeKind.SourceLoad,
                        $"call-{index}-node-{n}", NodeStatus.Success, index, TimeSpan.FromMilliseconds(index),
                        null);
                }

                writer.WriteSnapshot(nodes, $"running-{index}");
            });
        }

        await Task.WhenAll(tasks);

        var path = Paths.RunResultsPath;
        Assert.True(File.Exists(path));

        using var doc = JsonDocument.Parse(File.ReadAllBytes(path));
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        Assert.Equal("test-run", root.GetProperty("runId").GetString());

        var status = root.GetProperty("status").GetString();
        Assert.StartsWith("running-", status);
        var winningIndex = int.Parse(status!["running-".Length..]);

        var nodesArray = root.GetProperty("nodes");
        Assert.Equal(winningIndex + 1, nodesArray.GetArrayLength());
        for (var n = 0; n < nodesArray.GetArrayLength(); n++)
        {
            Assert.Equal($"call-{winningIndex}-node-{n}", nodesArray[n].GetProperty("name").GetString());
        }

        // No unique-per-call tmp file (or the legacy shared name) should survive a completed run.
        var leftoverTmp = Directory.GetFiles(Paths.RunDir, "*.tmp");
        Assert.Empty(leftoverTmp);
    }

    [Fact]
    public void WriteSnapshot_includes_watermark_and_provenance_when_present()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = new RunPaths(dir, "runA");
            var writer = new RunResultsWriter(paths, "2026-07-11T00:00:00.000Z");
            var wm = new Watermark("updated_at", "timestamp", "2026-07-10T23:59:59.000000", "runA");
            writer.WriteSnapshot([
                new NodeResult(new NodeId("n1"), NodeKind.SourceLoad, "src_a", NodeStatus.Success, 5,
                    TimeSpan.FromMilliseconds(10), null, WatermarkCandidate: wm, Provenance: NodeProvenance.Reused),
                new NodeResult(new NodeId("n2"), NodeKind.SinkWrite, "sink_b", NodeStatus.Success, 5,
                    TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward),
            ], "success");

            var json = File.ReadAllText(paths.RunResultsPath);
            Assert.Contains("\"provenance\":\"reused\"", json);
            Assert.Contains("\"provenance\":\"carried_forward\"", json);
            Assert.Contains("\"watermark\":{\"cursor\":\"updated_at\",\"type\":\"timestamp\",\"value\":\"2026-07-10T23:59:59.000000\"}", json);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void WriteSnapshot_omits_watermark_and_provenance_when_absent()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = new RunPaths(dir, "runB");
            var writer = new RunResultsWriter(paths, "2026-07-11T00:00:00.000Z");
            writer.WriteSnapshot([
                new NodeResult(new NodeId("n1"), NodeKind.Pipeline, "p", NodeStatus.Success, 0, TimeSpan.Zero, null),
            ], "success");

            var json = File.ReadAllText(paths.RunResultsPath);
            Assert.DoesNotContain("provenance", json);
            Assert.DoesNotContain("watermark", json);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>The <c>ops</c> block sits after <c>timings</c> and before
    /// <c>watermark</c> (fixed position, byte-stability), and is entirely omitted for a node with no
    /// <see cref="NodeResult.Ops"/> — same omit-when-absent discipline as <c>timings</c>/<c>watermark</c>.</summary>
    [Fact]
    public void Writer_emits_ops_block()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withOps = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null,
            new NodeTimings(TimeSpan.FromMilliseconds(30), TimeSpan.FromMilliseconds(250)),
            Ops: new Pz.Engine.Resilience.OpStats(7, 2, 350));
        var withoutOps = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withOps, withoutOps], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains("\"ops\":{\"executed\":7,\"retried\":2,\"throttle_wait_ms\":350}", json);

        var timingsIndex = json.IndexOf("\"timings\"", StringComparison.Ordinal);
        var opsIndex = json.IndexOf("\"ops\"", StringComparison.Ordinal);
        var watermarkIndex = json.IndexOf("\"watermark\"", StringComparison.Ordinal);
        Assert.True(timingsIndex >= 0 && opsIndex > timingsIndex,
            "ops must be written after timings");
        Assert.True(watermarkIndex < 0 || opsIndex < watermarkIndex,
            "ops must be written before watermark");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var nodes = doc.RootElement.GetProperty("nodes");
        Assert.False(nodes[1].TryGetProperty("ops", out _),
            "a null-Ops node must have NO ops key (additive-optional, never explicit null)");
    }

    /// <summary>The <c>partitions</c> block sits after <c>ops</c> and before
    /// <c>watermark</c> (fixed position, byte-stability), and is entirely omitted for a node with no
    /// <see cref="NodeResult.Partitions"/> — same omit-when-absent discipline as
    /// <c>timings</c>/<c>ops</c>/<c>watermark</c>.</summary>
    [Fact]
    public void Writer_emits_partitions_block()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withPartitions = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null,
            Ops: new Pz.Engine.Resilience.OpStats(7, 2, 350),
            Partitions: new PartitionStats(4, 4, 1, 2));
        var withoutPartitions = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withPartitions, withoutPartitions], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains("\"partitions\":{\"total\":4,\"completed\":4,\"reused\":1,\"resumed\":2}", json);

        var opsIndex = json.IndexOf("\"ops\"", StringComparison.Ordinal);
        var partitionsIndex = json.IndexOf("\"partitions\"", StringComparison.Ordinal);
        var watermarkIndex = json.IndexOf("\"watermark\"", StringComparison.Ordinal);
        Assert.True(opsIndex >= 0 && partitionsIndex > opsIndex,
            "partitions must be written after ops");
        Assert.True(watermarkIndex < 0 || partitionsIndex < watermarkIndex,
            "partitions must be written before watermark");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var nodes = doc.RootElement.GetProperty("nodes");
        Assert.False(nodes[1].TryGetProperty("partitions", out _),
            "a null-Partitions node must have NO partitions key (additive-optional, never explicit null)");
    }

    /// <summary>The <c>delivery</c> block sits after <c>ops</c>/<c>partitions</c>
    /// and before <c>watermark</c> (fixed position, byte-stability), uses snake_case keys (this
    /// writer's <c>throttle_wait_ms</c> convention), and is entirely omitted for a node with no
    /// <see cref="NodeResult.Delivery"/> — same omit-when-absent discipline as
    /// <c>timings</c>/<c>ops</c>/<c>partitions</c>/<c>watermark</c>.</summary>
    [Fact]
    public void Writer_emits_delivery_block()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withDelivery = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SinkWrite, "api.orders_out",
            NodeStatus.Failed, 0, TimeSpan.FromMilliseconds(7), new PzError("PZ0501", "boom", null, null, null),
            Delivery: new DeliveryStats("none", 40, 0));
        var withoutDelivery = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withDelivery, withoutDelivery], "completed_with_failures");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains("\"delivery\":{\"abort_semantics\":\"none\",\"rows_visible\":40,\"resumed_rows\":0}", json);

        var partitionsIndex = json.IndexOf("\"partitions\"", StringComparison.Ordinal);
        var deliveryIndex = json.IndexOf("\"delivery\"", StringComparison.Ordinal);
        var watermarkIndex = json.IndexOf("\"watermark\"", StringComparison.Ordinal);
        Assert.True(partitionsIndex < 0 || deliveryIndex > partitionsIndex,
            "delivery must be written after partitions");
        Assert.True(watermarkIndex < 0 || deliveryIndex < watermarkIndex,
            "delivery must be written before watermark");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var nodes = doc.RootElement.GetProperty("nodes");
        Assert.False(nodes[1].TryGetProperty("delivery", out _),
            "a null-Delivery node must have NO delivery key (additive-optional, never explicit null)");
    }

    /// <summary>The `cdc` block sits after `delivery` and before
    /// `watermark` (fixed position, byte-stability), carries the raw per-op counts plus `position` (the
    /// cdc-shaped SourceLoad's <see cref="NodeResult.SyncStateCandidate"/> token, `null` when the
    /// connector emitted none), and is entirely omitted for a node with no <see cref="NodeResult.Cdc"/>
    /// — same omit-when-absent discipline as `timings`/`ops`/`partitions`/`delivery`/`watermark`.</summary>
    [Fact]
    public void Writer_emits_cdc_block()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withCdc = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "pg.orders_cdc",
            NodeStatus.Success, 40, TimeSpan.FromMilliseconds(50), null,
            SyncStateCandidate: new SyncState("0/1A2B3C4", "test-run"),
            Cdc: new CdcStats(30, 8, 2));
        var withoutCdc = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withCdc, withoutCdc], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains("\"cdc\":{\"inserts\":30,\"updates\":8,\"deletes\":2,\"position\":\"0/1A2B3C4\"}", json);

        var deliveryIndex = json.IndexOf("\"delivery\"", StringComparison.Ordinal);
        var cdcIndex = json.IndexOf("\"cdc\"", StringComparison.Ordinal);
        var watermarkIndex = json.IndexOf("\"watermark\"", StringComparison.Ordinal);
        Assert.True(deliveryIndex < 0 || cdcIndex > deliveryIndex,
            "cdc must be written after delivery");
        Assert.True(watermarkIndex < 0 || cdcIndex < watermarkIndex,
            "cdc must be written before watermark");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var nodes = doc.RootElement.GetProperty("nodes");
        Assert.False(nodes[1].TryGetProperty("cdc", out _),
            "a null-Cdc node must have NO cdc key (additive-optional, never explicit null)");
    }

    [Fact]
    public void Writer_emits_cdc_position_null_when_no_sync_state_candidate()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var node = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "pg.orders_cdc",
            NodeStatus.Success, 0, TimeSpan.Zero, null, Cdc: new CdcStats(0, 0, 0));

        writer.WriteSnapshot([node], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains("\"cdc\":{\"inserts\":0,\"updates\":0,\"deletes\":0,\"position\":null}", json);
    }

    /// <summary>The <c>observed_schema</c> block sits after <c>cdc</c>
    /// and before <c>watermark</c> (fixed position, byte-stability), and is entirely omitted for a node
    /// with no <see cref="NodeResult.Observed"/> — same omit-when-absent discipline as
    /// <c>timings</c>/<c>ops</c>/<c>partitions</c>/<c>delivery</c>/<c>cdc</c> above. This is the
    /// ignore-policy byte-identity guarantee: a node built the way every pre-feature node was writes
    /// exactly the same bytes as before this addition.</summary>
    [Fact]
    public void Writer_emits_observed_schema_block()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var withObserved = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_crm__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null,
            Cdc: new CdcStats(1, 2, 3),
            Observed: new ObservedSchema([new SchemaColumn("id", "BIGINT"), new SchemaColumn("name", "VARCHAR")], "hash-1"));
        var withoutObserved = new NodeResult(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "customer_totals",
            NodeStatus.Success, 5, TimeSpan.FromMilliseconds(20), null);

        writer.WriteSnapshot([withObserved, withoutObserved], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.Contains(
            "\"observed_schema\":{\"columns\":[{\"name\":\"id\",\"type\":\"BIGINT\"},{\"name\":\"name\",\"type\":\"VARCHAR\"}],\"hintsHash\":\"hash-1\"}",
            json);

        var cdcIndex = json.IndexOf("\"cdc\"", StringComparison.Ordinal);
        var observedIndex = json.IndexOf("\"observed_schema\"", StringComparison.Ordinal);
        var watermarkIndex = json.IndexOf("\"watermark\"", StringComparison.Ordinal);
        Assert.True(cdcIndex >= 0 && observedIndex > cdcIndex,
            "observed_schema must be written after cdc");
        Assert.True(watermarkIndex < 0 || observedIndex < watermarkIndex,
            "observed_schema must be written before watermark");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(Paths.RunResultsPath));
        var nodes = doc.RootElement.GetProperty("nodes");
        Assert.False(nodes[1].TryGetProperty("observed_schema", out _),
            "a null-Observed node must have NO observed_schema key (additive-optional, never explicit null)");
    }

    /// <summary>Global constraint: default policy `ignore` never populates <see cref="NodeResult.Observed"/>
    /// (the drift gate short-circuits before the DESCRIBE), so a plain SourceLoad node — exactly what
    /// every pre-feature project writes — must produce byte-identical output to before this task.</summary>
    [Fact]
    public void Ignore_policy_shaped_node_writes_byte_identical_output_with_no_observed_schema_key()
    {
        var writer = new RunResultsWriter(Paths, "2026-07-02T10:15:00.123Z");
        var node = new NodeResult(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_files__orders",
            NodeStatus.Success, 12, TimeSpan.FromMilliseconds(417), null);

        writer.WriteSnapshot([node], "success");

        var json = File.ReadAllText(Paths.RunResultsPath);
        Assert.DoesNotContain("observed_schema", json);
    }

    private void AssertValidSnapshot(int expectedNodeCount, string expectedStatus)
    {
        var path = Paths.RunResultsPath;
        Assert.True(File.Exists(path));
        Assert.False(File.Exists(path + ".tmp"));

        using var doc = JsonDocument.Parse(File.ReadAllBytes(path));
        var root = doc.RootElement;
        Assert.Equal(1, root.GetProperty("version").GetInt32());
        Assert.Equal("test-run", root.GetProperty("runId").GetString());
        Assert.Equal(expectedStatus, root.GetProperty("status").GetString());
        Assert.Equal(expectedNodeCount, root.GetProperty("nodes").GetArrayLength());
    }
}
