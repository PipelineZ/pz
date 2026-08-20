using Pz.Core.Dag;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Artifacts;

public sealed class RunResultsReaderTests : IDisposable
{
    private readonly string _projectDir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_projectDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void No_runs_dir_returns_null()
    {
        Assert.Null(RunResultsReader.ReadLatest(_projectDir));
    }

    [Fact]
    public void Empty_runs_dir_returns_null()
    {
        Directory.CreateDirectory(Path.Combine(_projectDir, ".pz", "runs"));
        Assert.Null(RunResultsReader.ReadLatest(_projectDir));
    }

    [Fact]
    public void Reads_the_lexicographically_greatest_run_id()
    {
        WriteRun("20260101T000000000Z-0001", "success", []);
        WriteRun("20260102T000000000Z-0002", "completed_with_failures", []);
        WriteRun("20260101T120000000Z-0003", "fatal", []);

        var prior = RunResultsReader.ReadLatest(_projectDir);

        Assert.NotNull(prior);
        Assert.Equal("20260102T000000000Z-0002", prior!.RunId);
        Assert.Equal("completed_with_failures", prior.Status);
    }

    [Fact]
    public void Parses_node_id_name_and_status()
    {
        WriteRun("20260101T000000000Z-0001", "completed_with_failures",
        [
            ("aaaa", "src_files__orders", "success"),
            ("bbbb", "totals", "failed"),
            ("cccc", "lake.totals", "skipped"),
        ]);

        var prior = RunResultsReader.ReadLatest(_projectDir);

        Assert.NotNull(prior);
        Assert.Equal(3, prior!.Nodes.Count);
        Assert.Contains(prior.Nodes, n => n is { Id: "aaaa", Name: "src_files__orders", Status: "success" });
        Assert.Contains(prior.Nodes, n => n is { Id: "bbbb", Name: "totals", Status: "failed" });
        Assert.Contains(prior.Nodes, n => n is { Id: "cccc", Name: "lake.totals", Status: "skipped" });
    }

    [Fact]
    public void Skips_an_unparseable_newest_run_in_favor_of_the_next_older_one()
    {
        WriteRun("20260101T000000000Z-0001", "success", []);

        var corruptDir = Path.Combine(_projectDir, ".pz", "runs", "20260102T000000000Z-0002");
        Directory.CreateDirectory(corruptDir);
        File.WriteAllText(Path.Combine(corruptDir, "run_results.json"), "{not json");

        var prior = RunResultsReader.ReadLatest(_projectDir);

        Assert.NotNull(prior);
        Assert.Equal("20260101T000000000Z-0001", prior!.RunId);
    }

    [Fact]
    public void Skips_a_run_dir_with_no_run_results_file_at_all()
    {
        WriteRun("20260101T000000000Z-0001", "success", []);
        Directory.CreateDirectory(Path.Combine(_projectDir, ".pz", "runs", "20260102T000000000Z-0002"));

        var prior = RunResultsReader.ReadLatest(_projectDir);

        Assert.NotNull(prior);
        Assert.Equal("20260101T000000000Z-0001", prior!.RunId);
    }

    [Fact]
    public void ReadLatest_parses_kind_rows_and_watermark()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = new RunPaths(dir, "20260711T000000000Z-0001");
            var writer = new RunResultsWriter(paths, "2026-07-11T00:00:00.000Z");
            var wm = new Watermark("id", "bigint", "42", "x");
            writer.WriteSnapshot([
                new NodeResult(new NodeId("s1"), NodeKind.SourceLoad, "src_a", NodeStatus.Success, 7,
                    TimeSpan.Zero, null, WatermarkCandidate: wm),
            ], "completed_with_failures");

            var prior = RunResultsReader.ReadLatest(dir);

            Assert.NotNull(prior);
            var node = Assert.Single(prior!.Nodes);
            Assert.Equal("SourceLoad", node.Kind);
            Assert.Equal(7, node.Rows);
            Assert.NotNull(node.Watermark);
            Assert.Equal("id", node.Watermark!.Cursor);
            Assert.Equal("bigint", node.Watermark.Type);
            Assert.Equal("42", node.Watermark.Value);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary><c>observed_schema</c> round-trips through
    /// <see cref="RunResultsWriter"/> → <see cref="RunResultsReader"/> into <see cref="PriorNode.Observed"/>,
    /// mirroring the watermark round-trip above.</summary>
    [Fact]
    public void ReadLatest_parses_observed_schema()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var paths = new RunPaths(dir, "20260811T000000000Z-0001");
            var writer = new RunResultsWriter(paths, "2026-08-11T00:00:00.000Z");
            var observed = new ObservedSchema(
                [new SchemaColumn("id", "BIGINT"), new SchemaColumn("name", "VARCHAR")], "hash-1");
            writer.WriteSnapshot([
                new NodeResult(new NodeId("s1"), NodeKind.SourceLoad, "src_a", NodeStatus.Success, 7,
                    TimeSpan.Zero, null, Observed: observed),
            ], "success");

            var prior = RunResultsReader.ReadLatest(dir);

            Assert.NotNull(prior);
            var node = Assert.Single(prior!.Nodes);
            Assert.NotNull(node.Observed);
            Assert.Equal(observed.Columns, node.Observed!.Columns);
            Assert.Equal(observed.HintsHash, node.Observed.HintsHash);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void ReadLatest_tolerates_nodes_without_new_fields()
    {
        // A pre-upgrade run dir: hand-written JSON with only the original fields.
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        try
        {
            var runDir = Path.Combine(dir, ".pz", "runs", "20260701T000000000Z-0001");
            Directory.CreateDirectory(runDir);
            File.WriteAllText(Path.Combine(runDir, "run_results.json"),
                """{"version":1,"runId":"20260701T000000000Z-0001","status":"completed_with_failures","startedAt":"x","nodes":[{"id":"a","kind":"SinkWrite","name":"s","status":"failed","rows":0,"durationMs":1,"error":null}]}""");

            var prior = RunResultsReader.ReadLatest(dir);

            Assert.NotNull(prior);
            var node = Assert.Single(prior!.Nodes);
            Assert.Equal("SinkWrite", node.Kind);
            Assert.Equal(0, node.Rows);
            Assert.Null(node.Watermark);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
        }
    }

    private void WriteRun(string runId, string status, (string Id, string Name, string Status)[] nodes)
    {
        var runDir = Path.Combine(_projectDir, ".pz", "runs", runId);
        Directory.CreateDirectory(runDir);

        var nodesJson = string.Join(",", nodes.Select(n =>
            $$"""{"id":"{{n.Id}}","kind":"SourceLoad","name":"{{n.Name}}","status":"{{n.Status}}","rows":0,"durationMs":0,"error":null}"""));

        File.WriteAllText(Path.Combine(runDir, "run_results.json"),
            $$"""{"version":1,"runId":"{{runId}}","status":"{{status}}","startedAt":"2026-01-01T00:00:00.000Z","nodes":[{{nodesJson}}]}""");
    }
}
