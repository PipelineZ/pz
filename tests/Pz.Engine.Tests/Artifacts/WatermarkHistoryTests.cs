using Pz.Engine.Artifacts;

namespace Pz.Engine.Tests.Artifacts;

/// <summary>The rollback menu, read out of run artifacts. The dotted-key
/// tests are the point — `erp.dbo.orders` cannot be split on '.', so the scan tries every split
/// and matches against the node names the artifacts actually carry.</summary>
public sealed class WatermarkHistoryTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Writes a run_results.json with one SourceLoad node. `nodeName` is the staging relation
    /// name, which is what the real writer records — NOT the watermark key.</summary>
    private void MakeRun(string runId, string runStatus, string nodeName, string? value,
        string cursor = "updated_at", string type = "timestamp")
    {
        var dir = Path.Combine(_work, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        var watermark = value is null
            ? ""
            : $$""", "watermark": { "cursor": "{{cursor}}", "type": "{{type}}", "value": "{{value}}" }""";
        File.WriteAllText(Path.Combine(dir, "run_results.json"), $$"""
            {
              "status": "{{runStatus}}",
              "nodes": [
                { "id": "n1", "kind": "SourceLoad", "name": "{{nodeName}}", "status": "success"{{watermark}} }
              ]
            }
            """);
    }

    [Fact]
    public void Candidate_node_names_cover_every_split_of_a_dotted_key()
    {
        var candidates = WatermarkHistory.CandidateNodeNames("erp.dbo.orders");

        // source "erp" + dataset "dbo.orders" folds the dot; source "erp.dbo" is interpolated raw.
        Assert.Contains("src_erp__dbo_orders", candidates);
        Assert.Contains("src_erp.dbo__orders", candidates);
    }

    [Fact]
    public void History_is_newest_first_with_the_run_status()
    {
        MakeRun("20260727T020009111Z-3f2e", "completed_with_failures", "src_erp__orders", "2026-07-27T01:59:00.000000");
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");

        var result = WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.orders");

        Assert.Null(result.Ambiguity);
        Assert.Equal(2, result.Entries.Count);
        Assert.Equal("20260729T020013422Z-a91c", result.Entries[0].RunId);
        Assert.Equal("success", result.Entries[0].RunStatus);
        Assert.Equal("2026-07-29T02:00:00.000000", result.Entries[0].Value);
        Assert.Equal("completed_with_failures", result.Entries[1].RunStatus);
    }

    [Fact]
    public void A_dotted_dataset_name_resolves_through_the_folded_node_name()
    {
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__dbo_orders", "2026-07-29T02:00:00.000000");

        var result = WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.dbo.orders");

        Assert.Null(result.Ambiguity);
        Assert.Equal("2026-07-29T02:00:00.000000", Assert.Single(result.Entries).Value);
    }

    [Fact]
    public void A_run_with_no_watermark_block_is_absent_from_the_history()
    {
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", value: null);

        Assert.Empty(WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.orders").Entries);
    }

    [Fact]
    public void Two_splits_matching_one_run_is_reported_ambiguous_not_guessed()
    {
        // Both "erp" + "dbo.orders" and "erp.dbo" + "orders" have a recorded node here, so no split is
        // defensible and the scan must refuse rather than pick one.
        var dir = Path.Combine(_work, ".pz", "runs", "20260729T020013422Z-a91c");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), """
            {
              "status": "success",
              "nodes": [
                { "id": "n1", "kind": "SourceLoad", "name": "src_erp__dbo_orders", "status": "success",
                  "watermark": { "cursor": "updated_at", "type": "timestamp", "value": "2026-07-29T02:00:00.000000" } },
                { "id": "n2", "kind": "SourceLoad", "name": "src_erp.dbo__orders", "status": "success",
                  "watermark": { "cursor": "updated_at", "type": "timestamp", "value": "2026-07-28T02:00:00.000000" } }
              ]
            }
            """);

        var result = WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.dbo.orders");

        Assert.NotNull(result.Ambiguity);
        Assert.Contains("src_erp__dbo_orders", result.Ambiguity);
        Assert.Contains("src_erp.dbo__orders", result.Ambiguity);
    }

    [Fact]
    public void An_unparseable_run_results_is_skipped_not_fatal()
    {
        MakeRun("20260728T020011003Z-77bd", "success", "src_erp__orders", "2026-07-28T02:00:00.000000");
        var bad = Path.Combine(_work, ".pz", "runs", "20260729T020013422Z-a91c");
        Directory.CreateDirectory(bad);
        File.WriteAllText(Path.Combine(bad, "run_results.json"), "{ truncated");

        var result = WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.orders");

        Assert.Equal("20260728T020011003Z-77bd", Assert.Single(result.Entries).RunId);
    }

    [Fact]
    public void No_runs_directory_is_empty_history()
    {
        Directory.CreateDirectory(_work);

        Assert.Empty(WatermarkHistory.Read(new LocalRunArtifactStore(_work), "erp.orders").Entries);
    }

    [Fact]
    public void Read_latest_still_returns_the_newest_parseable_run()
    {
        // Guards the ReadLatest refactor: it now delegates to ReadAllNewestFirst and must behave identically.
        MakeRun("20260728T020011003Z-77bd", "success", "src_erp__orders", "2026-07-28T02:00:00.000000");
        MakeRun("20260729T020013422Z-a91c", "success", "src_erp__orders", "2026-07-29T02:00:00.000000");

        Assert.Equal("20260729T020013422Z-a91c", RunResultsReader.ReadLatest(_work)!.RunId);
    }
}
