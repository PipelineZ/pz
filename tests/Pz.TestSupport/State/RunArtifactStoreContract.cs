using Pz.Engine.Artifacts;
using Pz.Engine.Execution;

namespace Pz.TestSupport.State;

/// <summary>One contract over every
/// <see cref="IRunArtifactStore"/>. The ordering and skip-the-unreadable rules pinned here are what
/// `pz retry` and `pz state rollback` depend on (RunResultsReader.cs's class docs).</summary>
public abstract class RunArtifactStoreContract
{
    /// <summary>A fresh, empty store. Each call must be independent of the last.</summary>
    protected abstract IRunArtifactStore NewStore();

    protected abstract NodeResult SucceededSourceLoad(string nodeId, string name);

    /// <summary>Makes one already-written run's stored data present but unreadable — mechanics differ per
    /// backend (garbage bytes over run_results.json locally, an unparseable row in a SQL store).</summary>
    protected abstract void CorruptStoredRun(IRunArtifactStore store, string runId);

    private const string StartedAt = "2026-07-31T00:00:00.000Z";

    [SkippableFact]
    public void ReadLatest_on_an_empty_store_is_null()
    {
        Assert.Null(NewStore().ReadLatest());
    }

    [SkippableFact]
    public void ReadLatest_returns_the_greatest_run_id()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000003Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000002Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        Assert.Equal("20260731T000003Z", store.ReadLatest()!.RunId);
    }

    [SkippableFact]
    public void ReadAllNewestFirst_is_descending_by_run_id()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000002Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        Assert.Equal(["20260731T000002Z", "20260731T000001Z"],
            store.ReadAllNewestFirst().Select(r => r.RunId).ToArray());
    }

    [SkippableFact]
    public void A_later_snapshot_of_the_same_run_replaces_the_earlier_one()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "running");
        store.WriteSnapshot("20260731T000001Z", StartedAt,
            [SucceededSourceLoad("n1", "src_a"), SucceededSourceLoad("n2", "src_b")], "success");

        var run = store.ReadLatest()!;

        Assert.Equal("success", run.Status);
        Assert.Equal(2, run.Nodes.Count);
        Assert.Single(store.ReadAllNewestFirst());
    }

    [SkippableFact]
    public void Delete_removes_one_run_and_is_idempotent()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        store.Delete("20260731T000001Z");
        store.Delete("20260731T000001Z");
        store.Delete("never-existed");

        Assert.Null(store.ReadLatest());
    }

    [SkippableFact]
    public void ListCandidates_reports_every_stored_run()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000002Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        Assert.Equal(2, store.ListCandidates().Count);
    }

    /// <summary>RunResultsReader.cs's class doc: a run whose data is missing/mid-write/unparseable is
    /// skipped in favor of the next older one, never thrown. Pinned here so a future SQL-backed store
    /// cannot regress `pz retry` into failing outright on one bad row.</summary>
    [SkippableFact]
    public void An_unreadable_run_is_skipped_and_older_history_still_reads()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000002Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000003Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        CorruptStoredRun(store, "20260731T000002Z");

        Assert.Equal(["20260731T000003Z", "20260731T000001Z"],
            store.ReadAllNewestFirst().Select(r => r.RunId).ToArray());
    }

    /// <summary>The case `pz retry` actually depends on: a corrupted NEWEST run must not surface as "no
    /// prior run" or throw — it must fall through to the next older, readable one.</summary>
    [SkippableFact]
    public void ReadLatest_skips_a_corrupted_newest_run()
    {
        var store = NewStore();
        store.WriteSnapshot("20260731T000001Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot("20260731T000002Z", StartedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        CorruptStoredRun(store, "20260731T000002Z");

        Assert.Equal("20260731T000001Z", store.ReadLatest()!.RunId);
    }
}
