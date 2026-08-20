using Pz.Engine.Artifacts;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Artifacts;

/// <summary>The `pz clean` sweep against a real directory tree. The
/// "state/target/packages untouched" assertion runs for EVERY flag combination, not once — it is the
/// regression net for the one guarantee that has no flag to turn it off.</summary>
public sealed class RunSweeperTests : IDisposable
{
    private readonly string _project = Path.Combine(Path.GetTempPath(), "pz-sweeper-tests", Guid.NewGuid().ToString("N"));
    private static readonly DateTimeOffset Now = new(2026, 7, 29, 12, 0, 0, TimeSpan.Zero);

    public RunSweeperTests()
    {
        // The three directories no flag may ever reach.
        Directory.CreateDirectory(Path.Combine(_project, ".pz", "state"));
        Directory.CreateDirectory(Path.Combine(_project, ".pz", "target"));
        Directory.CreateDirectory(Path.Combine(_project, ".pz", "packages"));
        File.WriteAllText(Path.Combine(_project, ".pz", "state", "watermarks.json"), "{\"wm\":1}");
        File.WriteAllText(Path.Combine(_project, ".pz", "target", "plan.json"), "{\"plan\":1}");
        File.WriteAllText(Path.Combine(_project, ".pz", "packages", "marker.txt"), "pkg");
    }

    public void Dispose()
    {
        try { Directory.Delete(_project, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static string Id(DateTimeOffset at, string suffix = "a1b2") =>
        at.UtcDateTime.ToString("yyyyMMddTHHmmssfff") + "Z-" + suffix;

    /// <summary>Fabricates a run dir with a results file and, optionally, a staging DB of a known size.</summary>
    private string MakeRun(DateTimeOffset at, int stagingBytes = 512, string suffix = "a1b2")
    {
        var runId = Id(at, suffix);
        var dir = Path.Combine(_project, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), "{\"runId\":\"" + runId + "\"}");
        if (stagingBytes > 0)
        {
            File.WriteAllBytes(Path.Combine(dir, "staging.duckdb"), new byte[stagingBytes]);
        }

        return runId;
    }

    private string RunDir(string runId) => Path.Combine(_project, ".pz", "runs", runId);

    /// <summary>The invariant with no off switch.</summary>
    private void AssertProtectedDirsIntact()
    {
        Assert.Equal("{\"wm\":1}", File.ReadAllText(Path.Combine(_project, ".pz", "state", "watermarks.json")));
        Assert.Equal("{\"plan\":1}", File.ReadAllText(Path.Combine(_project, ".pz", "target", "plan.json")));
        Assert.Equal("pkg", File.ReadAllText(Path.Combine(_project, ".pz", "packages", "marker.txt")));
    }

    private static RetentionOptions Defaults => new(KeepLast: null, OlderThan: null, Purge: false);

    [Fact]
    public void Default_sweep_removes_older_staging_and_keeps_every_results_file()
    {
        var newest = MakeRun(Now.AddHours(-1), suffix: "0003");
        var older = MakeRun(Now.AddHours(-2), suffix: "0002");
        var oldest = MakeRun(Now.AddHours(-3), suffix: "0001");

        var outcome = RunSweeper.Sweep(_project, Defaults, Now, dryRun: false);

        Assert.True(File.Exists(Path.Combine(RunDir(newest), "staging.duckdb")));
        Assert.False(File.Exists(Path.Combine(RunDir(older), "staging.duckdb")));
        Assert.False(File.Exists(Path.Combine(RunDir(oldest), "staging.duckdb")));

        // Nothing is forgotten: every run_results.json survives.
        Assert.True(File.Exists(Path.Combine(RunDir(newest), "run_results.json")));
        Assert.True(File.Exists(Path.Combine(RunDir(older), "run_results.json")));
        Assert.True(File.Exists(Path.Combine(RunDir(oldest), "run_results.json")));

        Assert.Equal(1024, outcome.BytesFreed);
        Assert.Empty(outcome.Failures);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Staging_wal_sidecar_is_removed_with_the_staging_db()
    {
        MakeRun(Now.AddHours(-1), suffix: "0002");
        var older = MakeRun(Now.AddHours(-2), suffix: "0001");
        File.WriteAllBytes(Path.Combine(RunDir(older), "staging.duckdb.wal"), new byte[64]);

        var outcome = RunSweeper.Sweep(_project, Defaults, Now, dryRun: false);

        Assert.False(File.Exists(Path.Combine(RunDir(older), "staging.duckdb.wal")));
        Assert.Equal(512 + 64, outcome.BytesFreed);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Purge_removes_whole_run_directories()
    {
        var newest = MakeRun(Now.AddHours(-1), suffix: "0002");
        var older = MakeRun(Now.AddHours(-2), suffix: "0001");

        RunSweeper.Sweep(_project, Defaults with { Purge = true }, Now, dryRun: false);

        Assert.True(Directory.Exists(RunDir(newest)));
        Assert.False(Directory.Exists(RunDir(older)));
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Keep_last_zero_with_purge_empties_the_runs_directory()
    {
        MakeRun(Now.AddHours(-1), suffix: "0002");
        MakeRun(Now.AddHours(-2), suffix: "0001");

        RunSweeper.Sweep(_project, Defaults with { KeepLast = 0, Purge = true }, Now, dryRun: false);

        Assert.Empty(Directory.EnumerateDirectories(Path.Combine(_project, ".pz", "runs")));
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Dry_run_mutates_nothing_but_reports_the_same_decisions()
    {
        MakeRun(Now.AddHours(-1), suffix: "0002");
        var older = MakeRun(Now.AddHours(-2), suffix: "0001");

        var dry = RunSweeper.Sweep(_project, Defaults with { Purge = true }, Now, dryRun: true);

        Assert.True(Directory.Exists(RunDir(older)));
        Assert.Equal(SweepAction.DeleteDir, dry.Decisions.Single(d => d.Candidate.RunId == older).Action);

        var wet = RunSweeper.Sweep(_project, Defaults with { Purge = true }, Now, dryRun: false);

        Assert.Equal(
            dry.Decisions.Select(d => (d.Candidate.RunId, d.Action)),
            wet.Decisions.Select(d => (d.Candidate.RunId, d.Action)));
        Assert.Equal(dry.BytesFreed, wet.BytesFreed);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void A_live_run_directory_is_skipped()
    {
        MakeRun(Now.AddHours(-1), suffix: "0002");
        var live = MakeRun(Now.AddHours(-2), suffix: "0001");
        using var held = RunDirLock.Acquire(RunDir(live));

        var outcome = RunSweeper.Sweep(_project, Defaults with { KeepLast = 0, Purge = true }, Now, dryRun: false);

        Assert.True(Directory.Exists(RunDir(live)));
        Assert.Equal("live run", outcome.Decisions.Single(d => d.Candidate.RunId == live).Reason);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void A_released_lock_makes_the_directory_sweepable_again()
    {
        // The crash path: the OS drops the lock when the holder exits, so no age heuristic is needed.
        MakeRun(Now.AddHours(-1), suffix: "0002");
        var crashed = MakeRun(Now.AddHours(-2), suffix: "0001");
        var held = RunDirLock.Acquire(RunDir(crashed));
        held.Dispose();

        RunSweeper.Sweep(_project, Defaults with { KeepLast = 0, Purge = true }, Now, dryRun: false);

        Assert.False(Directory.Exists(RunDir(crashed)));
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Stale_restore_workdirs_are_always_swept()
    {
        MakeRun(Now.AddHours(-1));
        var tmp = Path.Combine(_project, ".pz", "tmp", "restore-deadbeef");
        Directory.CreateDirectory(tmp);
        File.WriteAllBytes(Path.Combine(tmp, "leftover.nupkg"), new byte[256]);

        var outcome = RunSweeper.Sweep(_project, Defaults, Now, dryRun: false);

        Assert.False(Directory.Exists(tmp));
        Assert.Equal(1, outcome.TmpDirsSwept);
        Assert.Equal(256, outcome.TmpBytesFreed);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void A_live_restore_workdir_is_skipped()
    {
        MakeRun(Now.AddHours(-1));
        var tmp = Path.Combine(_project, ".pz", "tmp", "restore-live");
        using var held = RunDirLock.Acquire(tmp);

        var outcome = RunSweeper.Sweep(_project, Defaults, Now, dryRun: false);

        Assert.True(Directory.Exists(tmp));
        Assert.Equal(0, outcome.TmpDirsSwept);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Missing_runs_directory_is_not_an_error()
    {
        var outcome = RunSweeper.Sweep(_project, Defaults, Now, dryRun: false);

        Assert.Empty(outcome.Decisions);
        Assert.Equal(0, outcome.BytesFreed);
        Assert.Empty(outcome.Failures);
        AssertProtectedDirsIntact();
    }

    [Fact]
    public void Scan_reports_staging_presence_and_size()
    {
        var withStaging = MakeRun(Now.AddHours(-1), stagingBytes: 300, suffix: "0002");
        var withoutStaging = MakeRun(Now.AddHours(-2), stagingBytes: 0, suffix: "0001");

        var candidates = RunSweeper.Scan(_project);

        var a = candidates.Single(c => c.RunId == withStaging);
        var b = candidates.Single(c => c.RunId == withoutStaging);
        Assert.True(a.HasStaging);
        Assert.Equal(300, a.StagingBytes);
        Assert.False(b.HasStaging);
        Assert.Equal(0, b.StagingBytes);
    }

    /// <summary>Under a NON-local artifact store, `.pz/runs/&lt;id&gt;/` still holds the run's
    /// staging.duckdb, because staging never leaves the machine -- so the sweep must reclaim it locally
    /// and report what it reclaimed. No docker needed: the store is a fake, the local directories are
    /// real.</summary>
    [Fact]
    public void A_remote_store_sweep_also_reclaims_local_staging()
    {
        var newest = MakeRun(Now.AddHours(-1), stagingBytes: 500, suffix: "0003");
        var older = MakeRun(Now.AddHours(-2), stagingBytes: 400, suffix: "0002");
        var store = new FakeRemoteStore([newest, older]);

        // KeepLast 1, Purge false -- the DEFAULT `pz clean`/automatic-retention shape.
        var outcome = RunSweeper.Sweep(_project, store, Defaults, Now, dryRun: false);

        Assert.True(Directory.Exists(RunDir(newest)));
        Assert.False(Directory.Exists(RunDir(older)));
        Assert.Equal(older, Assert.Single(store.Deleted));
        // The freed bytes are the local directory's real size, not the remote candidate's zero.
        Assert.True(outcome.BytesFreed >= 400, $"expected the local run dir's bytes, got {outcome.BytesFreed}");
        AssertProtectedDirsIntact();
    }

    /// <summary>A local run directory the remote store no longer knows about (its rows were swept by an
    /// earlier pass, or written under a different backend) must still be reclaimable -- otherwise it is
    /// orphaned on disk forever.</summary>
    [Fact]
    public void A_remote_store_sweep_reclaims_run_dirs_the_store_has_forgotten()
    {
        MakeRun(Now.AddHours(-1), stagingBytes: 500, suffix: "0003");
        var orphan = MakeRun(Now.AddHours(-2), stagingBytes: 400, suffix: "0002");
        var store = new FakeRemoteStore([]);

        RunSweeper.Sweep(_project, store, Defaults, Now, dryRun: false);

        Assert.False(Directory.Exists(RunDir(orphan)));
        AssertProtectedDirsIntact();
    }

    /// <summary>The live-run guard reaches remote candidates too, since they carry local disk facts:
    /// a directory a running process owns is never deleted, whatever the store says.</summary>
    [Fact]
    public void A_remote_store_sweep_never_touches_a_live_run_dir()
    {
        MakeRun(Now.AddHours(-1), stagingBytes: 500, suffix: "0003");
        var live = MakeRun(Now.AddHours(-2), stagingBytes: 400, suffix: "0002");
        using var held = RunDirLock.Acquire(RunDir(live));
        var store = new FakeRemoteStore([live]);

        RunSweeper.Sweep(_project, store, Defaults with { KeepLast = 0, Purge = true }, Now, dryRun: false);

        Assert.True(Directory.Exists(RunDir(live)));
        Assert.DoesNotContain(live, store.Deleted);
    }

    [Fact]
    public void A_remote_store_dry_run_deletes_nothing()
    {
        var older = MakeRun(Now.AddHours(-2), stagingBytes: 400, suffix: "0002");
        MakeRun(Now.AddHours(-1), stagingBytes: 500, suffix: "0003");
        var store = new FakeRemoteStore([older]);

        RunSweeper.Sweep(_project, store, Defaults with { KeepLast = 0, Purge = true }, Now, dryRun: true);

        Assert.True(Directory.Exists(RunDir(older)));
        Assert.Empty(store.Deleted);
    }

    /// <summary>Stands in for <c>SqlRunArtifactStore</c>: knows run ids and nothing else (no staging, no
    /// size, no lock), and deletes rows rather than directories. Only the two methods the sweep calls do
    /// anything -- the rest of <see cref="IRunArtifactStore"/> is out of scope here.</summary>
    private sealed class FakeRemoteStore(IReadOnlyList<string> runIds) : IRunArtifactStore
    {
        public List<string> Deleted { get; } = [];

        public IReadOnlyList<RunCandidate> ListCandidates() =>
            [.. runIds.Select(id => new RunCandidate(id, HasStaging: false, StagingBytes: 0, TotalBytes: 0, IsLive: false))];

        public void Delete(string runId) => Deleted.Add(runId);

        public void WriteSnapshot(string runId, string startedAtIso, IReadOnlyList<NodeResult> completed,
            string status, long? eventsDropped = null) => throw new NotSupportedException();

        public PriorRun? ReadLatest() => throw new NotSupportedException();

        public IEnumerable<PriorRun> ReadAllNewestFirst() => throw new NotSupportedException();
    }
}
