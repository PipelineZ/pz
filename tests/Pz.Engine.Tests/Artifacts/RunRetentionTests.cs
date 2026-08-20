using Pz.Engine.Artifacts;

namespace Pz.Engine.Tests.Artifacts;

/// <summary>The `pz clean` retention policy as a pure function. No filesystem
/// anywhere in this class — candidates are constructed directly, so every rule (default keep-last-1,
/// explicit --keep-last, --older-than boundaries, live runs, staging-absent runs) is a table fact.</summary>
public sealed class RunRetentionTests
{
    private static readonly DateTimeOffset Now = new(2026, 7, 29, 12, 0, 0, TimeSpan.Zero);

    /// <summary>A run id in the shape RunCommand produces: yyyyMMddTHHmmssfff + 'Z-' + 4 hex chars.</summary>
    private static string Id(DateTimeOffset at, string suffix = "a1b2") =>
        at.UtcDateTime.ToString("yyyyMMddTHHmmssfff") + "Z-" + suffix;

    private static RunCandidate Candidate(
        DateTimeOffset at, bool hasStaging = true, long stagingBytes = 1000, bool isLive = false) =>
        new(Id(at), hasStaging, stagingBytes, stagingBytes + 100, isLive);

    private static RetentionOptions Defaults => new(KeepLast: null, OlderThan: null, Purge: false);

    [Fact]
    public void Run_timestamp_parses_from_the_run_id()
    {
        Assert.True(RunRetention.TryParseRunTimestamp("20260729T103312123Z-a1b2", out var started));

        Assert.Equal(new DateTimeOffset(2026, 7, 29, 10, 33, 12, 123, TimeSpan.Zero), started);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-run")]
    [InlineData("20260729T103312123-a1b2")]  // missing the 'Z'
    [InlineData("20260729T1033121Z-a1b2")]   // too short
    [InlineData("20261329T103312123Z-a1b2")] // month 13
    public void Unparseable_run_ids_are_rejected(string runId)
    {
        Assert.False(RunRetention.TryParseRunTimestamp(runId, out _));
    }

    [Fact]
    public void Default_keeps_the_newest_and_sweeps_staging_from_the_rest()
    {
        var newest = Candidate(Now.AddHours(-1));
        var older = Candidate(Now.AddHours(-2));
        var oldest = Candidate(Now.AddHours(-3));

        var decisions = RunRetention.Decide([oldest, newest, older], Defaults, Now);

        // Output is newest-first regardless of input order.
        Assert.Equal([newest.RunId, older.RunId, oldest.RunId], decisions.Select(d => d.Candidate.RunId));
        Assert.Equal(SweepAction.Keep, decisions[0].Action);
        Assert.Equal(SweepAction.DeleteStaging, decisions[1].Action);
        Assert.Equal(SweepAction.DeleteStaging, decisions[2].Action);
    }

    [Fact]
    public void Purge_deletes_whole_directories_for_selected_runs()
    {
        var newest = Candidate(Now.AddHours(-1));
        var older = Candidate(Now.AddHours(-2));

        var decisions = RunRetention.Decide([newest, older], Defaults with { Purge = true }, Now);

        Assert.Equal(SweepAction.Keep, decisions[0].Action);
        Assert.Equal(SweepAction.DeleteDir, decisions[1].Action);
    }

    [Fact]
    public void Keep_last_zero_selects_every_run_including_the_newest()
    {
        var newest = Candidate(Now.AddHours(-1));
        var older = Candidate(Now.AddHours(-2));

        var decisions = RunRetention.Decide([newest, older], Defaults with { KeepLast = 0 }, Now);

        Assert.All(decisions, d => Assert.Equal(SweepAction.DeleteStaging, d.Action));
    }

    [Fact]
    public void Keep_last_n_protects_the_newest_n()
    {
        var runs = Enumerable.Range(1, 5).Select(i => Candidate(Now.AddHours(-i))).ToList();

        var decisions = RunRetention.Decide(runs, Defaults with { KeepLast = 3 }, Now);

        Assert.Equal(3, decisions.Count(d => d.Action == SweepAction.Keep));
        Assert.Equal(2, decisions.Count(d => d.Action == SweepAction.DeleteStaging));
    }

    [Fact]
    public void Older_than_still_protects_the_newest_run()
    {
        // The newest run is never swept unless --keep-last 0 is given explicitly — even when
        // it is itself far older than the threshold, because keeping `pz retry` workable is the invariant.
        var newest = Candidate(Now.AddDays(-400));
        var older = Candidate(Now.AddDays(-401));

        var decisions = RunRetention.Decide([newest, older], Defaults with { OlderThan = TimeSpan.FromDays(30) }, Now);

        Assert.Equal(SweepAction.Keep, decisions[0].Action);
        Assert.Equal(SweepAction.DeleteStaging, decisions[1].Action);
    }

    [Fact]
    public void Older_than_leaves_runs_inside_the_window_alone()
    {
        var newest = Candidate(Now.AddHours(-1));
        var inside = Candidate(Now.AddDays(-3));
        var outside = Candidate(Now.AddDays(-10));

        var decisions = RunRetention.Decide(
            [newest, inside, outside], Defaults with { OlderThan = TimeSpan.FromDays(7) }, Now);

        Assert.Equal(SweepAction.Keep, decisions[0].Action);        // newest, always protected
        Assert.Equal(SweepAction.Keep, decisions[1].Action);        // 3d old, inside a 7d window
        Assert.Equal(SweepAction.DeleteStaging, decisions[2].Action); // 10d old
    }

    [Fact]
    public void Older_than_boundary_is_exclusive()
    {
        var newest = Candidate(Now.AddHours(-1));
        var exactlySevenDays = Candidate(Now.AddDays(-7));

        var decisions = RunRetention.Decide(
            [newest, exactlySevenDays], Defaults with { OlderThan = TimeSpan.FromDays(7) }, Now);

        // "older than 7d" means strictly older; exactly 7d survives.
        Assert.Equal(SweepAction.Keep, decisions[1].Action);
    }

    [Fact]
    public void Live_runs_are_never_swept()
    {
        var newest = Candidate(Now.AddHours(-1));
        var live = Candidate(Now.AddHours(-2), isLive: true);

        var decisions = RunRetention.Decide([newest, live], Defaults with { KeepLast = 0, Purge = true }, Now);

        Assert.Equal(SweepAction.Keep, decisions[1].Action);
        Assert.Contains("live run", decisions[1].Reason);
    }

    [Fact]
    public void Selected_run_without_staging_is_kept_when_not_purging()
    {
        var newest = Candidate(Now.AddHours(-1));
        var noStaging = Candidate(Now.AddHours(-2), hasStaging: false, stagingBytes: 0);

        var decisions = RunRetention.Decide([newest, noStaging], Defaults, Now);

        Assert.Equal(SweepAction.Keep, decisions[1].Action);
    }

    [Fact]
    public void Selected_run_without_staging_is_still_purged_when_purging()
    {
        var newest = Candidate(Now.AddHours(-1));
        var noStaging = Candidate(Now.AddHours(-2), hasStaging: false, stagingBytes: 0);

        var decisions = RunRetention.Decide([newest, noStaging], Defaults with { Purge = true }, Now);

        Assert.Equal(SweepAction.DeleteDir, decisions[1].Action);
    }

    [Fact]
    public void Unparseable_run_id_is_never_selected_by_older_than()
    {
        // A hand-made directory has no derivable age; refusing to date it is the conservative reading.
        var newest = Candidate(Now.AddHours(-1));
        var handMade = new RunCandidate("scratch-dir", HasStaging: true, StagingBytes: 10, TotalBytes: 20, IsLive: false);

        var decisions = RunRetention.Decide(
            [newest, handMade], Defaults with { OlderThan = TimeSpan.FromMinutes(1) }, Now);

        var decision = decisions.Single(d => d.Candidate.RunId == "scratch-dir");
        Assert.Equal(SweepAction.Keep, decision.Action);
        Assert.Contains("undatable", decision.Reason);
    }

    [Fact]
    public void Unparseable_run_id_is_still_swept_by_keep_last()
    {
        // keep-last needs only ordering, which ordinal comparison always provides.
        var newest = Candidate(Now.AddHours(-1));
        var handMade = new RunCandidate("scratch-dir", HasStaging: true, StagingBytes: 10, TotalBytes: 20, IsLive: false);

        var decisions = RunRetention.Decide([newest, handMade], Defaults with { KeepLast = 0 }, Now);

        Assert.All(decisions, d => Assert.Equal(SweepAction.DeleteStaging, d.Action));
    }

    [Fact]
    public void A_stray_directory_can_never_become_the_protected_newest_run()
    {
        // Ordinal comparison alone would sort "scratch" ABOVE every real run id ('s' > '2'), making a
        // hand-made directory the protected "newest" while the actual newest run's staging got swept.
        // Datable ids are ordered ahead of undatable ones precisely to stop that.
        var realRun = Candidate(Now.AddHours(-1));
        var stray = new RunCandidate("scratch", HasStaging: true, StagingBytes: 10, TotalBytes: 20, IsLive: false);

        var decisions = RunRetention.Decide([stray, realRun], Defaults, Now);

        Assert.Equal(realRun.RunId, decisions[0].Candidate.RunId);
        Assert.Equal("newest", decisions[0].Reason);
        Assert.Equal(SweepAction.DeleteStaging, decisions[1].Action);
    }

    [Fact]
    public void Empty_input_produces_no_decisions()
    {
        Assert.Empty(RunRetention.Decide([], Defaults, Now));
    }
}
