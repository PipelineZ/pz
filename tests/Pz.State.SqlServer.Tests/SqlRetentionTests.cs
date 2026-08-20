using Microsoft.Data.SqlClient;
using Pz.Core.Dag;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.State.SqlServer;
using Pz.TestSupport;

namespace Pz.State.SqlServer.Tests;

/// <summary>Retention against a SQL Server <see cref="IRunArtifactStore"/>.
/// <see cref="RunRetention.Decide"/> is not re-tested here -- <c>RunRetentionTests</c> already pins
/// its rules exhaustively. What this suite covers is the applier:
/// <see cref="RunSweeper.Sweep(string, IRunArtifactStore, RetentionOptions, System.DateTimeOffset, bool)"/>
/// against a remote store instead of a local directory tree.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlRetentionTests(SqlServerFixture fixture)
{
    private static readonly DateTimeOffset FixedNow = new(2026, 7, 31, 0, 0, 0, TimeSpan.Zero);
    // A remote sweep DOES look at local disk (`.pz/runs/<id>/staging.duckdb` has to be reclaimed
    // wherever artifacts live), but a project directory that does not exist yields no
    // candidates and no deletions -- which is exactly what isolates these tests to the SQL side.
    // RunSweeperTests covers the local half against a real directory tree.
    private const string ProjectDir = "/nonexistent/project";

    private static NodeResult SucceededSourceLoad(string nodeId, string name) =>
        new(new NodeId(nodeId), NodeKind.SourceLoad, name, NodeStatus.Success, 0, TimeSpan.Zero, null);

    private SqlRunArtifactStore NewStore(out SqlStateConnection connection)
    {
        DockerFacts.SkipUnlessDocker();
        connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);
        return new SqlRunArtifactStore(connection, "test-project");
    }

    /// <summary>The core scenario: 5 runs, keep_last 2 -- the 3 oldest are selected and deleted, the 2
    /// newest survive, and BytesFreed is reported as 0, since a row count is not a byte count.</summary>
    [SkippableFact]
    public void Sweep_deletes_remote_runs_beyond_keep_last_and_reports_zero_bytes()
    {
        var store = NewStore(out _);
        for (var i = 1; i <= 5; i++)
        {
            store.WriteSnapshot(RunId(i), "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        }

        var options = new RetentionOptions(KeepLast: 2, OlderThan: null, Purge: false);
        var outcome = RunSweeper.Sweep(ProjectDir, store, options, FixedNow, dryRun: false);

        var swept = outcome.Decisions.Count(d => d.Action != SweepAction.Keep);
        Assert.Equal(3, swept);
        Assert.Equal(0, outcome.BytesFreed);
        Assert.Empty(outcome.Failures);

        Assert.Equal(
            [RunId(5), RunId(4)],
            store.ReadAllNewestFirst().Select(r => r.RunId).ToArray());
    }

    /// <summary>A run id in `pz`'s real shape (<c>yyyyMMddTHHmmssfff'Z'</c>, <see cref="RunRetention.TryParseRunTimestamp"/>)
    /// so <see cref="RunRetention.Decide"/>'s newest-first ordering is exercised exactly as it is for the
    /// local backend, not just ordinal string comparison.</summary>
    private static string RunId(int ordinal) => $"20260731T000000{ordinal:D3}Z";

    /// <summary>Never emits <see cref="SweepAction.DeleteStaging"/>: a remote candidate's
    /// <c>HasStaging</c> is always false (<c>SqlRunArtifactStore.ListCandidates</c>), and
    /// <see cref="RunSweeper.Sweep(string, IRunArtifactStore, RetentionOptions, System.DateTimeOffset, bool)"/>
    /// forces <c>Purge</c> on internally for a non-local store specifically so
    /// <see cref="RunRetention.Decide"/>'s "no staging database" branch is never reached -- if it were,
    /// nothing would ever be swept remotely and remote retention (bounding
    /// `pz.runs`/`run_nodes`/`run_events` growth) would silently fail.</summary>
    [SkippableFact]
    public void Sweep_never_selects_DeleteStaging_for_a_remote_candidate()
    {
        var store = NewStore(out _);
        for (var i = 1; i <= 3; i++)
        {
            store.WriteSnapshot(RunId(i), "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        }

        // Purge: false on the way in -- proves the force happens inside Sweep, not because the caller
        // asked for it.
        var options = new RetentionOptions(KeepLast: 1, OlderThan: null, Purge: false);
        var outcome = RunSweeper.Sweep(ProjectDir, store, options, FixedNow, dryRun: false);

        Assert.DoesNotContain(outcome.Decisions, d => d.Action == SweepAction.DeleteStaging);
        Assert.Equal(2, outcome.Decisions.Count(d => d.Action == SweepAction.DeleteDir));
    }

    /// <summary>There are no foreign keys between the three tables, so <see cref="IRunArtifactStore.Delete"/>
    /// must clear all three itself -- a partial delete would leave orphaned <c>run_nodes</c>/<c>run_events</c>
    /// rows that <see cref="IRunArtifactStore.ListCandidates"/> would never surface again (it only reads
    /// <c>runs</c>), so they would never get cleaned by a later sweep either.</summary>
    [SkippableFact]
    public void Delete_removes_the_run_from_all_three_tables_leaving_no_orphans()
    {
        var store = NewStore(out var connection);
        var runId = RunId(9);
        store.WriteSnapshot(runId, "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        InsertRunEvent(connection, runId);

        Assert.Equal(1, CountRows(connection, "runs", runId));
        Assert.Equal(1, CountRows(connection, "run_nodes", runId));
        Assert.Equal(1, CountRows(connection, "run_events", runId));

        store.Delete(runId);

        Assert.Equal(0, CountRows(connection, "runs", runId));
        Assert.Equal(0, CountRows(connection, "run_nodes", runId));
        Assert.Equal(0, CountRows(connection, "run_events", runId));
    }

    /// <summary>Same no-orphans guarantee, reached the way `pz clean`/automatic retention actually reach
    /// it -- through <see cref="RunSweeper.Sweep(string, IRunArtifactStore, RetentionOptions, System.DateTimeOffset, bool)"/>,
    /// not a direct <see cref="IRunArtifactStore.Delete"/> call.</summary>
    [SkippableFact]
    public void Swept_runs_leave_no_orphaned_rows_in_any_table()
    {
        var store = NewStore(out var connection);
        var sweptRunId = RunId(1);
        var keptRunId = RunId(2);
        store.WriteSnapshot(sweptRunId, "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        store.WriteSnapshot(keptRunId, "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        InsertRunEvent(connection, sweptRunId);
        InsertRunEvent(connection, keptRunId);

        var options = new RetentionOptions(KeepLast: 1, OlderThan: null, Purge: false);
        RunSweeper.Sweep(ProjectDir, store, options, FixedNow, dryRun: false);

        Assert.Equal(0, CountRows(connection, "runs", sweptRunId));
        Assert.Equal(0, CountRows(connection, "run_nodes", sweptRunId));
        Assert.Equal(0, CountRows(connection, "run_events", sweptRunId));

        Assert.Equal(1, CountRows(connection, "runs", keptRunId));
        Assert.Equal(1, CountRows(connection, "run_nodes", keptRunId));
        Assert.Equal(1, CountRows(connection, "run_events", keptRunId));
    }

    /// <summary>A dry run against a remote store must delete nothing -- same contract the local
    /// overload already guarantees.</summary>
    [SkippableFact]
    public void Dry_run_against_a_remote_store_deletes_nothing()
    {
        var store = NewStore(out _);
        for (var i = 1; i <= 3; i++)
        {
            store.WriteSnapshot(RunId(i), "2026-07-31T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");
        }

        var options = new RetentionOptions(KeepLast: 1, OlderThan: null, Purge: false);
        var outcome = RunSweeper.Sweep(ProjectDir, store, options, FixedNow, dryRun: true);

        Assert.Equal(2, outcome.Decisions.Count(d => d.Action == SweepAction.DeleteDir));
        Assert.Equal(3, store.ListCandidates().Count);
    }

    private static void InsertRunEvent(SqlStateConnection connection, string runId)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO ' + QUOTENAME(@schema) + " +
            "N'.run_events (run_id, seq, at, event, payload) VALUES (@run_id, 0, SYSUTCDATETIME(), " +
            "@event, @payload)'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64), @event NVARCHAR(64), @payload NVARCHAR(MAX)', " +
            "@run_id = @run_id, @event = @event, @payload = @payload;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);
        command.Parameters.AddWithValue("@event", "run_started");
        command.Parameters.AddWithValue("@payload", "{}");
        command.ExecuteNonQuery();
    }

    private static int CountRows(SqlStateConnection connection, string table, string runId)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT COUNT(*) FROM ' + QUOTENAME(@schema) + N'.' + " +
            "QUOTENAME(@table) + N' WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.AddWithValue("@run_id", runId);
        return (int)command.ExecuteScalar()!;
    }
}
