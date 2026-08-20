using Microsoft.Data.SqlClient;
using Pz.Core.Validation;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

using TestEntry = KeyedStateStoreContract.TestEntry;

/// <summary>Gate-based (no sleeps) proof that
/// <see cref="SqlKeyedStateStore{T}"/>'s optimistic concurrency actually rejects a stale writer instead
/// of silently clobbering or silently skipping. Two store instances stand in for two concurrent run
/// processes sharing one state database; each drives its own read-then-write sequence explicitly, so the
/// test is deterministic rather than racing real threads.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlKeyedStateStoreConcurrencyTests(SqlServerFixture fixture)
{
    private static SqlKeyedStateStore<TestEntry> NewStoreOn(SqlStateConnection connection) => new(
        connection, "concurrency",
        readEntry: static entry =>
        {
            var value = entry.GetProperty("value").GetString();
            var runId = entry.GetProperty("runId").GetString();
            return value is null || runId is null ? null : new TestEntry(value, runId);
        },
        writeEntry: static (writer, e) =>
        {
            writer.WriteString("value", e.Value);
            writer.WriteString("runId", e.RunId);
        });

    [SkippableFact]
    public void A_second_writer_that_read_the_same_version_is_PZ0520()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var first = NewStoreOn(connection);
        var second = NewStoreOn(connection);

        first.Set("a", new TestEntry("seed", "run-0"));

        // Both instances read the same version...
        Assert.NotNull(first.Get("a"));
        Assert.NotNull(second.Get("a"));

        // ...the first write succeeds and bumps the version...
        first.Set("a", new TestEntry("1", "run-1"));

        // ...so the second, still holding the stale version, must lose.
        var ex = Assert.Throws<PzConfigException>(() => second.Set("a", new TestEntry("2", "run-2")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
        Assert.Equal("1", first.Get("a")!.Value); // nothing was clobbered
    }

    [SkippableFact]
    public void An_insert_of_a_key_another_writer_already_inserted_is_PZ0520()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var first = NewStoreOn(connection);
        var second = NewStoreOn(connection);

        // Neither has read the key, so both carry no expected version.
        first.Set("fresh", new TestEntry("1", "run-1"));

        var ex = Assert.Throws<PzConfigException>(() => second.Set("fresh", new TestEntry("2", "run-2")));
        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
    }

    /// <summary>ONE store instance serves the whole run, and `Get` is
    /// called per node while SourceLoad nodes run in parallel under the topological dispatcher -- so the
    /// remembered-versions map is written from many threads at once. This drives that shape directly:
    /// every key is read concurrently, then each is written once (the real access pattern -- `Get` at
    /// plan time, `Set` at advancement). Every `Set` must take the compare-and-swap path, which only
    /// holds if no remembered version was lost; a lost one silently degrades that `Set` to
    /// insert-if-absent, which then conflicts with the row already there (PZ0520). So a single dropped
    /// entry fails this test loudly, and a corrupted bucket chain -- the worse outcome the unsynchronized
    /// dictionary also allowed -- shows up as a hang or an IndexOutOfRangeException here.</summary>
    [SkippableFact]
    public void Concurrent_reads_keep_every_remembered_version_for_a_later_write()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var store = NewStoreOn(connection);
        var keys = Enumerable.Range(0, 64).Select(i => $"k{i}").ToList();
        foreach (var key in keys)
        {
            store.Set(key, new TestEntry("seed", "run-0"));
        }

        // A fresh instance, so nothing is remembered from the seeding above: every version below comes
        // from a concurrent Get.
        var run = NewStoreOn(connection);
        Parallel.ForEach(keys, key => Assert.NotNull(run.Get(key)));

        // Would be PZ0520 for any key whose version the concurrent reads lost.
        foreach (var key in keys)
        {
            run.Set(key, new TestEntry("advanced", "run-1"));
        }

        Assert.All(keys, key => Assert.Equal("advanced", run.Get(key)!.Value));
    }

    /// <summary>The sibling test above only ever exercises the
    /// `WHERE NOT EXISTS` &#8594; zero-rows conflict path, because under SQL Server's default locking a
    /// second session's `INSERT ... WHERE NOT EXISTS` simply BLOCKS on the first session's row lock and
    /// re-evaluates after commit -- it never reaches the physical insert at all. `SqlKeyedStateStore`'s
    /// `SqlException.Number is 2627 or 2601` catch exists for a DIFFERENT interleaving: under
    /// READ_COMMITTED_SNAPSHOT (RCSI -- Azure SQL Database's default, and Azure is this project's stated
    /// deployment target), the `NOT EXISTS` check is a non-blocking snapshot read that does not see an
    /// still-uncommitted row, so a second session can pass the guard, then block on the physical
    /// insert's row lock, then hit the primary key once the first session commits. This test forces
    /// exactly that interleaving so the catch has real coverage instead of being untested dead code.
    ///
    /// Deterministic by ORDERING, not timing: a raw, uncommitted transaction holds the row lock no
    /// matter how long the background `Set()` takes to reach it, and the poll loop below blocks the
    /// test (not the two SQL sessions) until it has OBSERVED -- via `sys.dm_exec_requests`, not a guess
    /// -- that the background session is already waiting on that lock, i.e. that its snapshot read has
    /// already run and already missed the uncommitted row. Only then does the raw transaction commit.
    /// No sleep decides pass/fail; the poll's own short sleep is just a re-check interval bounded by a
    /// hard deadline, same shape as any other bounded-poll gate.</summary>
    [SkippableFact]
    public void An_RCSI_snapshot_read_that_misses_an_uncommitted_insert_still_conflicts_via_PZ0520()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);
        fixture.EnableReadCommittedSnapshot(connection);

        var store = NewStoreOn(connection);

        // A second, raw session inserts the SAME key store.Set() is about to target, and holds the
        // row lock by never committing until told to below.
        using var raw = connection.Open();
        var rawSpid = Convert.ToInt32(new SqlCommand("SELECT @@SPID", raw).ExecuteScalar());
        using var rawTransaction = raw.BeginTransaction();
        using (var insert = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = 'INSERT INTO ' + QUOTENAME(@schema) + " +
            "N'.state (scope, state_key, payload, version, updated_at) VALUES " +
            "(@scope, @key, @payload, 1, SYSUTCDATETIME())'; " +
            "EXEC sp_executesql @sql, N'@scope NVARCHAR(32), @key NVARCHAR(512), @payload NVARCHAR(MAX)', " +
            "@scope = @scope, @key = @key, @payload = @payload;",
            raw, rawTransaction))
        {
            insert.Parameters.AddWithValue("@schema", connection.Schema);
            insert.Parameters.AddWithValue("@scope", "concurrency");
            insert.Parameters.AddWithValue("@key", "raced");
            insert.Parameters.Add("@payload", System.Data.SqlDbType.NVarChar, -1).Value =
                "{\"value\":\"raw\",\"runId\":\"raw\"}";
            insert.ExecuteNonQuery();
        }

        // Never read by this store instance -- takes the insert-if-absent path, same as the sibling
        // test, but this time racing a session it cannot see under RCSI.
        var setTask = Task.Run(() => store.Set("raced", new TestEntry("2", "run-2")));

        using (var poll = connection.Open())
        {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            var blocked = false;
            while (DateTime.UtcNow < deadline)
            {
                using var check = new SqlCommand(
                    "SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id = @spid", poll);
                check.Parameters.AddWithValue("@spid", rawSpid);
                if ((int)check.ExecuteScalar()! > 0)
                {
                    blocked = true;
                    break;
                }

                Thread.Sleep(20);
            }

            Assert.True(blocked, "the background Set() never reached the row lock -- RCSI interleaving didn't happen");
        }

        rawTransaction.Commit();

        // Task.Wait(TimeSpan) throws (wrapped in AggregateException) as soon as the task faults --
        // it only returns false, without throwing, if the timeout elapses with the task still running.
        PzConfigException? conflict = null;
        var timedOut = false;
        try
        {
            timedOut = !setTask.Wait(TimeSpan.FromSeconds(10));
        }
        catch (AggregateException ex)
        {
            conflict = ex.InnerException as PzConfigException;
        }

        Assert.False(timedOut, "store.Set() did not complete after the blocking transaction committed");
        Assert.NotNull(conflict);
        Assert.Equal(PzErrorCode.StateConcurrencyConflict, conflict!.Error.Code);
    }
}
