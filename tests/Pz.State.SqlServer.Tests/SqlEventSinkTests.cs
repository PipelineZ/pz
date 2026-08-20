using Microsoft.Data.SqlClient;
using Pz.Diagnostics.Events;
using Pz.State.SqlServer;
using Pz.TestSupport;

namespace Pz.State.SqlServer.Tests;

/// <summary><see cref="SqlEventSink"/>'s ordering and overflow contracts.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlEventSinkTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public async Task Persisted_events_replay_in_publish_order()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var sink = new SqlEventSink(connection, "run-1", TimeProvider.System);
        var published = new List<RunEvent>
        {
            new RunStartedEvent(DateTimeOffset.UnixEpoch, "run-1", "demo", 2),
            new NodeStartedEvent(DateTimeOffset.UnixEpoch, "run-1", "n1", "SourceLoad", "src_a"),
            new NodeCompletedEvent(DateTimeOffset.UnixEpoch, "run-1", "n1", "SourceLoad", "src_a",
                "success", 10, 5, null, null, null),
            new RunCompletedEvent(DateTimeOffset.UnixEpoch, "run-1", "success", 1, 0, 0, 5),
        };

        foreach (var evt in published)
        {
            sink.Write(evt);
        }

        await sink.DisposeAsync(); // flushes whatever the background task had not yet drained

        var stored = fixture.ReadEvents(connection, "run-1");

        Assert.Equal(["run_started", "node_started", "node_completed", "run_completed"],
            stored.Select(e => e.Event).ToArray());
        Assert.Equal([0L, 1L, 2L, 3L], stored.Select(e => e.Seq).ToArray());
    }

    /// <summary>A renderer whose writer is gated off cannot drain; publishing past
    /// <see cref="SqlEventSink.MaxBuffered"/> must drop rather than block, because
    /// <see cref="RunEventBus.Publish"/> must never stall the engine. The gate makes this deterministic
    /// rather than a timing race against a real background writer.</summary>
    [SkippableFact]
    public async Task Overflowing_the_buffer_drops_events_and_counts_them()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var sink = SqlEventSink.WithWriterGatedForTests(connection, "run-1", TimeProvider.System);

        for (var i = 0; i < SqlEventSink.MaxBuffered + 50; i++)
        {
            sink.Write(new NodeProgressEvent(DateTimeOffset.UnixEpoch, "run-1", "n1", "src_a", i, 0, 0));
        }

        Assert.Equal(50L, sink.Dropped);
        await sink.ReleaseWriterAndDisposeForTests();
    }

    /// <summary>A <see cref="SqlEventSink.Write"/> that races a completed
    /// <see cref="SqlEventSink.DisposeAsync"/> must still count its drop rather than silently losing the
    /// event. Driven deterministically (no timing window): disposing fully first guarantees the channel
    /// writer is completed, so the subsequent <see cref="SqlEventSink.Write"/> call is certain to hit the
    /// closed-channel path rather than racing it.</summary>
    [SkippableFact]
    public async Task Writing_after_dispose_counts_the_drop()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var sink = new SqlEventSink(connection, "run-1", TimeProvider.System);
        sink.Write(new RunStartedEvent(DateTimeOffset.UnixEpoch, "run-1", "demo", 1));
        await sink.DisposeAsync();

        sink.Write(new RunStartedEvent(DateTimeOffset.UnixEpoch, "run-1", "demo", 1));

        Assert.Equal(1L, sink.Dropped);
    }
}

/// <summary><see cref="SqlEventSink.DisposeAsync"/>'s own <c>events_dropped</c> UPDATE silently
/// no-ops when no <c>pz.runs</c> row exists yet for the run. <see cref="SqlRunArtifactStore.WriteSnapshot"/>'s
/// <c>eventsDropped</c> parameter (folded in by the composition site, never by the sink itself) must
/// still deliver the count, because it is guaranteed to create the row via its own upsert.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlEventSinkArtifactStoreIntegrationTests(SqlServerFixture fixture)
{
    [SkippableFact]
    public async Task Dropped_count_survives_when_the_sink_is_disposed_before_any_run_header_exists()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var sink = SqlEventSink.WithWriterGatedForTests(connection, "run-1", TimeProvider.System);
        for (var i = 0; i < SqlEventSink.MaxBuffered + 7; i++)
        {
            sink.Write(new NodeProgressEvent(DateTimeOffset.UnixEpoch, "run-1", "n1", "src_a", i, 0, 0));
        }

        Assert.Equal(7L, sink.Dropped);

        // Disposed BEFORE any run header row exists -- SqlEventSink's own dispose-time UPDATE
        // (WriteDroppedCountAsync) is a documented no-op here (zero rows affected, no error).
        await sink.ReleaseWriterAndDisposeForTests();

        var store = new SqlRunArtifactStore(connection, "test-project");
        store.WriteSnapshot("run-1", "2026-07-31T00:00:00.000Z", [], "success", sink.Dropped);

        Assert.Equal(7, ReadEventsDropped(connection, "run-1"));
    }

    private static int ReadEventsDropped(SqlStateConnection connection, string runId)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT events_dropped FROM ' + QUOTENAME(@schema) + " +
            "N'.runs WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);
        return (int)command.ExecuteScalar()!;
    }
}

/// <summary><see cref="SqlEventSink.DisposeAsync"/> must not propagate a drain-loop fault. No docker
/// needed: <see cref="SqlStateConnection"/>'s
/// constructor only stores its connection string -- it never opens anything until <c>Open()</c> is
/// called -- and <c>DrainLoopAsync</c>'s very first line calls <see cref="TimeProvider.GetUtcNow"/>,
/// before touching the connection or the channel. A <see cref="TimeProvider"/> that throws there faults
/// the drain task immediately, deterministically, with no timing window and no real SQL Server.
/// Deliberately outside <see cref="SqlEventSinkTests"/>'s collection: that class's constructor takes
/// <see cref="SqlServerFixture"/>, whose own constructor skips the whole collection without docker --
/// this test has no such dependency, so it must not inherit one.</summary>
public sealed class SqlEventSinkDisposeFaultTests
{
    private sealed class ThrowingTimeProvider : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => throw new InvalidOperationException("boom");
    }

    [Fact]
    public async Task DisposeAsync_does_not_propagate_a_drain_loop_fault()
    {
        // Connect Timeout=1: WriteDroppedCountAsync (which runs after the drain task's fault is caught)
        // still tries to open this connection and fails -- that failure is swallowed by the sink's
        // catch, but capped short so the test stays fast rather than waiting out a default connection
        // timeout.
        var connection = new SqlStateConnection("Server=unused;Database=unused;Connect Timeout=1;", "pz");
        var sink = new SqlEventSink(connection, "run-1", new ThrowingTimeProvider());

        await sink.DisposeAsync(); // must not throw
    }
}
