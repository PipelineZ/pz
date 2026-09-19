using Microsoft.Data.SqlClient;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.State;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

/// <summary>Proves <see cref="SqlRunArtifactStore"/> stamps <c>finished_at</c> through an injected
/// <see cref="TimeProvider"/> rather than the ambient wall clock (<c>DateTime.UtcNow</c>), and refuses
/// -- rather than silently truncates -- a project/node name or watermark cursor/type/value past the
/// length its <c>sp_executesql</c> parameters are declared with.</summary>
[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlRunArtifactStoreTimeProviderTests(SqlServerFixture fixture)
{
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static NodeResult SucceededSourceLoad(string nodeId, string name) =>
        new(new NodeId(nodeId), NodeKind.SourceLoad, name, NodeStatus.Success, 0, TimeSpan.Zero, null);

    private static DateTime ReadFinishedAt(SqlStateConnection connection, string runId)
    {
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT finished_at FROM ' + QUOTENAME(@schema) + " +
            "N'.runs WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", runId);
        return (DateTime)command.ExecuteScalar()!;
    }

    [SkippableFact]
    public void The_terminal_snapshot_stamps_finished_at_from_the_injected_TimeProvider_not_the_wall_clock()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var fixedNow = new DateTimeOffset(2026, 3, 14, 9, 26, 53, TimeSpan.Zero);
        var store = new SqlRunArtifactStore(connection, "test-project", new FixedTimeProvider(fixedNow));

        store.WriteSnapshot("20260314T000000Z", "2026-03-14T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "success");

        Assert.Equal(fixedNow.UtcDateTime, ReadFinishedAt(connection, "20260314T000000Z"));
    }

    /// <summary>A "running" (non-terminal) snapshot must never stamp <c>finished_at</c> at all -- proven
    /// alongside the terminal case so a future change to the terminal check cannot silently make every
    /// snapshot terminal without a test noticing.</summary>
    [SkippableFact]
    public void A_running_snapshot_leaves_finished_at_null()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);

        var store = new SqlRunArtifactStore(connection, "test-project", new FixedTimeProvider(DateTimeOffset.UnixEpoch));
        store.WriteSnapshot("20260314T000001Z", "2026-03-14T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "running");

        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'SELECT finished_at FROM ' + QUOTENAME(@schema) + " +
            "N'.runs WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@run_id NVARCHAR(64)', @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.AddWithValue("@run_id", "20260314T000001Z");
        Assert.Equal(DBNull.Value, command.ExecuteScalar());
    }

    /// <summary>Each over-long field would otherwise be assigned into its declared <c>sp_executesql</c>
    /// parameter silently truncated -- no warning, no error. Refused client-side instead, before the
    /// value ever reaches a command: no docker/network round trip needed, since the guard fires before
    /// any connection is opened.</summary>
    [Fact]
    public void WriteSnapshot_refuses_a_project_name_over_the_256_character_limit()
    {
        var store = new SqlRunArtifactStore(connection: null!, new string('p', 257));

        var ex = Assert.Throws<PzConfigException>(() =>
            store.WriteSnapshot("r1", "2026-03-14T00:00:00.000Z", [SucceededSourceLoad("n1", "src_a")], "running"));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
        Assert.Contains("256", ex.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WriteSnapshot_refuses_a_node_name_over_the_512_character_limit()
    {
        var store = new SqlRunArtifactStore(connection: null!, "test-project");
        var overLong = SucceededSourceLoad("n1", new string('n', 513));

        var ex = Assert.Throws<PzConfigException>(() =>
            store.WriteSnapshot("r1", "2026-03-14T00:00:00.000Z", [overLong], "running"));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
        Assert.Contains("512", ex.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WriteSnapshot_refuses_a_watermark_cursor_over_the_256_character_limit()
    {
        var store = new SqlRunArtifactStore(connection: null!, "test-project");
        var node = SucceededSourceLoad("n1", "src_a") with
        {
            WatermarkCandidate = new Watermark(new string('c', 257), "TIMESTAMP", "2026-03-14", "r1"),
        };

        var ex = Assert.Throws<PzConfigException>(() =>
            store.WriteSnapshot("r1", "2026-03-14T00:00:00.000Z", [node], "running"));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
        Assert.DoesNotContain(new string('c', 257), ex.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WriteSnapshot_refuses_a_watermark_value_over_the_256_character_limit()
    {
        var store = new SqlRunArtifactStore(connection: null!, "test-project");
        var node = SucceededSourceLoad("n1", "src_a") with
        {
            WatermarkCandidate = new Watermark("updated_at", "TIMESTAMP", new string('v', 257), "r1"),
        };

        var ex = Assert.Throws<PzConfigException>(() =>
            store.WriteSnapshot("r1", "2026-03-14T00:00:00.000Z", [node], "running"));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
    }

    [Fact]
    public void WriteSnapshot_refuses_a_watermark_type_over_the_64_character_limit()
    {
        var store = new SqlRunArtifactStore(connection: null!, "test-project");
        var node = SucceededSourceLoad("n1", "src_a") with
        {
            WatermarkCandidate = new Watermark("updated_at", new string('t', 65), "2026-03-14", "r1"),
        };

        var ex = Assert.Throws<PzConfigException>(() =>
            store.WriteSnapshot("r1", "2026-03-14T00:00:00.000Z", [node], "running"));

        Assert.Equal(PzErrorCode.SqlStateValueTooLong, ex.Error.Code);
        Assert.Contains("64", ex.Error.Message, StringComparison.Ordinal);
    }
}
