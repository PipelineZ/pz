using Microsoft.Data.SqlClient;
using Pz.Core.Dag;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;
using Pz.Engine.State;
using Pz.State.SqlServer;
using Pz.TestSupport;
using Pz.TestSupport.State;

namespace Pz.State.SqlServer.Tests;

[Collection(SqlServerFixture.CollectionName)]
public sealed class SqlRunArtifactStoreContractTests(SqlServerFixture fixture) : RunArtifactStoreContract
{
    /// <summary>Each store's backing connection, so <see cref="CorruptStoredRun"/> can reach the row
    /// directly without <see cref="NewStore"/> handing back anything but a fresh, independent store.</summary>
    private readonly Dictionary<IRunArtifactStore, SqlStateConnection> _connections = [];

    protected override IRunArtifactStore NewStore()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnection();
        SqlStateSchema.EnsureCurrent(connection);
        var store = new SqlRunArtifactStore(connection, "test-project");
        _connections[store] = connection;
        return store;
    }

    protected override NodeResult SucceededSourceLoad(string nodeId, string name) =>
        new(new NodeId(nodeId), NodeKind.SourceLoad, name, NodeStatus.Success, 0, TimeSpan.Zero, null);

    /// <summary>On SQL Server, "present but unreadable" is an unparseable `payload` column on the run's
    /// node row(s), written directly against the database rather than through the store -- same
    /// mechanism as <c>SqlKeyedStateStoreContractTests.CorruptStoredState</c>.</summary>
    protected override void CorruptStoredRun(IRunArtifactStore store, string runId)
    {
        var connection = _connections[store];
        using var sqlConnection = connection.Open();
        using var command = new SqlCommand(
            "DECLARE @sql NVARCHAR(MAX) = N'UPDATE ' + QUOTENAME(@schema) + " +
            "N'.run_nodes SET payload = @payload WHERE run_id = @run_id'; " +
            "EXEC sp_executesql @sql, N'@payload NVARCHAR(MAX), @run_id NVARCHAR(64)', " +
            "@payload = @payload, @run_id = @run_id;",
            sqlConnection);
        command.Parameters.AddWithValue("@schema", connection.Schema);
        command.Parameters.Add("@payload", System.Data.SqlDbType.NVarChar, -1).Value = "{ not json at all";
        command.Parameters.AddWithValue("@run_id", runId);
        command.ExecuteNonQuery();
    }

    /// <summary>The behaviour that differs from the local backend: a run's stored data is not a
    /// document, so a snapshot with a growing node list upserts rather than duplicates -- and the
    /// terminal snapshot's status sticks.</summary>
    [SkippableFact]
    public void Repeated_snapshots_upsert_nodes_rather_than_duplicating_them()
    {
        DockerFacts.SkipUnlessDocker();
        var store = NewStore();
        const string startedAt = "2026-07-31T00:00:00.000Z";

        store.WriteSnapshot("20260731T000001Z", startedAt, [SucceededSourceLoad("n1", "src_a")], "running");
        store.WriteSnapshot("20260731T000001Z", startedAt,
            [SucceededSourceLoad("n1", "src_a"), SucceededSourceLoad("n2", "src_b")], "running");
        store.WriteSnapshot("20260731T000001Z", startedAt,
            [SucceededSourceLoad("n1", "src_a"), SucceededSourceLoad("n2", "src_b")], "success");

        var run = store.ReadLatest()!;

        Assert.Equal(2, run.Nodes.Count);
        Assert.Equal("success", run.Status);
    }

    /// <summary><c>ReadRun</c> must populate <see cref="PriorNode.Observed"/> from the `payload`
    /// column's JSON, or `pz schema accept` (which reads it through
    /// <c>backends.Artifacts.ReadLatest()</c>) is silently inert under `state: {backend: sqlserver}`.
    /// Proves the round trip end-to-end through the real store (WriteSnapshot -> SQL Server ->
    /// ReadLatest), mirroring the local backend's equivalent coverage in RunResultsReaderTests.</summary>
    [SkippableFact]
    public void ReadLatest_round_trips_observed_schema()
    {
        DockerFacts.SkipUnlessDocker();
        var store = NewStore();
        const string startedAt = "2026-07-31T00:00:00.000Z";
        var observed = new ObservedSchema(
            [new SchemaColumn("id", "BIGINT"), new SchemaColumn("email", "VARCHAR")], "hh-abc123");
        var node = SucceededSourceLoad("n1", "src_a") with { Observed = observed };

        store.WriteSnapshot("20260731T000001Z", startedAt, [node], "success");

        var run = store.ReadLatest()!;
        var readBack = run.Nodes.Single().Observed;

        Assert.NotNull(readBack);
        Assert.Equal("hh-abc123", readBack.HintsHash);
        Assert.Equal(
            [("id", "BIGINT"), ("email", "VARCHAR")],
            readBack.Columns.Select(c => (c.Name, c.Type)).ToArray());
    }

    /// <summary>Same shape, but no `observed_schema` was ever written for the node -- "not observed"
    /// must stay null rather than becoming a spuriously non-null value.</summary>
    [SkippableFact]
    public void ReadLatest_leaves_observed_schema_null_when_never_written()
    {
        DockerFacts.SkipUnlessDocker();
        var store = NewStore();
        const string startedAt = "2026-07-31T00:00:00.000Z";

        store.WriteSnapshot("20260731T000001Z", startedAt, [SucceededSourceLoad("n1", "src_a")], "success");

        var run = store.ReadLatest()!;

        Assert.Null(run.Nodes.Single().Observed);
    }

    /// <summary>The O(N^2) regression this store used to have: <c>SnapshotRunEvents.NodeCompleted</c>
    /// hands the CUMULATIVE node list to every call, so a naive "upsert everything in the list" store
    /// costs 1+2+...+N round trips across N growing snapshots of an N-node run. Proves the fix is a true
    /// delta -- exactly one upsert per node, however many times its unchanged content is re-sent --
    /// by counting actual <c>UpsertNode</c> executions rather than trusting timing.</summary>
    [SkippableFact]
    public void Growing_snapshots_upsert_only_new_nodes()
    {
        DockerFacts.SkipUnlessDocker();
        var store = (SqlRunArtifactStore)NewStore();
        const string startedAt = "2026-07-31T00:00:00.000Z";
        const string runId = "20260731T000010Z";

        var nodes = new List<NodeResult>();
        for (var i = 1; i <= 5; i++)
        {
            nodes.Add(SucceededSourceLoad($"n{i}", $"src_{i}"));
            store.WriteSnapshot(runId, startedAt, nodes, "running");
        }

        // Without delta tracking this would be 1+2+3+4+5 = 15; with it, one upsert per node.
        Assert.Equal(5, store.NodeUpsertCountForTests);

        // The terminal-status snapshot RunCommand.ExecuteRun takes at the end of a run re-sends the
        // exact same cumulative list one more time -- must not upsert any node again.
        store.WriteSnapshot(runId, startedAt, nodes, "success");
        Assert.Equal(5, store.NodeUpsertCountForTests);

        var run = store.ReadLatest()!;
        Assert.Equal(5, run.Nodes.Count);
        Assert.Equal("success", run.Status);
    }

    /// <summary>The delta is by CONTENT, not just presence: <see cref="IRunArtifactStore.WriteSnapshot"/>'s
    /// contract is "new or changed", so a node id reported again with different content (never exercised
    /// by the one real caller, whose <c>NodeResult</c>s are immutable and reported once, but not excluded
    /// by the interface) must still land.</summary>
    [SkippableFact]
    public void A_node_reported_again_with_different_content_is_re_upserted()
    {
        DockerFacts.SkipUnlessDocker();
        var store = (SqlRunArtifactStore)NewStore();
        const string startedAt = "2026-07-31T00:00:00.000Z";
        const string runId = "20260731T000011Z";

        var node = SucceededSourceLoad("n1", "src_a");
        store.WriteSnapshot(runId, startedAt, [node], "running");
        Assert.Equal(1, store.NodeUpsertCountForTests);

        var updated = node with { RowsMoved = 42 };
        store.WriteSnapshot(runId, startedAt, [updated], "running");
        Assert.Equal(2, store.NodeUpsertCountForTests);

        Assert.Equal(42, store.ReadLatest()!.Nodes.Single().Rows);
    }

    /// <summary>A write whose transaction fails partway must not be marked written, or the next
    /// snapshot would wrongly believe the node is already durable and skip it forever.
    /// <see cref="SqlServerFixture.NewConnectionWithoutDdlRights"/>'s login authenticates fine but its
    /// database has no <c>pz</c> schema at all -- <c>WriteSnapshot</c>'s very first statement
    /// ("UPDATE pz.runs ...") fails with "Invalid object name", a deterministic failure independent of
    /// ANSI truncation/warning settings.</summary>
    [SkippableFact]
    public void A_failed_write_does_not_mark_the_node_as_written()
    {
        DockerFacts.SkipUnlessDocker();
        var connection = fixture.NewConnectionWithoutDdlRights();
        var store = new SqlRunArtifactStore(connection, "test-project");
        const string startedAt = "2026-07-31T00:00:00.000Z";
        const string runId = "20260731T000012Z";

        Assert.ThrowsAny<Exception>(() =>
            store.WriteSnapshot(runId, startedAt, [SucceededSourceLoad("n1", "src_a")], "running"));

        Assert.Equal(0, store.NodeUpsertCountForTests);
    }
}
