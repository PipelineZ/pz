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
}
