using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Composite (multi-column) merge keys against the real Testcontainers instance -- every other
/// docker-backed merge/delete-apply suite (<see cref="SqlServerSinkAcceptance"/>, <see
/// cref="MsSinkAuditTests"/>, <see cref="SqlServerDeleteApplyTests"/>) uses a single-column key, so this is
/// the first end-to-end proof that a composite key actually creates the composite unique index, dedups
/// last-writer-wins across the FULL key, and applies deletes keyed on every column.</summary>
[Collection("sqlserver")]
public sealed class SqlServerCompositeKeyMergeTests(MsSqlContainerFixture fixture)
{
    private static readonly Schema RowSchema = new(
    [
        new Field("tenant", Int32Type.Default, nullable: false),
        new Field("id", Int32Type.Default, nullable: false),
        new Field("val", StringType.Default, nullable: false),
    ], null);

    private static readonly Schema KeySchema = new(
    [
        new Field("tenant", Int32Type.Default, nullable: false),
        new Field("id", Int32Type.Default, nullable: false),
    ], null);

    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    [SkippableFact]
    public async Task Composite_key_merge_upserts_and_dedups_last_writer_wins_across_the_full_key()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_composite_merge";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["tenant", "id"] };

        // Same (tenant, id) = (1, 1) pair twice in one batch with different values -- only the
        // arrival-order-last row must survive (StagingSequenceColumn tiebreak), proving dedup keys on
        // BOTH columns together, not just the first.
        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            await WriteRowsAsync(session, [(1, 1, "first"), (1, 1, "second"), (1, 2, "b"), (2, 1, "x")]);
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.Equal(
            new[] { (1, 1, "second"), (1, 2, "b"), (2, 1, "x") },
            await ReadRowsAsync(table));

        // A second merge: (1,1) and (2,1) update in place, (3,1) is a new key. A composite key never
        // collides across tenants sharing the same `id` -- (1,1) and (2,1) must update independently.
        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            await WriteRowsAsync(session, [(1, 1, "third"), (2, 1, "x2"), (3, 1, "y")]);
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.Equal(
            new[] { (1, 1, "third"), (1, 2, "b"), (2, 1, "x2"), (3, 1, "y") },
            await ReadRowsAsync(table));

        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Composite_key_merge_applies_deletes_keyed_on_every_column()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_composite_delete";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["tenant", "id"], OnDelete = "delete" };

        await using (var seed = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await seed.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            await WriteRowsAsync(session, [(1, 1, "a"), (1, 2, "b"), (2, 1, "x")]);
            await session.CommitAsync(CancellationToken.None);
        }

        // Delete (1, 1) only -- (2, 1) shares the SAME `id` under a different tenant and must survive,
        // proving the delete join matches on both key columns, not just the non-tenant one.
        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session);
            await WriteKeysAsync(deleteSession, [(1, 1)]);
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.Equal(new[] { (1, 2, "b"), (2, 1, "x") }, await ReadRowsAsync(table));

        await DropAsync(table);
    }

    private static async Task WriteRowsAsync(ISinkWriteSession session, (int Tenant, int Id, string Val)[] rows)
    {
        var tenant = new Int32Array.Builder();
        var id = new Int32Array.Builder();
        var val = new StringArray.Builder();
        foreach (var (t, i, v) in rows)
        {
            tenant.Append(t);
            id.Append(i);
            val.Append(v);
        }

        using var batch = new RecordBatch(RowSchema, [tenant.Build(), id.Build(), val.Build()], rows.Length);
        await session.WriteBatchAsync(batch, CancellationToken.None);
    }

    private static async Task WriteKeysAsync(IDeleteApplyingWriteSession session, (int Tenant, int Id)[] keys)
    {
        var tenant = new Int32Array.Builder();
        var id = new Int32Array.Builder();
        foreach (var (t, i) in keys)
        {
            tenant.Append(t);
            id.Append(i);
        }

        using var batch = new RecordBatch(KeySchema, [tenant.Build(), id.Build()], keys.Length);
        await session.ApplyDeleteKeysAsync(batch, CancellationToken.None);
    }

    private async Task<(int Tenant, int Id, string Val)[]> ReadRowsAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"select tenant, id, val from dbo.[{table}] order by tenant, id", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(int, int, string)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2)));
        }

        return [.. rows];
    }

    private async Task DropAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(connection, $"drop table if exists dbo.[{table}]");
    }
}
