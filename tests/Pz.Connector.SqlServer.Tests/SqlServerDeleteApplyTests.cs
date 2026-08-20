using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary><see cref="SqlServerSinkWriteSession"/>'s
/// <see cref="IDeleteApplyingWriteSession"/> surface -- hard delete (<c>on_delete: delete</c>) and soft
/// delete (<c>on_delete: soft</c>, nullable <c>_pz_deleted_at</c>) against the real Testcontainers
/// instance shared by <see cref="MsSqlContainerFixture"/>.</summary>
[Collection("sqlserver")]
public sealed class SqlServerDeleteApplyTests(MsSqlContainerFixture fixture)
{
    private static readonly Schema RowSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("val", Int32Type.Default, nullable: false),
    ], null);

    private static readonly Schema KeySchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
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
    public async Task Hard_delete_applies_transactionally()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_del_hard";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"], OnDelete = "delete" };

        await SeedAsync(sink, spec, [(1L, 10), (2L, 20), (3L, 30)]);

        await using var reader = new SqlConnection(fixture.ConnectionString);
        await reader.OpenAsync().ConfigureAwait(false);

        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session);

            await WriteRowsAsync(session, [(2L, 200)]);
            await WriteKeysAsync(deleteSession, [3L]);

            // Transactionality: a concurrent reader on a SEPARATE connection must still see the
            // pre-commit state (all 3 rows) -- the delete only takes effect at CommitAsync.
            var midCount = await ScalarAsync<int>(reader, $"select count(*) from dbo.[{table}]");
            Assert.Equal(3, midCount);

            await session.CommitAsync(CancellationToken.None);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal([(1L, 10), (2L, 200)], rows);
    }

    [SkippableFact]
    public async Task Soft_delete_sets_flag_and_later_reupsert_clears_it()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_del_soft";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"], OnDelete = "soft" };

        await SeedAsync(sink, spec, [(1L, 10), (2L, 20), (3L, 30)]);

        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session);
            await WriteRowsAsync(session, [(2L, 200)]);
            await WriteKeysAsync(deleteSession, [3L]);
            await session.CommitAsync(CancellationToken.None);
        }

        var flags = await ReadDeletedFlagsAsync(table);
        Assert.False(flags[1L]);
        Assert.False(flags[2L]);
        Assert.True(flags[3L]);
        var rows = await ReadRowsAsync(table);
        Assert.Equal([(1L, 10), (2L, 200), (3L, 30)], rows);

        // A LATER session re-upserting key 3 must clear its _pz_deleted_at flag.
        await using (var session2 = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            await WriteRowsAsync(session2, [(3L, 300)]);
            await session2.CommitAsync(CancellationToken.None);
        }

        var flagsAfter = await ReadDeletedFlagsAsync(table);
        Assert.False(flagsAfter[3L]);
        var rowsAfter = await ReadRowsAsync(table);
        Assert.Equal([(1L, 10), (2L, 200), (3L, 300)], rowsAfter);
    }

    [SkippableFact]
    public async Task Soft_delete_column_schema_policy_gates_pre_existing_target()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_del_schema";
        await DropAsync(table);
        await ExecuteAsync($"create table dbo.[{table}] (id bigint not null, val int not null, constraint [pk_{table}] primary key clustered (id))");

        ISinkConnector connector = new SqlServerConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"], OnDelete = "soft" };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("_pz_deleted_at", ex.Message, StringComparison.Ordinal);

        var additiveSpec = spec with { SchemaPolicy = "additive" };
        await using (var session = await sink.BeginWriteAsync(additiveSpec, RowSchema, CancellationToken.None))
        {
            await session.CommitAsync(CancellationToken.None);
        }

        var columns = await LoadColumnTypesAsync(table);
        Assert.True(columns.ContainsKey("_pz_deleted_at"));
        Assert.Equal("datetime2", columns["_pz_deleted_at"]);
    }

    [SkippableFact]
    public async Task Abort_after_delete_apply_leaves_target_untouched()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_del_abort";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"], OnDelete = "delete" };

        await SeedAsync(sink, spec, [(1L, 10), (2L, 20), (3L, 30)]);

        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session);
            await WriteRowsAsync(session, [(2L, 999)]);
            await WriteKeysAsync(deleteSession, [3L]);
            await session.AbortAsync(CancellationToken.None);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal([(1L, 10), (2L, 20), (3L, 30)], rows);
    }

    [SkippableFact]
    public async Task Delete_replay_of_absent_key_is_idempotent()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "cdc_del_idempotent";
        await DropAsync(table);

        ISinkConnector connector = new SqlServerConnector();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"], OnDelete = "delete" };

        await SeedAsync(sink, spec, [(1L, 10), (2L, 20), (3L, 30)]);

        // Same key batch applied twice within one session -- must not error, and must produce the
        // same result as applying it once.
        await using (var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session);
            await WriteKeysAsync(deleteSession, [3L]);
            await WriteKeysAsync(deleteSession, [3L]);
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.Equal([(1L, 10), (2L, 20)], await ReadRowsAsync(table));

        // A LATER session deleting an already-absent key is a no-op, not an error.
        await using (var session2 = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None))
        {
            var deleteSession2 = Assert.IsAssignableFrom<IDeleteApplyingWriteSession>(session2);
            await WriteKeysAsync(deleteSession2, [3L]);
            await session2.CommitAsync(CancellationToken.None);
        }

        Assert.Equal([(1L, 10), (2L, 20)], await ReadRowsAsync(table));
    }

    private static async Task WriteRowsAsync(ISinkWriteSession session, (long Id, int Val)[] rows)
    {
        var builder = new ArrowBatchBuilder(RowSchema);
        foreach (var (id, val) in rows)
        {
            builder.AppendRow([id, val]);
        }

        using var batch = builder.Flush()!;
        await session.WriteBatchAsync(batch, CancellationToken.None);
    }

    private static async Task WriteKeysAsync(IDeleteApplyingWriteSession session, long[] keys)
    {
        var builder = new ArrowBatchBuilder(KeySchema);
        foreach (var key in keys)
        {
            builder.AppendRow([key]);
        }

        using var batch = builder.Flush()!;
        await session.ApplyDeleteKeysAsync(batch, CancellationToken.None);
    }

    private async Task SeedAsync(ISink sink, OutputSpec spec, (long Id, int Val)[] rows)
    {
        await using var session = await sink.BeginWriteAsync(spec, RowSchema, CancellationToken.None);
        await WriteRowsAsync(session, rows);
        await session.CommitAsync(CancellationToken.None);
    }

    private async Task DropAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await MsSqlContainerFixture.ExecuteAsync(connection, $"drop table if exists dbo.[{table}]");
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await MsSqlContainerFixture.ExecuteAsync(connection, sql);
    }

    private static async Task<T> ScalarAsync<T>(SqlConnection connection, string sql)
    {
        await using var command = new SqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
        return (T)Convert.ChangeType(result!, typeof(T), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<(long Id, int Val)[]> ReadRowsAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new SqlCommand($"select id, val from dbo.[{table}] order by id", connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var rows = new List<(long, int)>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            rows.Add((reader.GetInt64(0), reader.GetInt32(1)));
        }

        return [.. rows];
    }

    private async Task<Dictionary<long, bool>> ReadDeletedFlagsAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new SqlCommand($"select id, [_pz_deleted_at] from dbo.[{table}]", connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var flags = new Dictionary<long, bool>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            flags[reader.GetInt64(0)] = !reader.IsDBNull(1);
        }

        return flags;
    }

    private async Task<Dictionary<string, string>> LoadColumnTypesAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new SqlCommand(
            "select c.name, tp.name from sys.columns c " +
            "join sys.types tp on c.user_type_id = tp.user_type_id " +
            "join sys.tables t on c.object_id = t.object_id " +
            "where t.name = @table",
            connection);
        command.Parameters.AddWithValue("@table", table);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var columns = new Dictionary<string, string>(StringComparer.Ordinal);
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            columns[reader.GetString(0)] = reader.GetString(1);
        }

        return columns;
    }
}
