using Apache.Arrow;
using Apache.Arrow.Types;
using Npgsql;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;
using Pz.TestSupport;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Runs the TestKit's sink acceptance suite, merge facts included, against
/// the real <see cref="PostgresConnector"/>, against a Testcontainers postgres instance (see
/// <see cref="PostgresContainerFixture"/>). <see cref="SmallOutput"/> uses "replace" mode against a fixed
/// table name: unlike InMemory's per-<see cref="CreateSink"/>-call fresh store, a postgres target
/// genuinely persists in the shared container across facts, and "replace" tolerates that (each commit
/// truncates first). <see cref="MergeOutput"/>'s table is instead dropped before every merge fact via
/// <see cref="ResetMergeTargetAsync"/>, since the merge facts need a known starting state and the
/// suite must survive being run twice in a row without manual cleanup.</summary>
[Collection("postgres")]
public sealed class PostgresSinkAcceptance(PostgresContainerFixture fixture) : SinkConnectorAcceptanceTests
{
    private const string SmallTable = "sink_accept_small";
    private const string MergeTable = "sink_accept_merge";
    private const string ReplaceTable = "sink_accept_replace";

    private static readonly Schema IdNameSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    // Belt-and-braces alongside the fixture's own constructor-level skip: every INHERITED fact calls
    // this first, so a docker-less run SKIPs cleanly. See PostgresContainerFixture's doc comment.
    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new PostgresConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
    });

    protected override OutputSpec SmallOutput => new("pg", SmallTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput => new("pg", MergeTable, "merge", "fail_on_change",
        new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override OutputSpec? ReplaceOutput => new("pg", ReplaceTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override Task ResetMergeTargetAsync() => DropAsync(MergeTable);

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var table = spec.Output;
        return await ReadIdNameTableAsync(table);
    }

    [SkippableFact]
    public async Task Merge_on_existing_table_without_unique_constraint_errors_with_hint()
    {
        const string table = "sink_accept_merge_nounique";
        await DropAsync(table);
        await ExecuteAsync($"create table public.\"{table}\" (id bigint not null, name text not null)");

        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("id", ex.Message, StringComparison.Ordinal);

        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Merge_on_existing_table_with_partial_unique_index_errors_with_hint()
    {
        const string table = "sink_accept_merge_partialunique";
        await DropAsync(table);
        await ExecuteAsync($"create table public.\"{table}\" (id bigint not null, name text not null)");
        // A PARTIAL unique index covers exactly the key column set but cannot back ON CONFLICT (id) --
        // postgres requires a non-partial, non-expression arbiter index/constraint.
        await ExecuteAsync($"create unique index on public.\"{table}\" (id) where name is not null");

        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("id", ex.Message, StringComparison.Ordinal);

        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Merge_key_only_table_degrades_to_do_nothing()
    {
        const string table = "sink_accept_merge_keyonly";
        await DropAsync(table);

        var idOnlySchema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("pg", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"] };

        await using (var session = await sink.BeginWriteAsync(spec, idOnlySchema, CancellationToken.None))
        {
            var builder = new ArrowBatchBuilder(idOnlySchema);
            builder.AppendRow([1L]);
            builder.AppendRow([2L]);
            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        // Re-commits id=1 (already present) alongside a genuinely new id=3 -- with no non-key columns to
        // SET, "do nothing" must let the conflicting row survive untouched and the new row insert cleanly
        // (not throw a duplicate-key violation).
        await using (var session = await sink.BeginWriteAsync(spec, idOnlySchema, CancellationToken.None))
        {
            var builder = new ArrowBatchBuilder(idOnlySchema);
            builder.AppendRow([1L]);
            builder.AppendRow([3L]);
            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var count = await CountAsync(table);
        Assert.Equal(3L, count);

        await DropAsync(table);
    }

    private async Task<IReadOnlyList<RecordBatch>> ReadIdNameTableAsync(string table)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        var exists = await TableExistsAsync(connection, table).ConfigureAwait(false);
        if (!exists)
        {
            return [];
        }

        await using var command = new NpgsqlCommand($"select id, name from public.\"{table}\" order by id", connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        var rowCount = 0;
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            idBuilder.Append(reader.GetInt64(0));
            nameBuilder.Append(reader.GetString(1));
            rowCount++;
        }

        return [new RecordBatch(IdNameSchema, [idBuilder.Build(), nameBuilder.Build()], rowCount)];
    }

    private async Task<bool> TableExistsAsync(NpgsqlConnection connection, string table)
    {
        await using var command = new NpgsqlCommand(
            "select 1 from information_schema.tables where table_schema = 'public' and table_name = @table",
            connection);
        command.Parameters.AddWithValue("table", table);
        return await command.ExecuteScalarAsync().ConfigureAwait(false) is not null;
    }

    private async Task<long> CountAsync(string table)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"select count(*) from public.\"{table}\"", connection);
        return (long)(await command.ExecuteScalarAsync().ConfigureAwait(false))!;
    }

    private async Task DropAsync(string table) => await ExecuteAsync($"drop table if exists public.\"{table}\"");

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(fixture.ConnectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }
}
