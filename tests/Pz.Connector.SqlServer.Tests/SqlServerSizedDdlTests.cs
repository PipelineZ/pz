using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>End-to-end proof, against a real server, that the
/// resolve-derive-mirror pipeline (MsEffectiveTypes.Resolve -> MsDdl.EnsureTargetAsync ->
/// SqlServerSink.StagingTypes -> SqlServerSink.BuildBulkWriteMessage) lands the right column width
/// for a fresh target, respects/enforces a declared `columns:` type, tolerates a pre-existing target
/// whether hand-sized or old-pz-created nvarchar(max), and mirrors a sized target into merge staging.
/// Each case uses its own table name -- the container/database is shared across the collection.</summary>
[Collection("sqlserver")]
public sealed class SqlServerSizedDdlTests(MsSqlContainerFixture fixture)
{
    private static readonly Schema IdNoteSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("note", StringType.Default, nullable: true),
    ], null);

    private static ISinkConnector CreateSink() => new SqlServerConnector();

    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    private async Task ExecAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(connection, sql);
    }

    private async Task DropAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(connection, $"drop table if exists dbo.[{table}]");
    }

    private static RecordBatch OneRow(long id, string note)
    {
        var idArr = new Int64Array.Builder().Append(id).Build();
        var noteArr = new StringArray.Builder().Append(note).Build();
        return new RecordBatch(IdNoteSchema, [idArr, noteArr], 1);
    }

    /// <summary>sys.columns/sys.types -- type name plus raw
    /// max_length (bytes; nvarchar(n) reports 2n, nvarchar(max) reports -1).</summary>
    private async Task<(string TypeName, int MaxLength)> QueryColumnAsync(string table, string column)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            "select tp.name, c.max_length from sys.columns c " +
            "join sys.types tp on tp.user_type_id = c.user_type_id " +
            "where c.object_id = object_id(@t) and c.name = @c", connection);
        command.Parameters.AddWithValue("@t", $"dbo.{table}");
        command.Parameters.AddWithValue("@c", column);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(), $"column '{column}' not found on dbo.{table}");
        return (reader.GetString(0), reader.GetInt16(1));
    }

    // Case 1: derived -- observed 12 -> target 24 -> bucket 32 -> nvarchar(32) -> max_length 64 bytes.
    [SkippableFact]
    public async Task Derived_bucket_sizes_a_fresh_target()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_derived";
        await DropAsync(table);

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change", new Dictionary<string, object?>())
        { MaxTextLengths = new Dictionary<string, long> { ["note"] = 12 } };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "hello world!");
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(64, maxLength);

        await DropAsync(table);
    }

    // Case 2: fallback -- null stats -> nvarchar(4000) -> max_length 8000 bytes.
    [SkippableFact]
    public async Task Null_stats_fall_back_to_nvarchar4000()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_fallback";
        await DropAsync(table);

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change", new Dictionary<string, object?>());
        Assert.Null(spec.MaxTextLengths);

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "x");
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(8000, maxLength);

        await DropAsync(table);
    }

    // Case 3: a declared columns: entry wins over stats entirely, even when the stats would derive
    // a much wider bucket.
    [SkippableFact]
    public async Task Declared_columns_kwarg_wins_over_stats()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_declared";
        await DropAsync(table);

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["columns"] = new Dictionary<string, object?> { ["note"] = "nvarchar(20)" },
            })
        { MaxTextLengths = new Dictionary<string, long> { ["note"] = 3000 } };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "short");
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(40, maxLength);

        await DropAsync(table);
    }

    // Case 4: a pre-created, hand-sized target is accepted with no columns: kwarg at all -- the
    // relaxed check for undeclared string columns doesn't touch it.
    [SkippableFact]
    public async Task Preexisting_hand_sized_target_is_accepted()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_presized";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, note nvarchar(50))");

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change", new Dictionary<string, object?>())
        { MaxTextLengths = new Dictionary<string, long> { ["note"] = 12 } };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "hello");
            await session.WriteBatchAsync(batch, CancellationToken.None);
            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(1, result.RowsWritten);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(100, maxLength); // untouched: still nvarchar(50)

        await DropAsync(table);
    }

    // Case 5: the compat case -- an old pz-created nvarchar(max) target is still accepted.
    [SkippableFact]
    public async Task Preexisting_nvarchar_max_target_is_accepted()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_presized_max";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, note nvarchar(max))");

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change", new Dictionary<string, object?>())
        { MaxTextLengths = new Dictionary<string, long> { ["note"] = 12 } };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "hello");
            await session.WriteBatchAsync(batch, CancellationToken.None);
            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(1, result.RowsWritten);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(-1, maxLength); // still nvarchar(max)

        await DropAsync(table);
    }

    // Case 6: exactness is still enforced when columns: IS declared -- a mismatch against a
    // pre-existing target fails naming the expected declared type.
    [SkippableFact]
    public async Task Declared_type_mismatch_against_preexisting_target_fails()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_mismatch";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, note nvarchar(50))");

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["columns"] = new Dictionary<string, object?> { ["note"] = "nvarchar(20)" },
            });

        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("expected 'nvarchar(20)'", ex.Message, StringComparison.Ordinal);

        await DropAsync(table);
    }

    // Case 7: a value too wide for a pre-existing (undeclared, hence accepted) narrow column fails
    // the bulk write with the 2628/8152 remediation hint pointing at columns:.
    [SkippableFact]
    public async Task Truncation_error_carries_the_columns_kwarg_hint()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_truncate";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, note nvarchar(4))");

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "append", "fail_on_change", new Dictionary<string, object?>());

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            using var batch = OneRow(1, "way too long");
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                async () => await session.WriteBatchAsync(batch, CancellationToken.None));
            Assert.Contains("columns:", ex.Message, StringComparison.Ordinal);
        }

        await DropAsync(table);
    }

    // Case 8: merge against a sized pre-existing target -- the #temp staging mirror (StagingTypes)
    // takes the sized path (not nvarchar(max)) and the merge still upserts correctly.
    [SkippableFact]
    public async Task Merge_upserts_against_a_sized_target()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "szddl_merge";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, note nvarchar(50))");
        await ExecAsync($"create unique index ux_{table}_id on dbo.[{table}] (id)");
        await ExecAsync($"insert into dbo.[{table}] (id, note) values (1, N'old')");

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change", new Dictionary<string, object?>())
        { Keys = ["id"], MaxTextLengths = new Dictionary<string, long> { ["note"] = 12 } };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, IdNoteSchema, CancellationToken.None))
        {
            var id = new Int64Array.Builder().Append(1).Append(2).Build();
            var note = new StringArray.Builder().Append("updated").Append("new").Build();
            using (var batch = new RecordBatch(IdNoteSchema, [id, note], 2))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(2, result.RowsWritten);
        }

        await using (var connection = new SqlConnection(fixture.ConnectionString))
        {
            await connection.OpenAsync();
            await using var command = new SqlCommand($"select id, note from dbo.[{table}] order by id", connection);
            await using var reader = await command.ExecuteReaderAsync();
            var rows = new List<(long Id, string Note)>();
            while (await reader.ReadAsync())
            {
                rows.Add((reader.GetInt64(0), reader.GetString(1)));
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal("updated", rows.Single(r => r.Id == 1).Note);
            Assert.Equal("new", rows.Single(r => r.Id == 2).Note);
        }

        var (typeName, maxLength) = await QueryColumnAsync(table, "note");
        Assert.Equal("nvarchar", typeName);
        Assert.Equal(100, maxLength); // target untouched, still nvarchar(50)

        await DropAsync(table);
    }
}
