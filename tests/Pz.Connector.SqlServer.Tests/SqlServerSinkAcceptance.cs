using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class SqlServerSinkAcceptance(MsSqlContainerFixture fixture) : SinkConnectorAcceptanceTests
{
    private const string SmallTable = "sink_accept_small";
    private const string MergeTable = "sink_accept_merge";
    private const string ReplaceTable = "sink_accept_replace";

    private static readonly Schema IdNameSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    protected override void GateFact() => DockerFacts.SkipUnlessDocker();

    protected override ISinkConnector CreateSink() => new SqlServerConnector();

    protected override ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    protected override OutputSpec SmallOutput => new("ms", SmallTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override OutputSpec? MergeOutput => new("ms", MergeTable, "merge", "fail_on_change",
        new Dictionary<string, object?>()) { Keys = ["id"] };

    protected override OutputSpec? ReplaceOutput => new("ms", ReplaceTable, "replace", "fail_on_change",
        new Dictionary<string, object?>());

    protected override Task ResetMergeTargetAsync() => DropAsync(MergeTable);

    protected override async ValueTask<IReadOnlyList<RecordBatch>> ReadCommittedAsync(ISinkConnector connector, OutputSpec spec)
    {
        var table = spec.Output;
        await using var sqlConnector = await ((ISourceConnector)connector).OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await sqlConnector.PlanReadAsync(
            new DatasetSpec("ms", table, new Dictionary<string, object?> { ["query"] = $"select id, name from dbo.[{table}] order by id" }),
            ReadHints.None, CancellationToken.None);
        var batches = new List<RecordBatch>();
        await foreach (var b in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        return batches;
    }

    private async Task DropAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(connection, $"drop table if exists dbo.[{table}]");
    }

    private async Task ExecAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await MsSqlContainerFixture.ExecuteAsync(connection, sql);
    }

    [SkippableFact]
    public async Task Merge_on_existing_table_without_unique_index_errors_with_hint()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_accept_merge_nounique";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (id bigint not null, name nvarchar(max) not null)");

        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["id"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("id", ex.Message, StringComparison.Ordinal);
        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Replace_on_fk_referenced_target_falls_back_to_delete()
    {
        DockerFacts.SkipUnlessDocker();
        const string parent = "sink_fk_parent";
        const string child = "sink_fk_child";
        await ExecAsync($"drop table if exists dbo.[{child}]");
        await ExecAsync($"drop table if exists dbo.[{parent}]");
        // Target referenced by an FK: TRUNCATE raises 4712, the session must DELETE instead and
        // still commit the replace.
        await ExecAsync($"create table dbo.[{parent}] (id bigint not null primary key, name nvarchar(max))");
        await ExecAsync($"insert into dbo.[{parent}] values (999, N'stale')");
        await ExecAsync($"create table dbo.[{child}] (pid bigint references dbo.[{parent}](id))");

        var spec = new OutputSpec("ms", parent, "replace", "fail_on_change",
            new Dictionary<string, object?>());
        // The pre-existing target has a PK/not-null shape; write a matching id/name batch.
        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        await using var session = await sink.BeginWriteAsync(spec, IdNameSchema, CancellationToken.None);
        var id = new Int64Array.Builder().Append(1).Build();
        var name = new StringArray.Builder().Append("fresh").Build();
        using (var batch = new RecordBatch(IdNameSchema, [id, name], 1))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(1, result.RowsWritten);

        var committed = await ReadCommittedAsync(connector, spec);
        Assert.Equal(1, committed.Sum(b => b.Length)); // 999/'stale' gone, 1/'fresh' present
        await ExecAsync($"drop table if exists dbo.[{child}]");
        await DropAsync(parent);
    }
}
