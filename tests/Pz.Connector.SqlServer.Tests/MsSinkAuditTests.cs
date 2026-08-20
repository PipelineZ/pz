using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Sink-side audit tests: three behaviors a production mart hits -- decimal
/// precision mismatch names both types, a pre-existing target with an extra identity column is
/// tolerated (explicit column list means identity fills itself), and unicode survives a merge
/// insert-then-update cycle.</summary>
[Collection("sqlserver")]
public sealed class MsSinkAuditTests(MsSqlContainerFixture fixture)
{
    private static readonly Schema KDecimalSchema = new(
    [
        new Field("k", Int32Type.Default, nullable: false),
        new Field("v", new Decimal128Type(38, 9), nullable: true),
    ], null);

    private static readonly Schema KStringSchema = new(
    [
        new Field("k", Int32Type.Default, nullable: false),
        new Field("v", StringType.Default, nullable: true),
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

    [SkippableFact]
    public async Task Precision_mismatch_against_pre_existing_target_names_both_types_and_the_column()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_prec";
        await DropAsync(table);
        await ExecAsync($"create table dbo.[{table}] (k int not null primary key, v decimal(18,2))");

        var connector = CreateSink();
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["k"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, KDecimalSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("decimal(18,2)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("decimal(38,9)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'v'", ex.Message, StringComparison.Ordinal);

        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Extra_identity_column_on_pre_existing_target_is_tolerated()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_ident";
        await DropAsync(table);
        await ExecAsync($"""
            create table dbo.[{table}] (
                audit_id int identity primary key,
                k int not null,
                v nvarchar(max),
                constraint ux_sink_ident_k unique (k)
            )
            """);

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["k"] };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, KStringSchema, CancellationToken.None))
        {
            var k = new Int32Array.Builder().Append(1).Append(2).Build();
            var v = new StringArray.Builder().Append("a").Append("b").Build();
            using (var batch = new RecordBatch(KStringSchema, [k, v], 2))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, KStringSchema, CancellationToken.None))
        {
            var k = new Int32Array.Builder().Append(1).Build();
            var v = new StringArray.Builder().Append("a2").Build();
            using (var batch = new RecordBatch(KStringSchema, [k, v], 1))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }

        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(
            $"select audit_id, k, v from dbo.[{table}] order by k", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(int AuditId, int K, string V)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2)));
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal("a2", rows.Single(r => r.K == 1).V);
        Assert.Equal("b", rows.Single(r => r.K == 2).V);
        var auditIds = rows.Select(r => r.AuditId).ToArray();
        Assert.All(auditIds, id => Assert.True(id != 0));
        Assert.Equal(auditIds.Length, auditIds.Distinct().Count());

        await DropAsync(table);
    }

    [SkippableFact]
    public async Task Unicode_survives_merge_insert_then_update()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_unicode";
        await DropAsync(table);

        var connector = CreateSink();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["k"] };

        const string first = "café 漢字 🚀";
        const string second = "café 漢字 🚀 v2";

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, KStringSchema, CancellationToken.None))
        {
            var k = new Int32Array.Builder().Append(1).Build();
            var v = new StringArray.Builder().Append(first).Build();
            using (var batch = new RecordBatch(KStringSchema, [k, v], 1))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, KStringSchema, CancellationToken.None))
        {
            var k = new Int32Array.Builder().Append(1).Build();
            var v = new StringArray.Builder().Append(second).Build();
            using (var batch = new RecordBatch(KStringSchema, [k, v], 1))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }

        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"select k, v from dbo.[{table}]", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(int K, string V)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetInt32(0), reader.GetString(1)));
        }

        var row = Assert.Single(rows);
        Assert.Equal(1, row.K);
        Assert.Equal(second, row.V);

        await DropAsync(table);
    }
}
