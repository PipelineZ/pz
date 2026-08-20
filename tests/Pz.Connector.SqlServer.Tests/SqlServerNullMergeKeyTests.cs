using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Pins the sink's actual behavior for a NULL merge-key value against a pre-existing target with
/// a nullable key column -- MsDdl.BuildCreateTableSql only marks key columns `not null` when the sink
/// AUTO-creates the table (Keys is otherwise unconstrained by the ABI), and MERGE's `t.[k] = s.[k]` ON
/// clause never matches NULL (three-valued SQL comparison), so nothing enforces or documents what happens
/// on a pre-existing target whose key column allows it. This is a proof of the ACTUAL outcome, not an
/// assertion of a "correct" one -- undocumented today, and a regression net so a future change to it is
/// deliberate.</summary>
[Collection("sqlserver")]
public sealed class SqlServerNullMergeKeyTests(MsSqlContainerFixture fixture)
{
    private static readonly Schema KVSchema = new(
    [
        new Field("k", Int32Type.Default, nullable: true),
        new Field("v", StringType.Default, nullable: false),
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
    public async Task Second_null_keyed_merge_violates_the_targets_unique_index_instead_of_silently_duplicating()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "sink_null_key";
        await DropAsync(table);
        // A pre-existing target: nullable key column + a unique index the merge-key precondition
        // requires (mirrors Merge_on_existing_table_without_unique_index_errors_with_hint's setup).
        // SQL Server's UNIQUE index -- unlike Postgres -- permits at most ONE null value, so a second
        // null-keyed row is a real constraint violation, not silent accumulation: MERGE's ON clause
        // never matches NULL = NULL, so WHEN NOT MATCHED always re-inserts a null-keyed row rather
        // than updating the first one.
        await ExecAsync($"create table dbo.[{table}] (k int null, v nvarchar(max) not null)");
        await ExecAsync($"create unique index ux_{table}_k on dbo.[{table}] (k)");

        ISinkConnector connector = new SqlServerConnector();
        var spec = new OutputSpec("ms", table, "merge", "fail_on_change",
            new Dictionary<string, object?>()) { Keys = ["k"] };

        await using (var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var session = await sink.BeginWriteAsync(spec, KVSchema, CancellationToken.None))
        {
            await WriteRowAsync(session, null, "first");
            await session.CommitAsync(CancellationToken.None);
        }

        Assert.Equal([(null, "first")], await ReadRowsAsync(table));

        PzConnectorException ex;
        await using (var laterSink = await connector.OpenAsync(ValidConfig, CancellationToken.None))
        await using (var laterSession = await laterSink.BeginWriteAsync(spec, KVSchema, CancellationToken.None))
        {
            await WriteRowAsync(laterSession, null, "second");
            ex = await Assert.ThrowsAsync<PzConnectorException>(
                async () => await laterSession.CommitAsync(CancellationToken.None));
        }
        // The DisposeAsync above rolls back the failed-commit transaction (SqlTransaction.Dispose's
        // implicit rollback) -- doing it before reading matters: otherwise the still-open transaction's
        // lock on the table blocks the read from a separate connection.

        Assert.Contains("unique", ex.Message, StringComparison.OrdinalIgnoreCase);

        // The first run's row is untouched -- the second run's commit failed outright, not partially.
        Assert.Equal([(null, "first")], await ReadRowsAsync(table));

        await DropAsync(table);
    }

    private static async Task WriteRowAsync(ISinkWriteSession session, int? k, string v)
    {
        var kBuilder = new Int32Array.Builder();
        if (k is null)
        {
            kBuilder.AppendNull();
        }
        else
        {
            kBuilder.Append(k.Value);
        }

        var vBuilder = new StringArray.Builder().Append(v);
        using var batch = new RecordBatch(KVSchema, [kBuilder.Build(), vBuilder.Build()], 1);
        await session.WriteBatchAsync(batch, CancellationToken.None);
    }

    private async Task<(int? K, string V)[]> ReadRowsAsync(string table)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand($"select k, v from dbo.[{table}] order by v", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(int?, string)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.IsDBNull(0) ? null : reader.GetInt32(0), reader.GetString(1)));
        }

        return [.. rows];
    }

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
}
