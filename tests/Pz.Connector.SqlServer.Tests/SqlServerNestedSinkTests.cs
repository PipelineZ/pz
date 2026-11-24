using Apache.Arrow;
using Apache.Arrow.Types;
using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Nested columns into SQL Server: a STRUCT, LIST or MAP lands as its JSON text in an
/// <c>nvarchar(max)</c> column, readable with SQL Server's own JSON functions, through append and merge alike.</summary>
[Collection("sqlserver")]
public sealed class SqlServerNestedSinkTests(MsSqlContainerFixture fixture)
{
    private ConnectorConfig ValidConfig => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["database"] = fixture.Database,
        ["user"] = fixture.User,
        ["password"] = fixture.Password,
        ["trust_server_certificate"] = true,
    });

    private static readonly StructType RecType = new([
        new Field("n", Int32Type.Default, true),
        new Field("tags", new ListType(new Field("item", StringType.Default, true)), true),
    ]);

    private static RecordBatch Batch(long firstId)
    {
        var ids = new Int64Array.Builder().Append(firstId).Append(firstId + 1).Build();

        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        tags.AppendNull();
        var rec = new StructArray(RecType, 2,
            [new Int32Array.Builder().Append(7).AppendNull().Build(), tags.Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(true).Build(), nullCount: 0);

        var xs = new ListArray.Builder(Int64Type.Default);
        var xValues = (Int64Array.Builder)xs.ValueBuilder;
        xs.Append();
        xValues.Append(1);
        xValues.Append(2);
        xs.AppendNull();

        var map = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        map.Append();
        ((StringArray.Builder)map.KeyBuilder).Append("k");
        ((Int32Array.Builder)map.ValueBuilder).Append(firstId == 0 ? 1 : 2);
        map.AppendNull();

        var xsArray = xs.Build();
        var mapArray = map.Build();
        var schema = new Schema([
            new Field("id", Int64Type.Default, false),
            new Field("rec", RecType, true),
            new Field("xs", xsArray.Data.DataType, true),
            new Field("m", mapArray.Data.DataType, true),
        ], null);
        return new RecordBatch(schema, [ids, rec, xsArray, mapArray], 2);
    }

    private async Task WriteAsync(string table, string mode, long firstId)
    {
        ISinkConnector connector = new SqlServerConnector();
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.NestedTypes));
        await using var sink = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        using var batch = Batch(firstId);
        var spec = new OutputSpec("mssql", table, mode, "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = mode == "merge" ? ["id"] : [],
        };
        await using var session = await sink.BeginWriteAsync(spec, batch.Schema, CancellationToken.None);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        Assert.Equal(2, (await session.CommitAsync(CancellationToken.None)).RowsWritten);
    }

    private async Task<List<string>> QueryAsync(string sql)
    {
        await using var connection = new SqlConnection(fixture.ConnectionString);
        await connection.OpenAsync();
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<string>();
        while (await reader.ReadAsync())
        {
            rows.Add(string.Join(" | ", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "NULL" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    [SkippableFact]
    public async Task Nested_columns_land_as_JSON_text_in_nvarchar_max()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "nested_append";
        await QueryAsync($"drop table if exists dbo.[{table}]");

        await WriteAsync(table, "append", 0);

        Assert.Equal(
            ["rec | nvarchar | -1", "xs | nvarchar | -1", "m | nvarchar | -1"],
            await QueryAsync(
                "select c.name, tp.name, c.max_length from sys.columns c join sys.types tp on tp.user_type_id = c.user_type_id " +
                $"where c.object_id = object_id('dbo.{table}') and c.name <> 'id' order by c.column_id"));
        Assert.Equal(
            [
                """0 | {"n":7,"tags":["a",null]} | [1,2] | {"k":1} | 7 | 1""",
                """1 | {"n":null,"tags":null} | NULL | NULL | NULL | 1""",
            ],
            await QueryAsync($"select id, rec, xs, m, json_value(rec, '$.n'), isjson(rec) from dbo.[{table}] order by id"));
    }

    [SkippableFact]
    public async Task Merge_upserts_nested_columns_and_an_existing_nvarchar_target_passes_the_drift_check()
    {
        DockerFacts.SkipUnlessDocker();
        const string table = "nested_merge";
        await QueryAsync($"drop table if exists dbo.[{table}]");
        await QueryAsync($"create table dbo.[{table}] (id bigint not null primary key, rec nvarchar(max), xs nvarchar(4000), m nvarchar(max))");

        await WriteAsync(table, "merge", 0);
        await WriteAsync(table, "merge", 1);

        Assert.Equal(
            [
                """0 | {"k":1}""",
                """1 | {"k":2}""",
                "2 | NULL",
            ],
            await QueryAsync($"select id, m from dbo.[{table}] order by id"));
    }
}
