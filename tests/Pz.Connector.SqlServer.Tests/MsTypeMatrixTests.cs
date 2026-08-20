using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class MsTypeMatrixTests(MsSqlContainerFixture fixture)
{
    [SkippableFact]
    public async Task Canonical_and_widened_types_round_trip_with_declared_arrow_types()
    {
        DockerFacts.SkipUnlessDocker();
        var connector = new SqlServerConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
                ["user"] = fixture.User, ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            }), CancellationToken.None);

        var spec = new DatasetSpec("ms", "matrix", new Dictionary<string, object?>
        {
            ["query"] = "select * from dbo.matrix order by id",
        });
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var batches = new List<RecordBatch>();
        await foreach (var b in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal(2, batch.Length);
        // Probe schema (SchemaOnly, no execution) must equal the batch schema exactly.
        Assert.True(schema.FieldsList.Select(f => (f.Name, f.DataType.TypeId))
            .SequenceEqual(batch.Schema.FieldsList.Select(f => (f.Name, f.DataType.TypeId))));

        object? Get(string column, int row)
        {
            var i = batch.Schema.GetFieldIndex(column);
            var a = batch.Column(i);
            return a.IsNull(row) ? null : a switch
            {
                Int32Array x => x.GetValue(row),
                Int64Array x => x.GetValue(row),
                DoubleArray x => x.GetValue(row),
                Decimal128Array x => x.GetValue(row),
                StringArray x => x.GetString(row),
                BooleanArray x => x.GetValue(row),
                Date32Array x => x.GetDateOnly(row),
                TimestampArray x => x.GetTimestamp(row),
                _ => throw new InvalidOperationException(a.GetType().Name),
            };
        }

        Assert.Equal(42, Get("c_int", 0));
        Assert.Equal(7, Get("c_tinyint", 0));
        Assert.Equal(-3, Get("c_smallint", 0));
        Assert.Equal(9_000_000_000L, Get("c_bigint", 0));
        Assert.Equal(1.5d, Get("c_float", 0));
        Assert.Equal(2.5d, Get("c_real", 0));
        Assert.Equal(12345.123456789m, Get("c_decimal", 0));
        Assert.Equal(99.99m, (decimal)Get("c_money", 0)!);
        Assert.Equal("hello", Get("c_nvarchar", 0));
        Assert.Equal("world", Get("c_varchar", 0));
        Assert.Equal("abc  ", Get("c_char", 0)); // char(5) pads
        Assert.Equal("11111111-2222-3333-4444-555555555555", Get("c_guid", 0));
        Assert.Equal(true, Get("c_bit", 0));
        Assert.Equal(new DateOnly(2026, 3, 27), Get("c_date", 0));
        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero), Get("c_datetime2", 0));
        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero), Get("c_dto", 0)); // 12:30+02 -> 10:30Z

        foreach (var field in batch.Schema.FieldsList.Where(f => f.Name != "id"))
        {
            Assert.Null(Get(field.Name, 1));
        }
    }
}
