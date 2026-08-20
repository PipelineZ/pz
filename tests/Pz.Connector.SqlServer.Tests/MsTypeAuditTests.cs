using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class MsTypeAuditTests(MsSqlContainerFixture fixture)
{
    [SkippableFact]
    public async Task Type_audit_coverage_round_trips_with_declared_arrow_types()
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

        var spec = new DatasetSpec("ms", "audit", new Dictionary<string, object?>
        {
            ["query"] = "select * from dbo.type_audit order by id",
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

        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero), Get("c_datetime", 0));
        Assert.Equal(new DateTimeOffset(2026, 3, 27, 10, 30, 0, TimeSpan.Zero), Get("c_smalldatetime", 0));
        Assert.Equal(12345.67m, (decimal)Get("c_numeric", 0)!);
        Assert.Equal(99.99m, (decimal)Get("c_smallmoney", 0)!);
        Assert.Equal("ab   ", Get("c_nchar", 0)); // nchar(5) pads
        Assert.Equal("legacy-text", Get("c_text", 0));
        Assert.Equal("legacy-ntext", Get("c_ntext", 0));
        var big = (string)Get("c_nvarchar_max", 0)!;
        Assert.Equal(10000, big.Length);
        Assert.All(big, ch => Assert.Equal('x', ch));
        Assert.Equal("café 漢字 🚀", Get("c_unicode", 0));
        Assert.Equal("CaseSensitive", Get("c_cs", 0));
        Assert.Equal(12345678901234567890.123456789m, (decimal)Get("c_big_decimal", 0)!);
        Assert.Equal(new DateOnly(1, 1, 1), Get("c_date_min", 0));
        Assert.Equal(new DateOnly(9999, 12, 31), Get("c_date_max", 0));
        Assert.Equal(
            new DateTimeOffset(9999, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(9999990),
            Get("c_dt2_max", 0));

        foreach (var field in batch.Schema.FieldsList.Where(f => f.Name != "id"))
        {
            Assert.Null(Get(field.Name, 1));
        }
    }
}
