using System.Text.Json.Nodes;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

public class ContractProjectorTests
{
    private static readonly IReadOnlyDictionary<string, string> Columns = new Dictionary<string, string>
    {
        ["id"] = "bigint", ["title"] = "varchar", ["done"] = "boolean", ["updated_at"] = "timestamp",
    };

    [Fact]
    public void Schema_fields_follow_declaration_order_and_types()
    {
        var schema = ContractProjector.BuildSchema(Columns);
        Assert.Equal(["id", "title", "done", "updated_at"], schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.IsType<Int64Type>(schema.FieldsList[0].DataType);
        Assert.IsType<TimestampType>(schema.FieldsList[3].DataType);
        Assert.All(schema.FieldsList, f => Assert.True(f.IsNullable));
    }

    [Fact]
    public void Projects_values_extra_keys_ignored_missing_null()
    {
        var record = JsonNode.Parse(
            """{ "id": 7, "title": "x", "extra": true, "updated_at": "2026-07-01T10:00:00Z" }""");
        var row = ContractProjector.ProjectRow(record, Columns, "dataset 'issues'");
        Assert.Equal(7L, row[0]);
        Assert.Equal("x", row[1]);
        Assert.Null(row[2]); // missing "done" -> null
        Assert.Equal(new DateTimeOffset(2026, 7, 1, 10, 0, 0, TimeSpan.Zero), row[3]);
    }

    [Fact]
    public void Type_mismatch_is_permanent_and_never_echoes_the_value()
    {
        var record = JsonNode.Parse("""{ "id": "SECRET-not-a-number" }""");
        var ex = Assert.Throws<PzConnectorException>(
            () => ContractProjector.ProjectRow(record, Columns, "dataset 'issues'"));
        Assert.False(ex.IsTransient);
        Assert.Contains("id", ex.Message);
        Assert.Contains("dataset 'issues'", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
    }

    [Fact]
    public void Non_object_record_projects_all_nulls()
    {
        var row = ContractProjector.ProjectRow(JsonNode.Parse("[1,2]"), Columns, "ctx");
        Assert.All(row, Assert.Null);
    }

    [Fact]
    public void BuildSchema_with_unknown_type_throws_with_column_name()
    {
        var columns = new Dictionary<string, string> { ["bad_column"] = "unknown_type" };
        var ex = Assert.Throws<PzConnectorException>(() => ContractProjector.BuildSchema(columns));
        Assert.Contains("bad_column", ex.Message);
        Assert.Contains("unknown_type", ex.Message);
    }

    [Fact]
    public void BuildSchema_decimal_type_has_correct_precision_and_scale()
    {
        var columns = new Dictionary<string, string> { ["amount"] = "decimal" };
        var schema = ContractProjector.BuildSchema(columns);
        var decimalType = (Decimal128Type)schema.FieldsList[0].DataType;
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(9, decimalType.Scale);
    }

    [Fact]
    public void BuildSchema_timestamp_type_has_correct_timezone_and_unit()
    {
        var columns = new Dictionary<string, string> { ["created_at"] = "timestamp" };
        var schema = ContractProjector.BuildSchema(columns);
        var timestampType = (TimestampType)schema.FieldsList[0].DataType;
        Assert.Equal("+00:00", timestampType.Timezone);
        Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
    }

    [Fact]
    public void Projects_decimal_and_date_types()
    {
        var columns = new Dictionary<string, string>
        {
            ["price"] = "decimal",
            ["purchase_date"] = "date",
        };
        var record = JsonNode.Parse("""{ "price": 12.34, "purchase_date": "2026-07-01" }""");
        var row = ContractProjector.ProjectRow(record, columns, "transactions");
        Assert.Equal(12.34m, row[0]);
        Assert.Equal(new DateOnly(2026, 7, 1), row[1]);
    }
}
