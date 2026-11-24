using System.Text.Json.Nodes;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

/// <summary>A nested contract type through <see cref="ContractProjector"/> (http and json file reads): the schema
/// carries the nested Arrow type and a record's subtree becomes the typed value; a subtree of the wrong shape
/// fails naming the column, not the value.</summary>
public sealed class ContractProjectorNestedTests
{
    private static readonly Dictionary<string, string> Columns = new()
    {
        ["id"] = "bigint",
        ["user"] = "struct(name varchar, roles varchar[])",
        ["scores"] = "map(varchar, double)",
    };

    [Fact]
    public void A_record_subtree_becomes_a_typed_nested_value()
    {
        var schema = ContractProjector.BuildSchema(Columns);
        var record = JsonNode.Parse("""{"id": 7, "user": {"name": "ann", "roles": ["admin", null]}, "scores": {"q1": 1.5}}""");

        var row = ContractProjector.ProjectRow(record, Columns, "test");

        Assert.IsType<StructType>(schema.GetFieldByName("user").DataType);
        Assert.Equal(7L, row[0]);
        var user = Assert.IsType<Dictionary<string, object?>>(row[1]);
        Assert.Equal("ann", user["name"]);
        Assert.Equal(["admin", null], Assert.IsType<List<object?>>(user["roles"]));
        Assert.Equal(1.5, Assert.IsType<Dictionary<object, object?>>(row[2])["q1"]);
    }

    [Fact]
    public void A_subtree_of_the_wrong_shape_names_the_column()
    {
        var record = JsonNode.Parse("""{"id": 7, "user": ["not", "an", "object"]}""");

        var ex = Assert.Throws<PzConnectorException>(() => ContractProjector.ProjectRow(record, Columns, "test"));

        Assert.Contains("column 'user'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("not", ex.Message.Replace("not a valid", "", StringComparison.Ordinal), StringComparison.Ordinal);
    }
}
