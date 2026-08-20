using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpSourceSchemaTests
{
    private static async Task<Apache.Arrow.Schema> SchemaFor(Dictionary<string, object?> options)
    {
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = "https://api.example.com" }),
            CancellationToken.None);
        return (await source.GetSchemaAsync(new DatasetSpec("s", "d", options), CancellationToken.None)).Schema;
    }

    [Fact]
    public async Task Raw_mode_envelope_without_cursor()
    {
        var schema = await SchemaFor(new() { ["path"] = "/items" });
        Assert.Equal(["payload", "pz_page", "pz_fetched_at"],
            schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.IsType<StringType>(schema.FieldsList[0].DataType);
        Assert.IsType<Int32Type>(schema.FieldsList[1].DataType);
        Assert.IsType<TimestampType>(schema.FieldsList[2].DataType);
    }

    [Fact]
    public async Task Raw_mode_envelope_appends_typed_cursor()
    {
        var schema = await SchemaFor(new()
        {
            ["path"] = "/items", ["cursor"] = "updated_at", ["cursor_type"] = "timestamp",
        });
        Assert.Equal(["payload", "pz_page", "pz_fetched_at", "updated_at"],
            schema.FieldsList.Select(f => f.Name).ToArray());
        Assert.IsType<TimestampType>(schema.FieldsList[3].DataType);
    }

    [Fact]
    public async Task Contract_mode_uses_declared_columns_exactly()
    {
        var schema = await SchemaFor(new()
        {
            ["path"] = "/items",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });
        Assert.Equal(["id", "name"], schema.FieldsList.Select(f => f.Name).ToArray());
    }

    [Fact]
    public async Task Plan_returns_single_partition()
    {
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = "https://api.example.com" }),
            CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d",
            new Dictionary<string, object?> { ["path"] = "/items" }), ReadHints.None, CancellationToken.None);
        Assert.Single(partitions);
    }
}
