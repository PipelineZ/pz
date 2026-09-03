using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MotherDuck.Tests;

public sealed class MotherDuckConnectorTests
{
    private static ConnectorConfig Config() =>
        new(new Dictionary<string, object?> { ["database"] = "lake", ["token"] = "tok" });

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new MotherDuckConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            Assert.NotNull(Json.Schema.JsonSchema.FromText(s));
        }
    }

    [Fact]
    public void Connector_is_native_only_without_transactional()
    {
        var c = new MotherDuckConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("motherduck", c.Info.Name);
    }

    [Fact]
    public async Task Validate_has_no_cross_field_rules()
    {
        Assert.Empty((await new MotherDuckConnector().ValidateAsync(Config(), CancellationToken.None)).Errors);
    }

    [Fact]
    public async Task Check_has_no_offline_probe()
    {
        var check = await new MotherDuckConnector().CheckConnectionAsync(Config(), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.StartsWith("not checked", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Universal_tier_is_refused_with_PZ0312_and_schema_needs_a_contract()
    {
        await using var source = await ((ISourceConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        var readEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));
        Assert.StartsWith("PZ0312", readEx.Message, StringComparison.Ordinal);

        await using var sink = await ((ISinkConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        var writeEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(new OutputSpec("wh", "o", "append", "fail_on_change", new Dictionary<string, object?>()), new Apache.Arrow.Schema([], null), CancellationToken.None));
        Assert.StartsWith("PZ0312", writeEx.Message, StringComparison.Ordinal);

        var schema = await source.GetSchemaAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        }), CancellationToken.None);
        Assert.Equal(ArrowTypeId.Int64, Assert.Single(schema.Schema.FieldsList).DataType.TypeId);
        await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), CancellationToken.None));
    }

    [Fact]
    public async Task Scan_and_copy_reference_the_database_by_name_and_share_setup()
    {
        await using var source = await ((ISourceConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        await using var sink = await ((ISinkConnector)new MotherDuckConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out var scan));
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "o", "append", "fail_on_change", new Dictionary<string, object?>()), out var copy));
        Assert.Equal(scan!.SetupStatements, copy!.SetupStatements);
        Assert.Equal("\"lake\".\"events\"", scan.SqlFragment);
        Assert.Equal("motherduck attach", scan.Mechanism);
        Assert.Equal("motherduck insert", copy.Mechanism);
    }
}
