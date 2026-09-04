using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.AzureBlob.Tests;

public sealed class AzureConnectorTests
{
    [Fact]
    public void Info_reports_azure_and_current_protocol()
    {
        var c = new AzureConnector();
        Assert.Equal("azureblob", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new AzureConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    [Fact]
    public async Task Validate_missing_auth_discriminator_fails()
    {
        var c = new AzureConnector();
        var result = await c.ValidateAsync(new ConnectorConfig(new Dictionary<string, object?>()), CancellationToken.None);
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("auth", StringComparison.Ordinal));
    }

    [Fact]
    public void Dataset_schema_embeds_the_catalog_format_properties()
    {
        var c = new AzureConnector();
        Assert.Contains(FileFormatCatalog.SchemaProperties, c.DatasetConfigSchema, StringComparison.Ordinal);
    }
}
