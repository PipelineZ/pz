using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Connector-level contract: the published schemas are well-formed JSON Schema, and the
/// dataset schema carries the shared catalog's <c>format</c> enum -- sftp has no native tier, but its
/// `format:` option is resolved through the same <see cref="FileFormatCatalog"/> as the file-place
/// connectors that do.</summary>
public sealed class SftpConnectorTests
{
    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new SftpConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    [Fact]
    public void Dataset_schema_embeds_the_catalog_format_properties()
    {
        var c = new SftpConnector();
        Assert.Contains(FileFormatCatalog.SchemaProperties, c.DatasetConfigSchema, StringComparison.Ordinal);
    }
}
