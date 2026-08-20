using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

public sealed class AzureUrlTests
{
    private static Dictionary<string, object?> Opts(string? container = "c", string? path = "p/data.parquet",
        string? scheme = null)
    {
        var d = new Dictionary<string, object?>();
        if (container is not null) d["container"] = container;
        if (path is not null) d["path"] = path;
        if (scheme is not null) d["scheme"] = scheme;
        return d;
    }

    [Fact]
    public void Parse_defaults_scheme_to_az_and_renders_url()
    {
        var loc = AzureUrl.ParseDataset(Opts(), "dataset 'orders'");
        Assert.Equal("az", loc.Scheme);
        Assert.Equal("az://c/p/data.parquet", AzureUrl.Render(loc));
    }

    [Fact]
    public void Parse_accepts_abfss_scheme()
    {
        var loc = AzureUrl.ParseDataset(Opts(scheme: "abfss"), "dataset 'orders'");
        Assert.Equal("abfss://c/p/data.parquet", AzureUrl.Render(loc));
    }

    [Fact]
    public void Parse_rejects_unknown_scheme_naming_subject()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureUrl.ParseDataset(Opts(scheme: "s3"), "dataset 'orders'"));
        Assert.False(ex.IsTransient);
        Assert.Contains("dataset 'orders'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("s3", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_missing_container_is_named_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureUrl.ParseDataset(Opts(container: null), "dataset 'orders'"));
        Assert.Contains("container", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Escape_doubles_single_quotes()
    {
        Assert.Equal("a''b", AzureUrl.Escape("a'b"));
    }

    [Fact]
    public void Render_sink_prefix_joins_key()
    {
        var loc = AzureUrl.ParseSink(Opts(path: "raw/orders"), "output 'out'", objectName: "out.parquet");
        Assert.Equal("az://c/raw/orders/out.parquet", AzureUrl.Render(loc));
    }
}
