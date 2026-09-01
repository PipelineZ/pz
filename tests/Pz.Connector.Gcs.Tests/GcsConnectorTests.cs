using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Connector-level contract: identity/capabilities, offline validation delegating to the
/// auth matrix, and the auth→direction gate — a source is native-only so it exists only under hmac
/// (the refusal names the fix), while a sink opens under every method.</summary>
public sealed class GcsConnectorTests
{
    private static ConnectorConfig Hmac() => new(new Dictionary<string, object?>
    {
        ["auth"] = "hmac",
        ["key_id"] = "k",
        ["secret"] = "s",
    });

    private static ConnectorConfig Adc() => new(new Dictionary<string, object?> { ["auth"] = "adc" });

    [Fact]
    public void Info_and_capabilities_cover_both_tiers()
    {
        var connector = new GcsConnector();
        Assert.Equal("gcs", connector.Info.Name);
        var caps = connector.Capabilities;
        Assert.True(caps.HasFlag(ConnectorCapabilities.NativeScan));
        Assert.True(caps.HasFlag(ConnectorCapabilities.NativeCopy));
        Assert.True(caps.HasFlag(ConnectorCapabilities.ReplaceWrites));
        Assert.True(caps.HasFlag(ConnectorCapabilities.BoundedWindow));
        Assert.True(caps.HasFlag(ConnectorCapabilities.PathTemplating));
        Assert.True(caps.HasFlag(ConnectorCapabilities.GatedOperations));
    }

    [Fact]
    public async Task Validate_aggregates_the_auth_matrix_and_url_style()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["auth"] = "hmac",
            ["url_style"] = "wrong",
        });
        var result = await new GcsConnector().ValidateAsync(config, CancellationToken.None);
        Assert.Equal(3, result.Errors.Count);
        Assert.Contains(result.Errors, e => e.Contains("'key_id'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'secret'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("url_style", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Valid_hmac_and_valid_adc_both_pass_validation()
    {
        var connector = new GcsConnector();
        Assert.True((await connector.ValidateAsync(Hmac(), CancellationToken.None)).IsValid);
        Assert.True((await connector.ValidateAsync(Adc(), CancellationToken.None)).IsValid);
    }

    [Fact]
    public async Task Hmac_source_opens()
    {
        await using var source = await ((ISourceConnector)new GcsConnector()).OpenAsync(Hmac(), CancellationToken.None);
        Assert.NotNull(source);
    }

    [Fact]
    public async Task Non_hmac_source_is_refused_naming_the_fix()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await ((ISourceConnector)new GcsConnector()).OpenAsync(Adc(), CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("auth: hmac", ex.Message, StringComparison.Ordinal);
        Assert.Contains("HMAC", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Sink_opens_under_hmac_and_adc()
    {
        var connector = (ISinkConnector)new GcsConnector();
        await using var hmacSink = await connector.OpenAsync(Hmac(), CancellationToken.None);
        await using var adcSink = await connector.OpenAsync(Adc(), CancellationToken.None);
        Assert.NotNull(hmacSink);
        Assert.NotNull(adcSink);
    }

    [Fact]
    public async Task Hmac_connectivity_check_defers_to_run_time()
    {
        var check = await new GcsConnector().CheckConnectionAsync(Hmac(), CancellationToken.None);
        Assert.True(check.Ok);
        Assert.Contains("run time", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Connection_schema_is_strict_and_dataset_schema_mirrors_the_file_connectors()
    {
        var connector = new GcsConnector();
        Assert.Contains("\"additionalProperties\": false", connector.ConnectionConfigSchema, StringComparison.Ordinal);
        Assert.Contains("\"auth\"", connector.ConnectionConfigSchema, StringComparison.Ordinal);
        Assert.Contains("files_per_partition", connector.DatasetConfigSchema, StringComparison.Ordinal);
        Assert.Contains("\"columns\"", connector.DatasetConfigSchema, StringComparison.Ordinal);
    }
}
