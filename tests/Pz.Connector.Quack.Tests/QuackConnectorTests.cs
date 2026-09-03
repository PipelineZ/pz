using System.Net;
using System.Net.Sockets;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Quack.Tests;

public sealed class QuackConnectorTests
{
    private static ConnectorConfig Config(string uri = "quack:lake.internal:9494", string token = "abcd") =>
        new(new Dictionary<string, object?> { ["uri"] = uri, ["token"] = token });

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new QuackConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            Assert.NotNull(Json.Schema.JsonSchema.FromText(s));
        }
    }

    [Fact]
    public void Connector_is_native_only_without_transactional()
    {
        var c = new QuackConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("quack", c.Info.Name);
    }

    [Fact]
    public async Task Validate_requires_a_quack_uri_and_a_token_of_at_least_four_characters()
    {
        var bad = await new QuackConnector().ValidateAsync(Config("http://x", "abc"), CancellationToken.None);
        Assert.Equal(2, bad.Errors.Count);
        Assert.Contains(bad.Errors, e => e.Contains("quack:host", StringComparison.Ordinal));
        Assert.Contains(bad.Errors, e => e.Contains("four characters", StringComparison.Ordinal));

        Assert.Empty((await new QuackConnector().ValidateAsync(Config(), CancellationToken.None)).Errors);
    }

    [Fact]
    public async Task Check_probes_the_server_over_tcp()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var ok = await new QuackConnector().CheckConnectionAsync(Config($"quack:127.0.0.1:{port}"), CancellationToken.None);
        Assert.True(ok.Ok);
        Assert.Contains("credentials are verified at run time", ok.Message, StringComparison.Ordinal);

        listener.Stop();
        var down = await new QuackConnector().CheckConnectionAsync(Config($"quack:127.0.0.1:{port}"), CancellationToken.None);
        Assert.False(down.Ok);
        Assert.StartsWith("transient:", down.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Check_probes_an_ipv6_literal_without_its_brackets()
    {
        Skip.IfNot(Socket.OSSupportsIPv6, "no IPv6 loopback on this host");
        using var listener = new TcpListener(IPAddress.IPv6Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var ok = await new QuackConnector().CheckConnectionAsync(Config($"quack:[::1]:{port}"), CancellationToken.None);
        Assert.True(ok.Ok);
        Assert.Contains("[::1]", ok.Message);
        listener.Stop();
    }

    [Fact]
    public async Task Check_is_permanent_on_a_malformed_uri()
    {
        var check = await new QuackConnector().CheckConnectionAsync(Config("nope"), CancellationToken.None);
        Assert.False(check.Ok);
        Assert.StartsWith("permanent:", check.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Universal_tier_is_refused_with_PZ0312_and_schema_needs_a_contract()
    {
        await using var source = await ((ISourceConnector)new QuackConnector()).OpenAsync(Config(), CancellationToken.None);
        var readEx = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));
        Assert.StartsWith("PZ0312", readEx.Message, StringComparison.Ordinal);

        await using var sink = await ((ISinkConnector)new QuackConnector()).OpenAsync(Config(), CancellationToken.None);
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
    public async Task Scan_and_copy_share_one_attach_alias()
    {
        await using var source = await ((ISourceConnector)new QuackConnector()).OpenAsync(Config(), CancellationToken.None);
        await using var sink = await ((ISinkConnector)new QuackConnector()).OpenAsync(Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(new DatasetSpec("wh", "events", new Dictionary<string, object?>()), out var scan));
        Assert.True(sink.TryGetNativeCopy(new OutputSpec("wh", "o", "append", "fail_on_change", new Dictionary<string, object?>()), out var copy));
        Assert.Equal(scan!.SetupStatements, copy!.SetupStatements);
        Assert.Equal("quack attach", scan.Mechanism);
        Assert.Equal("quack insert", copy.Mechanism);
    }
}
