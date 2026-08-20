using System.Text;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MySql.Tests;

public sealed class MySqlConnectorTests
{
    private static ConnectorConfig Config() => new(new Dictionary<string, object?>
    {
        ["host"] = "db.example.com",
        ["database"] = "analytics",
    });

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new MySqlConnector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }

    [Fact]
    public void Connector_is_native_only_in_both_directions()
    {
        var c = new MySqlConnector();
        Assert.IsAssignableFrom<INativeOnlySource>(c);
        Assert.IsAssignableFrom<INativeOnlySink>(c);
        Assert.Equal(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
            ConnectorCapabilities.ReplaceWrites |
            ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound,
            c.Capabilities);
        Assert.Equal("mysql", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
    }

    [Fact]
    public async Task PlanReadAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.PlanReadAsync(new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task BeginWriteAsync_refuses_the_universal_tier_with_PZ0312()
    {
        await using var sink = await ((ISinkConnector)new MySqlConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new OutputSpec("wh", "orders_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(spec, new Apache.Arrow.Schema([], null), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaAsync_returns_the_declared_contract_as_the_schema()
    {
        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new DatasetSpec("wh", "orders", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["placed_on"] = "date" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        Assert.Collection(schema.Schema.FieldsList,
            f => { Assert.Equal("id", f.Name); Assert.Equal(ArrowTypeId.Int64, f.DataType.TypeId); },
            f => { Assert.Equal("placed_on", f.Name); Assert.Equal(ArrowTypeId.Date32, f.DataType.TypeId); });
    }

    [Fact]
    public async Task GetSchemaAsync_without_a_contract_is_a_clear_permanent_refusal()
    {
        await using var source = await ((ISourceConnector)new MySqlConnector()).OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await source.GetSchemaAsync(new DatasetSpec("wh", "orders", new Dictionary<string, object?>()), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("columns:", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_no_longer_restricts_values_now_that_they_ride_a_secret()
    {
        // Every credential rides CREATE SECRET as an ordinary, ''-escaped SQL string literal
        // (MySqlSqlGenTests proves the escaping) rather than a key=value attach string that cannot
        // carry a space or a quote, so ValidateAsync has nothing left to refuse -- a password
        // containing a space, a quote, AND an '=' passes.
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "h",
            ["database"] = "d",
            ["password"] = "has space='quote'",
        });

        var result = await new MySqlConnector().ValidateAsync(config, CancellationToken.None);

        Assert.True(result.IsValid);
    }

    [Fact]
    public void Greeting_parses_a_v10_handshake()
    {
        var payload = new byte[] { 0x0a }
            .Concat(Encoding.ASCII.GetBytes("8.4.5")).Append((byte)0)
            .Concat(new byte[] { 1, 2, 3, 4 }).ToArray();
        var packet = new byte[] { (byte)payload.Length, 0, 0, 0 }.Concat(payload).ToArray();

        Assert.True(MySqlGreeting.TryParse(packet, out var version, out var error));
        Assert.Equal("8.4.5", version);
        Assert.Null(error);
    }

    [Fact]
    public void Greeting_parses_a_pre_auth_error_packet()
    {
        var message = Encoding.ASCII.GetBytes("Host 'x' is not allowed to connect");
        var payload = new byte[] { 0xff, 0x6a, 0x04 }.Concat(message).ToArray();
        var packet = new byte[] { (byte)payload.Length, 0, 0, 0 }.Concat(payload).ToArray();

        Assert.True(MySqlGreeting.TryParse(packet, out var version, out var error));
        Assert.Null(version);
        Assert.Equal("Host 'x' is not allowed to connect", error);
    }

    [Fact]
    public void Greeting_rejects_non_mysql_bytes()
    {
        Assert.False(MySqlGreeting.TryParse("HTTP/1.1 400 Bad Request"u8, out _, out _));
        Assert.False(MySqlGreeting.TryParse([], out _, out _));
    }
}
