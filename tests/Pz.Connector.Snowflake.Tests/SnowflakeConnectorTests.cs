using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeConnectorTests
{
    private static ConnectorConfig Config(params (string, object?)[] kv) =>
        new(kv.ToDictionary(x => x.Item1, x => x.Item2));

    private static ConnectorConfig Valid() => Config(
        ("account", "myorg-myacct"), ("user", "PZ_SVC"), ("private_key_path", "/keys/pz.p8"),
        ("database", "ANALYTICS"), ("warehouse", "PZ_WH"));

    [Fact]
    public void Connection_string_uses_jwt_authenticator()
    {
        var cs = SnowflakeConnector.BuildConnectionString(Valid());
        Assert.Contains("authenticator=snowflake_jwt", cs);
        Assert.Contains("account=myorg-myacct", cs);
        Assert.Contains("user=PZ_SVC", cs);
        Assert.Contains("private_key_file=/keys/pz.p8", cs);
        Assert.Contains("db=ANALYTICS", cs);
        Assert.Contains("warehouse=PZ_WH", cs);
    }

    [Fact]
    public void Optional_role_and_passphrase_flow_through()
    {
        var config = Config(
            ("account", "a"), ("user", "u"), ("private_key_path", "/k.p8"),
            ("database", "d"), ("warehouse", "w"), ("role", "LOADER"), ("private_key_passphrase", "pw"));
        var cs = SnowflakeConnector.BuildConnectionString(config);
        Assert.Contains("role=LOADER", cs);
        Assert.Contains("private_key_pwd=pw", cs);
    }

    [Theory]
    [InlineData("account")]
    [InlineData("user")]
    [InlineData("private_key_path")]
    [InlineData("database")]
    [InlineData("warehouse")]
    public void Missing_required_field_throws_nontransient_naming_it(string missing)
    {
        var values = Valid().Values.Where(kv => kv.Key != missing).ToDictionary(kv => kv.Key, kv => kv.Value);
        var ex = Assert.Throws<PzConnectorException>(
            () => SnowflakeConnector.BuildConnectionString(new ConnectorConfig(values)));
        Assert.False(ex.IsTransient);
        Assert.Contains(missing, ex.Message);
    }

    [Fact]
    public void Info_and_capabilities_match_the_spec()
    {
        var c = new SnowflakeConnector();
        Assert.Equal("snowflake", c.Info.Name);
        var caps = c.Capabilities;
        Assert.True(caps.HasFlag(ConnectorCapabilities.Merge));
        Assert.True(caps.HasFlag(ConnectorCapabilities.Transactional));
        Assert.True(caps.HasFlag(ConnectorCapabilities.ReplaceWrites));
        Assert.True(caps.HasFlag(ConnectorCapabilities.BoundedWindow));
        Assert.True(caps.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound));
        Assert.False(caps.HasFlag(ConnectorCapabilities.NativeScan));
        Assert.False(caps.HasFlag(ConnectorCapabilities.ApplyDeletes));
    }
}
