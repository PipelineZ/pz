using Pz.Connectors.Abstractions;
using Snowflake.Data.Client;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeConnectorTests
{
    private static ConnectorConfig Config(params (string, object?)[] kv) =>
        new(kv.ToDictionary(x => x.Item1, x => x.Item2));

    private static ConnectorConfig Valid() => Config(
        ("account", "myorg-myacct"), ("user", "PZ_SVC"), ("private_key_path", "/keys/pz.p8"),
        ("database", "ANALYTICS"), ("warehouse", "PZ_WH"));

    /// <summary>Parses the produced connection string back through the same
    /// <see cref="SnowflakeDbConnectionStringBuilder"/> the connector builds it with, rather than
    /// raw <c>Contains</c> -- the builder quotes a value containing <c>;</c>/<c>"</c>, so a
    /// substring check on the raw string would be brittle (or wrong) exactly where quoting matters.</summary>
    private static SnowflakeDbConnectionStringBuilder Parse(string connectionString) =>
        new() { ConnectionString = connectionString };

    [Fact]
    public void Connection_string_uses_jwt_authenticator()
    {
        var cs = SnowflakeConnector.BuildConnectionString(Valid());
        var parsed = Parse(cs);
        Assert.Equal("snowflake_jwt", parsed["authenticator"]);
        Assert.Equal("myorg-myacct", parsed["account"]);
        Assert.Equal("PZ_SVC", parsed["user"]);
        Assert.Equal("/keys/pz.p8", parsed["private_key_file"]);
        Assert.Equal("ANALYTICS", parsed["db"]);
        Assert.Equal("PZ_WH", parsed["warehouse"]);
    }

    [Fact]
    public void Optional_role_and_passphrase_flow_through()
    {
        var config = Config(
            ("account", "a"), ("user", "u"), ("private_key_path", "/k.p8"),
            ("database", "d"), ("warehouse", "w"), ("role", "LOADER"), ("private_key_passphrase", "pw"));
        var parsed = Parse(SnowflakeConnector.BuildConnectionString(config));
        Assert.Equal("LOADER", parsed["role"]);
        Assert.Equal("pw", parsed["private_key_pwd"]);
    }

    [Fact]
    public void Passphrase_containing_a_semicolon_round_trips_intact()
    {
        // The hand-rolled `key=value;` concatenation this replaced would truncate at the `;` and
        // spawn a spurious property; SnowflakeDbConnectionStringBuilder quotes the value instead.
        var config = Config(
            ("account", "a"), ("user", "u"), ("private_key_path", "/k.p8"),
            ("database", "d"), ("warehouse", "w"), ("private_key_passphrase", "sup;er;secret"));
        var parsed = Parse(SnowflakeConnector.BuildConnectionString(config));
        Assert.Equal("sup;er;secret", parsed["private_key_pwd"]);
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
