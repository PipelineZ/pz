using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer.Tests;

public class SqlServerConnectorTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] pairs) =>
        new(pairs.ToDictionary(p => p.Key, p => p.Value));

    [Fact]
    public void BuildConnectionString_requires_host()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => SqlServerConnector.BuildConnectionString(Config(("database", "db"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("host", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildConnectionString_requires_database()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => SqlServerConnector.BuildConnectionString(Config(("host", "srv"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("database", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildConnectionString_renders_host_comma_port_and_credentials()
    {
        var cs = SqlServerConnector.BuildConnectionString(Config(
            ("host", "srv"), ("port", 14330), ("database", "db"), ("user", "u"), ("password", "p")));
        var b = new SqlConnectionStringBuilder(cs);
        Assert.Equal("srv,14330", b.DataSource);
        Assert.Equal("db", b.InitialCatalog);
        Assert.Equal("u", b.UserID);
        Assert.Equal("p", b.Password);
        Assert.Equal("pz", b.ApplicationName);
    }

    [Fact]
    public void BuildConnectionString_omits_port_for_named_instance()
    {
        var cs = SqlServerConnector.BuildConnectionString(Config(("host", "srv\\inst"), ("database", "db")));
        Assert.Equal("srv\\inst", new SqlConnectionStringBuilder(cs).DataSource);
    }

    [Fact]
    public void BuildConnectionString_passes_authentication_encrypt_trust_through()
    {
        var cs = SqlServerConnector.BuildConnectionString(Config(
            ("host", "srv"), ("database", "db"),
            ("authentication", "Active Directory Default"),
            ("encrypt", false), ("trust_server_certificate", true)));
        var b = new SqlConnectionStringBuilder(cs);
        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryDefault, b.Authentication);
        Assert.Equal(SqlConnectionEncryptOption.Optional, b.Encrypt);
        Assert.True(b.TrustServerCertificate);
    }

    [Fact]
    public void BuildConnectionString_supports_system_assigned_managed_identity()
    {
        var cs = SqlServerConnector.BuildConnectionString(Config(
            ("host", "myserver.database.windows.net"), ("database", "mart"),
            ("authentication", "Active Directory Managed Identity")));

        var parsed = new SqlConnectionStringBuilder(cs);
        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, parsed.Authentication);
        Assert.Equal(string.Empty, parsed.Password);
    }

    [Fact]
    public void BuildConnectionString_supports_user_assigned_managed_identity_via_user_field()
    {
        // For a user-assigned identity, SqlClient takes the identity's client id in User ID.
        var cs = SqlServerConnector.BuildConnectionString(Config(
            ("host", "myserver.database.windows.net"), ("database", "mart"),
            ("authentication", "Active Directory Managed Identity"),
            ("user", "11111111-2222-3333-4444-555555555555")));

        var parsed = new SqlConnectionStringBuilder(cs);
        Assert.Equal(SqlAuthenticationMethod.ActiveDirectoryManagedIdentity, parsed.Authentication);
        Assert.Equal("11111111-2222-3333-4444-555555555555", parsed.UserID);
    }

    [Fact]
    public void BuildConnectionString_rejects_unknown_authentication_with_named_option()
    {
        var ex = Assert.Throws<PzConnectorException>(() => SqlServerConnector.BuildConnectionString(
            Config(("host", "srv"), ("database", "db"), ("authentication", "Bogus Mode"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("authentication", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Quote_doubles_closing_brackets()
    {
        Assert.Equal("[weird]]name]", MsDdl.Quote("weird]name"));
        Assert.Equal("[plain]", MsDdl.Quote("plain"));
    }

    [Fact]
    public void Info_and_capabilities_declare_the_contract()
    {
        var c = new SqlServerConnector();
        Assert.Equal("sqlserver", c.Info.Name);
        Assert.Equal(ProtocolVersion.Major, c.Info.ProtocolMajor);
        var expected = ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown |
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.Merge | ConnectorCapabilities.Transactional |
            ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound |
            ConnectorCapabilities.ApplyDeletes | ConnectorCapabilities.ChangeCapture |
            ConnectorCapabilities.TextLengthStats;
        Assert.Equal(expected, c.Capabilities);
    }
}
