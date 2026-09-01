using Pz.Engine.Execution;

namespace Pz.Cli.Tests;

public class BuiltinConnectorsTests
{
    [Fact]
    public void Http_is_registered_as_source()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("http", registry.Sources.Keys);
    }

    [Fact]
    public void Http_is_registered_as_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("http", registry.Sinks.Keys);
    }

    [Fact]
    public void Http_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.Http", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void MySql_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("mysql", registry.Sources.Keys);
        Assert.Contains("mysql", registry.Sinks.Keys);
    }

    [Fact]
    public void MySql_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.MySql", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void Sqlite_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("sqlite", registry.Sources.Keys);
        Assert.Contains("sqlite", registry.Sinks.Keys);
    }

    [Fact]
    public void Sqlite_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.Sqlite", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void S3_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("s3", registry.Sources.Keys);
        Assert.Contains("s3", registry.Sinks.Keys);
    }

    [Fact]
    public void Gcs_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("gcs", registry.Sources.Keys);
        Assert.Contains("gcs", registry.Sinks.Keys);
    }

    [Fact]
    public void Gcs_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.Gcs", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void Sftp_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("sftp", registry.Sources.Keys);
        Assert.Contains("sftp", registry.Sinks.Keys);
    }

    [Fact]
    public void Sftp_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.Sftp", BuiltinConnectors.PackageIds);
    }
}
