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
    public void DuckDb_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("duckdb", registry.Sources.Keys);
        Assert.Contains("duckdb", registry.Sinks.Keys);
    }

    [Fact]
    public void DuckDb_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.DuckDb", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void DuckLake_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("ducklake", registry.Sources.Keys);
        Assert.Contains("ducklake", registry.Sinks.Keys);
    }

    [Fact]
    public void DuckLake_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.DuckLake", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void Quack_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("quack", registry.Sources.Keys);
        Assert.Contains("quack", registry.Sinks.Keys);
    }

    [Fact]
    public void Quack_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.Quack", BuiltinConnectors.PackageIds);
    }

    [Fact]
    public void MotherDuck_is_registered_as_source_and_sink()
    {
        var registry = BuiltinConnectors.CreateRegistry();
        Assert.Contains("motherduck", registry.Sources.Keys);
        Assert.Contains("motherduck", registry.Sinks.Keys);
    }

    [Fact]
    public void MotherDuck_package_id_is_builtin()
    {
        Assert.Contains("Pz.Connector.MotherDuck", BuiltinConnectors.PackageIds);
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
