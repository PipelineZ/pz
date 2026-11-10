using Pz.Connector.AzureBlob;
using Pz.Connector.DuckDb;
using Pz.Connector.DuckLake;
using Pz.Connector.Gcs;
using Pz.Connector.Http;
using Pz.Connector.Iceberg;
using Pz.Connector.LocalFiles;
using Pz.Connector.MotherDuck;
using Pz.Connector.MySql;
using Pz.Connector.S3;
using Pz.Connector.Postgres;
using Pz.Connector.Quack;
using Pz.Connector.Sftp;
using Pz.Connector.Sqlite;
using Pz.Connector.SqlServer;
using Pz.Connectors.Abstractions;
using Pz.Engine.Execution;

namespace Pz.Cli;

/// <summary>
/// Builtin connector registry: the CLI wires the first-party connectors directly, project-referencing
/// each <c>connectors/Pz.Connector.*</c> project (see <see cref="PackageIds"/>), rather than restoring
/// them as NuGet packages.
/// <see cref="ConnectorRegistry"/> itself stays host-agnostic (name → instance), so
/// <see cref="ConnectorRegistryFactory"/> only adds to, never replaces, this construction site:
/// builtins are always registered; non-builtin connectors declared in project.yml additionally come
/// from a <c>ProcessConnectorHost</c> over restored <c>.pz/packages</c>.
///
/// Note on <c>base_dir</c>: <see cref="LocalFilesConnector"/> keeps itself pure — it never assumes a
/// project layout, resolving relative dataset/output <c>path</c>s only against a connection option
/// named <c>base_dir</c>. This registry does not (and should not) inject that option; see
/// <see cref="Pz.Core.Loading.ProjectDirectoryAnchor"/>, which every verb applies before compiling,
/// since only the host knows the project directory.
/// </summary>
internal static class BuiltinConnectors
{
    /// <summary>Package ids satisfied in-process by <see cref="CreateRegistry"/>: a connector requirement
    /// naming one of these never needs <c>pz restore</c> and is excluded from NuGet resolution, the lock
    /// file, and drift checking (<see cref="ConnectorRegistryFactory"/> is the only consumer that needs
    /// to tell "builtin" from "must come from .pz/packages" apart).</summary>
    public static readonly IReadOnlySet<string> PackageIds = new HashSet<string>(StringComparer.Ordinal)
    {
        "Pz.Connector.LocalFiles", "Pz.Connector.S3", "Pz.Connector.Postgres", "Pz.Connector.SqlServer",
        "Pz.Connector.AzureBlob", "Pz.Connector.Http", "Pz.Connector.MySql", "Pz.Connector.Sqlite",
        "Pz.Connector.Sftp", "Pz.Connector.Gcs", "Pz.Connector.DuckDb", "Pz.Connector.DuckLake",
        "Pz.Connector.Quack", "Pz.Connector.MotherDuck", "Pz.Connector.Iceberg",
    };

    public static ConnectorRegistry CreateRegistry()
    {
        var registry = new ConnectorRegistry();
        AddBoth(registry, "localfiles", new LocalFilesConnector());
        AddBoth(registry, "postgres", new PostgresConnector());
        AddBoth(registry, "s3", new S3Connector());
        AddBoth(registry, "sqlserver", new SqlServerConnector());
        AddBoth(registry, "azureblob", new AzureConnector());
        AddBoth(registry, "gcs", new GcsConnector());
        AddBoth(registry, "http", new HttpConnector());
        AddBoth(registry, "mysql", new MySqlConnector());
        AddBoth(registry, "sqlite", new SqliteConnector());
        AddBoth(registry, "duckdb", new DuckDbConnector());
        AddBoth(registry, "ducklake", new DuckLakeConnector());
        AddBoth(registry, "quack", new QuackConnector());
        AddBoth(registry, "motherduck", new MotherDuckConnector());
        AddBoth(registry, "iceberg", new IcebergConnector());
        AddBoth(registry, "sftp", new SftpConnector());
        return registry;
    }

    private static void AddBoth<T>(ConnectorRegistry registry, string name, T connector)
        where T : ISourceConnector, ISinkConnector
    {
        registry.AddSource(name, connector);
        registry.AddSink(name, connector);
    }
}
