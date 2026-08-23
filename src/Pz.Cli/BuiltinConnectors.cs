using Pz.Connector.AzureBlob;
using Pz.Connector.Http;
using Pz.Connector.LocalFiles;
using Pz.Connector.MySql;
using Pz.Connector.S3;
using Pz.Connector.Postgres;
using Pz.Connector.Sqlite;
using Pz.Connector.SqlServer;
using Pz.Engine.Execution;

namespace Pz.Cli;

/// <summary>
/// Builtin connector registry: the CLI wires the connectors v0 needs directly, project-
/// referencing <c>connectors/Pz.Connector.LocalFiles</c>, <c>connectors/Pz.Connector.Postgres</c>, and
/// <c>connectors/Pz.Connector.S3</c>, rather than restoring them as NuGet packages.
/// <see cref="ConnectorRegistry"/> itself stays host-agnostic (name → instance), so
/// <see cref="ConnectorRegistryFactory"/> only adds to, never replaces, this construction site:
/// builtins are always registered; non-builtin connectors declared in project.yml additionally come
/// from a <c>ConnectorHost</c> over restored <c>.pz/packages</c>.
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
    };

    public static ConnectorRegistry CreateRegistry()
    {
        var registry = new ConnectorRegistry();
        var localFiles = new LocalFilesConnector();
        registry.AddSource("localfiles", localFiles);
        registry.AddSink("localfiles", localFiles);
        var postgres = new PostgresConnector();
        registry.AddSource("postgres", postgres);
        registry.AddSink("postgres", postgres);
        var s3 = new S3Connector();
        registry.AddSource("s3", s3);
        registry.AddSink("s3", s3);
        var sqlServer = new SqlServerConnector();
        registry.AddSource("sqlserver", sqlServer);
        registry.AddSink("sqlserver", sqlServer);
        var azure = new AzureConnector();
        registry.AddSource("azureblob", azure);
        registry.AddSink("azureblob", azure);
        var http = new HttpConnector();
        registry.AddSource("http", http);
        registry.AddSink("http", http);
        var mysql = new MySqlConnector();
        registry.AddSource("mysql", mysql);
        registry.AddSink("mysql", mysql);
        var sqlite = new SqliteConnector();
        registry.AddSource("sqlite", sqlite);
        registry.AddSink("sqlite", sqlite);
        return registry;
    }
}
