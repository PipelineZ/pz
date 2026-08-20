using Microsoft.Data.SqlClient;
using Pz.Connectors.Abstractions;

[assembly: PzConnector("sqlserver", typeof(Pz.Connector.SqlServer.SqlServerConnector))]

namespace Pz.Connector.SqlServer;

/// <summary>Microsoft SQL Server source + sink connector. Universal Arrow path with a connector-owned
/// typed reader (SqlServerArrowReader); sink uses SqlBulkCopy with mode-specific strategies.
/// <see cref="ConnectorCapabilities.ApplyDeletes"/>: merge write sessions apply
/// cdc delete-key batches, hard or soft, in the same transaction as the merge. Registered under the
/// logical name "sqlserver". Connection options: host/database required; port,
/// user/password, authentication (SqlClient passthrough for Entra ID), encrypt,
/// trust_server_certificate optional.</summary>
public sealed class SqlServerConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("sqlserver", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.PartitionedRead |
        ConnectorCapabilities.Merge | ConnectorCapabilities.Transactional |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound |
        ConnectorCapabilities.ApplyDeletes | ConnectorCapabilities.ChangeCapture |
        ConnectorCapabilities.TextLengthStats;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["host","database"], "properties": { "host": { "type": "string" }, "port": { "type": "integer", "minimum": 1, "maximum": 65535 }, "database": { "type": "string" }, "user": { "type": "string" }, "password": { "type": "string" }, "authentication": { "type": "string" }, "encrypt": { "type": "boolean" }, "trust_server_certificate": { "type": "boolean" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "query": { "type": "string" }, "procedure": { "type": "string" }, "parameters": { "type": "object", "additionalProperties": { "type": ["string","number","boolean","null"] } }, "partition_column": { "type": "string" }, "partitions": { "type": "integer", "minimum": 1, "maximum": 16 }, "capture_instance": { "type": "string" }, "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false, "not": { "required": ["query","procedure"] } }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            await using var connection = new SqlConnection(BuildConnectionString(config));
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new SqlCommand("select 1", connection);
            await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return new ConnectionCheck(true);
        }
        catch (SqlException ex)
        {
            // ConnectionCheck carries no transience field; fold it into the message tag so callers
            // can parse it (same convention as the other database connector checks).
            return new ConnectionCheck(false, $"{(ex.IsTransient ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SqlServerSource(BuildConnectionString(config)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new SqlServerSink(BuildConnectionString(config)));

    /// <summary>Public (not internal) so
    /// <c>Pz.Cli.StateBackendFactory</c> can reuse it when <c>state.connection</c> names a
    /// <c>connector: sqlserver</c> entry -- one credential-resolution path for "sqlserver" config,
    /// whether the connection is used for a pipeline's data or for pz's own state.</summary>
    public static string BuildConnectionString(ConnectorConfig config)
    {
        var host = config.GetString("host") ??
            throw new PzConnectorException("sqlserver connection requires 'host'", isTransient: false);
        var port = config.GetInt("port");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = port is null ? host : $"{host},{port}",
            InitialCatalog = config.GetString("database") ??
                throw new PzConnectorException("sqlserver connection requires 'database'", isTransient: false),
            ApplicationName = "pz",
        };

        var user = config.GetString("user");
        if (user is not null)
        {
            builder.UserID = user;
        }

        var password = config.GetString("password");
        if (password is not null)
        {
            builder.Password = password;
        }

        var authentication = config.GetString("authentication");
        if (authentication is not null)
        {
            try
            {
                // SqlClient's own parser accepts the documented display forms ("Active Directory
                // Default" etc.); routing through the builder keyword keeps us off its enum names.
                builder["Authentication"] = authentication;
            }
            catch (ArgumentException ex)
            {
                throw new PzConnectorException(
                    $"sqlserver connection 'authentication' value '{authentication}' is not recognized " +
                    "by SqlClient -- hint: use a documented mode such as 'Active Directory Default'",
                    isTransient: false, innerException: ex);
            }
        }

        if (config.Values.ContainsKey("encrypt"))
        {
            builder.Encrypt = config.GetBool("encrypt") ? SqlConnectionEncryptOption.Mandatory : SqlConnectionEncryptOption.Optional;
        }

        if (config.Values.ContainsKey("trust_server_certificate"))
        {
            builder.TrustServerCertificate = config.GetBool("trust_server_certificate");
        }

        return builder.ConnectionString;
    }
}
