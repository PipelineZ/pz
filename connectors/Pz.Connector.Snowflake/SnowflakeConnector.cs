using Snowflake.Data.Client;
using Pz.Connectors.Abstractions;

[assembly: PzConnector("snowflake", typeof(Pz.Connector.Snowflake.SnowflakeConnector))]

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake source + sink connector. Key-pair (JWT) authentication only -- no password
/// auth surface. Registered under the logical name "snowflake". Connection options:
/// account/user/private_key_path/database/warehouse required; private_key_passphrase/role optional.
/// ISourceConnector/ISinkConnector OpenAsync are wired in Tasks 6-7 (SnowflakeSource/SnowflakeSink
/// do not exist yet); until then they throw a non-transient PzConnectorException naming the gap,
/// rather than a bare NotImplementedException, so a caller that reaches them sees pz's own error
/// shape instead of an unhandled CLR exception.</summary>
public sealed class SnowflakeConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("snowflake", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.Merge |
        ConnectorCapabilities.Transactional | ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["account","user","private_key_path","database","warehouse"], "properties": { "account": { "type": "string" }, "user": { "type": "string" }, "private_key_path": { "type": "string" }, "private_key_passphrase": { "type": "string" }, "database": { "type": "string" }, "warehouse": { "type": "string" }, "role": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "query": { "type": "string" } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            await using var connection = new SnowflakeDbConnection { ConnectionString = BuildConnectionString(config) };
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = "select 1";
            await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return new ConnectionCheck(true);
        }
        catch (Exception ex)
        {
            // ConnectionCheck carries no transience field; fold it into the message tag so callers
            // can parse it (same convention as the other database connector checks).
            return new ConnectionCheck(false, $"{(SfErrors.IsTransient(ex) ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new PzConnectorException("snowflake source not yet wired", isTransient: false);

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new PzConnectorException("snowflake sink not yet wired", isTransient: false);

    /// <summary>Builds the Snowflake.Data key=value connection string. Key-pair (JWT) auth only;
    /// the driver has no builder class worth using for this shape. Public (not internal) so
    /// CheckConnectionAsync's convention and Tasks 6-7's OpenAsync implementations share one
    /// credential-resolution path.</summary>
    public static string BuildConnectionString(ConnectorConfig config)
    {
        string Require(string key) => config.GetString(key) ??
            throw new PzConnectorException($"snowflake connection requires '{key}'", isTransient: false);

        var parts = new List<string>
        {
            $"account={Require("account")}",
            $"user={Require("user")}",
            "authenticator=snowflake_jwt",
            $"private_key_file={Require("private_key_path")}",
            $"db={Require("database")}",
            $"warehouse={Require("warehouse")}",
            "application=pz",
        };
        if (config.GetString("private_key_passphrase") is { } pwd) { parts.Add($"private_key_pwd={pwd}"); }
        if (config.GetString("role") is { } role) { parts.Add($"role={role}"); }
        return string.Join(";", parts);
    }
}
