using Pz.Connectors.Abstractions;

[assembly: PzConnector("mysql", typeof(Pz.Connector.MySql.MySqlConnector))]

namespace Pz.Connector.MySql;

/// <summary>MySQL source + sink connector — native-path-only. DuckDB's own `mysql`
/// extension is the ENTIRE data plane: reads are <c>mysql_query('alias', '…')</c> native scans,
/// writes are attach + INSERT / CREATE OR REPLACE native copies, and the connector ships with zero
/// .NET MySQL driver — it is pure SQL-fragment generation (<see cref="INativeOnlySource"/> +
/// <see cref="INativeOnlySink"/>; engine.force_universal fails at plan time with PZ0312). The
/// control-plane cost of zero-driver: <see cref="CheckConnectionAsync"/> is a raw TCP greeting probe
/// (reachability + server version, not credentials), and `pz validate --connect`'s schema fetch works
/// only for datasets with a declared `columns:` contract. Registered under the logical name
/// "mysql". Connection options: host/database required; port (default 3306), user, password,
/// ssl_mode optional. No merge writes (the DuckDB mysql catalog has no upsert) and no cdc.</summary>
public sealed class MySqlConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    public ConnectorInfo Info => new("mysql", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["host","database"], "properties": { "host": { "type": "string" }, "port": { "type": "integer", "minimum": 1, "maximum": 65535 }, "database": { "type": "string" }, "user": { "type": "string" }, "password": { "type": "string" }, "ssl_mode": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "query": { "type": "string" }, "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    /// <summary>No cross-field rules to enforce: every connection value
    /// rides the CREATE SECRET statement as an ordinary, '' -escaped single-quoted SQL literal (see
    /// <see cref="MySqlSql.CreateSecretSql"/>), so nothing here
    /// occupies a non-quotable position that a space/quote/`=` could break out of. <c>host</c>/
    /// <c>database</c> required-ness is already enforced by <see cref="ConnectionConfigSchema"/>'s
    /// `required` list, ahead of this call.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var host = config.GetString("host");
        if (host is null)
        {
            return new ValueTask<ConnectionCheck>(new ConnectionCheck(false, "permanent: mysql connection requires 'host'"));
        }

        return MySqlGreeting.ProbeAsync(host, (int)(config.GetInt("port") ?? 3306), ct);
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new MySqlSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new MySqlSink(config));
}
