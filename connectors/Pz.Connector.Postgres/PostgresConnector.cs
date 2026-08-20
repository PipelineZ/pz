using Npgsql;
using Pz.Connectors.Abstractions;

[assembly: PzConnector("postgres", typeof(Pz.Connector.Postgres.PostgresConnector))]

namespace Pz.Connector.Postgres;

/// <summary>Postgres source + sink connector, Npgsql-backed. Universal path only in v0 -- capabilities
/// declare <see cref="ConnectorCapabilities.ColumnPruning"/>, <see
/// cref="ConnectorCapabilities.PredicatePushdown"/>, <see cref="ConnectorCapabilities.PartitionedRead"/>
/// (equal-width range partitioning over a declared <c>partition_column</c>, see <see
/// cref="PostgresSource.PlanReadAsync"/>), <see cref="ConnectorCapabilities.Merge"/> (<c>ON
/// CONFLICT</c>-based upsert, see <see cref="PostgresSink"/>), and <see
/// cref="ConnectorCapabilities.BoundedWindow"/> (<c>cursor &lt;= upper_bound</c> pushdown, see <see
/// cref="PostgresSource.BuildSelect"/>); no <see cref="ConnectorCapabilities.NativeScan"/>
/// (no postgres_scanner native scan is wired up yet) and no <see cref="ConnectorCapabilities.NativeCopy"/>
/// (the sink is universal-path only -- see <see cref="PostgresSink"/>). <see
/// cref="ConnectorCapabilities.ApplyDeletes"/>: merge write sessions apply cdc
/// delete-key batches, hard or soft, in the same transaction as the upsert. Registered under the
/// logical name "postgres".
/// Connection options: <c>host</c> and <c>database</c> are required; <c>port</c> defaults to 5432;
/// <c>user</c>, <c>password</c>, <c>ssl_mode</c> are optional. Sink output options: <c>schema</c> (default
/// <c>public</c>), <c>table</c> (default = the output's name).</summary>
public sealed class PostgresConnector : ISourceConnector, ISinkConnector
{
    public ConnectorInfo Info => new("postgres", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ColumnPruning |
        ConnectorCapabilities.PredicatePushdown | ConnectorCapabilities.PartitionedRead |
        ConnectorCapabilities.Merge | ConnectorCapabilities.Transactional |
        ConnectorCapabilities.ReplaceWrites |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound |
        ConnectorCapabilities.ApplyDeletes | ConnectorCapabilities.ChangeCapture;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["host","database"], "properties": { "host": { "type": "string" }, "port": { "type": "integer", "minimum": 1, "maximum": 65535 }, "database": { "type": "string" }, "user": { "type": "string" }, "password": { "type": "string" }, "ssl_mode": { "type": "string" } }, "additionalProperties": false }""";

    // "columns" is not read by PostgresSource today (its schema is inferred from the ADO.NET reader,
    // not a declared contract), but the dataset-level columns: block is a generic mechanism --
    // ProjectLoader/SpecBuilder merge it into every connector's dataset options uniformly, regardless
    // of whether that connector consumes it -- so it is accepted here too, for forward compatibility.
    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "query": { "type": "string" }, "partition_column": { "type": "string" }, "partitions": { "type": "integer", "minimum": 1, "maximum": 16 }, "publication": { "type": "string" }, "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            await using var connection = new NpgsqlConnection(BuildConnectionString(config));
            await connection.OpenAsync(ct).ConfigureAwait(false);
            await using var command = new NpgsqlCommand("select 1", connection);
            await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return new ConnectionCheck(true);
        }
        catch (NpgsqlException ex)
        {
            // The ConnectionCheck record carries no separate transience field (its shape is
            // (bool Ok, string? Message)), so IsTransient is folded into the
            // message text -- callers that need to branch on it can parse the leading tag.
            return new ConnectionCheck(false, $"{(ex.IsTransient ? "transient" : "permanent")}: {ex.Message}");
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new PostgresSource(BuildConnectionString(config)));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new PostgresSink(BuildConnectionString(config)));

    internal static string BuildConnectionString(ConnectorConfig config)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = config.GetString("host") ??
                throw new PzConnectorException("postgres connection requires 'host'", isTransient: false),
            Port = (int)(config.GetInt("port") ?? 5432),
            Database = config.GetString("database") ??
                throw new PzConnectorException("postgres connection requires 'database'", isTransient: false),
            // Pin the session to UTC. pz canonicalizes every watermark/window value to a UTC wall-clock
            // string with NO offset (WindowMath: "every pz timestamp is UTC by convention"), and pushes it
            // as an untyped literal (BuildSelect). Postgres coerces an offset-less literal against a
            // `timestamptz` cursor column in the SESSION time zone -- so on a non-UTC session the boundary
            // shifts by the offset, silently skipping (loss) or re-reading (duplication) a band of rows and
            // then advancing the watermark past the skipped ones permanently. Under a UTC session the
            // coercion is exact for pz's stored values. Reads already normalize timestamptz to UTC (Npgsql)
            // and the sink writes UTC instants, so UTC is consistent end-to-end.
            Timezone = "UTC",
        };

        var user = config.GetString("user");
        if (user is not null)
        {
            builder.Username = user;
        }

        var password = config.GetString("password");
        if (password is not null)
        {
            builder.Password = password;
        }

        var sslMode = config.GetString("ssl_mode");
        if (sslMode is not null)
        {
            builder.SslMode = Enum.Parse<SslMode>(sslMode, ignoreCase: true);
        }

        return builder.ConnectionString;
    }
}
