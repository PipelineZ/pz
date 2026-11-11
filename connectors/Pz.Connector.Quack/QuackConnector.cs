using Pz.Connectors.Abstractions;
using Pz.Shared.Probes;
using Pz.Shared.Quack;

[assembly: PzConnector("quack", typeof(Pz.Connector.Quack.QuackConnector))]

namespace Pz.Connector.Quack;

/// <summary>Remote DuckDB server (Quack protocol) source + sink connector — native-path-only. The
/// engine's DuckDB session attaches the server once per connection through the `quack` extension;
/// every read/write is a plain statement against that alias, executed by the server. Zero drivers:
/// <see cref="CheckConnectionAsync"/> is a TCP reachability probe (credentials are verified by the
/// first run's attach, whose failure is a redacted PZ0311), and `pz validate --connect`'s schema
/// fetch works only with a declared `columns:` contract. TLS is the reverse proxy's job in front
/// of the server; the client assumes HTTPS for non-loopback hosts. Registered as "quack".</summary>
public sealed class QuackConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink, IOutputConfigSchema
{
    public ConnectorInfo Info => new("quack", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["uri", "token"], "properties": { "uri": { "type": "string" }, "token": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    // QuackSink reads no connector-owned write option at all -- every write.* key beyond the
    // engine-owned ones is unknown.
    public string OutputConfigSchema =>
        """{ "type": "object", "properties": {}, "additionalProperties": false }""";

    /// <summary>Offline, aggregate: the uri must parse as quack:host[:port]; the server refuses tokens
    /// shorter than four characters, so refuse them here rather than at first run.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        if (config.GetString("uri") is { } uri && !QuackUri.TryParse(uri, out _, out _))
        {
            errors.Add("'uri' must be of the form quack:host[:port]");
        }

        if (config.GetString("token") is { } token && token.Length < 4)
        {
            errors.Add("'token' must be at least four characters (the quack server refuses shorter tokens)");
        }

        return new(errors.Count == 0 ? ValidationResult.Success : ValidationResult.Failed([.. errors]));
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        config.GetString("uri") is { } uri && QuackUri.TryParse(uri, out var host, out var port)
            ? TcpProbe.CheckAsync(host, port, "quack server", ct)
            : new(new ConnectionCheck(false, "permanent: quack connection 'uri' must be of the form quack:host[:port]"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new QuackSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new QuackSink(config));
}
