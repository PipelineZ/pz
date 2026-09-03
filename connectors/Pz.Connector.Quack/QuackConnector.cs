using System.Net.Sockets;
using Pz.Connectors.Abstractions;

[assembly: PzConnector("quack", typeof(Pz.Connector.Quack.QuackConnector))]

namespace Pz.Connector.Quack;

/// <summary>Remote DuckDB server (Quack protocol) source + sink connector — native-path-only. The
/// engine's DuckDB session attaches the server once per connection through the `quack` extension;
/// every read/write is a plain statement against that alias, executed by the server. Zero drivers:
/// <see cref="CheckConnectionAsync"/> is a TCP reachability probe (credentials are verified by the
/// first run's attach, whose failure is a redacted PZ0311), and `pz validate --connect`'s schema
/// fetch works only with a declared `columns:` contract. TLS is the reverse proxy's job in front
/// of the server; the client assumes HTTPS for non-loopback hosts. Registered as "quack".</summary>
public sealed class QuackConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    public ConnectorInfo Info => new("quack", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["uri", "token"], "properties": { "uri": { "type": "string" }, "token": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

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

    /// <summary>Kept in lockstep with <c>DuckLakeProbe.TcpAsync</c> (replicated, never
    /// referenced).</summary>
    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        if (config.GetString("uri") is not { } uri || !QuackUri.TryParse(uri, out var host, out var port))
        {
            return new ConnectionCheck(false, "permanent: quack connection 'uri' must be of the form quack:host[:port]");
        }

        try
        {
            using var client = new TcpClient();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ConnectTimeout);
            // A bracketed IPv6 literal keeps its brackets in the canonical uri; the socket wants the bare address.
            await client.ConnectAsync(host.Trim('[', ']'), port, timeout.Token).ConfigureAwait(false);
            return new ConnectionCheck(true, $"quack server reachable at {host}:{port} (tcp); credentials are verified at run time");
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new ConnectionCheck(false, $"transient: quack server at {host}:{port} did not accept a connection within {ConnectTimeout.TotalSeconds:0}s");
        }
        catch (SocketException ex)
        {
            return new ConnectionCheck(false, $"transient: quack server at {host}:{port} is unreachable ({ex.SocketErrorCode})");
        }
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new QuackSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new QuackSink(config));
}
