using System.Net.Sockets;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The control-plane half of one connector instance: opens the gRPC channel over the process's
/// unix socket, runs the handshake discipline (protocol-major and capability agreement are load-bearing
/// preconditions, not best-effort checks), configures the connector, and translates every later
/// connector-originated failure into the same exception a caller would see from an in-process connector.
///
/// <para>Ownership: a <see cref="PcpClient"/> never spawns or kills <see cref="ConnectorProcess"/> — the
/// caller owns that lifetime. It only takes responsibility for the process once
/// <c>ConnectAndConfigureAsync</c> returns successfully, at which point <see cref="DisposeAsync"/> is the
/// shutdown ladder's first two rungs (Shutdown RPC, then grace-kill).</para></summary>
public sealed class PcpClient : IAsyncDisposable
{
    private readonly ConnectorProcess _process;
    private readonly GrpcChannel _channel;

    private PcpClient(ConnectorProcess process, GrpcChannel channel, Hello hello, PzConnector.PzConnectorClient grpc)
    {
        _process = process;
        _channel = channel;
        Hello = hello;
        Grpc = grpc;
    }

    /// <summary>The connector's handshake response, kept verbatim: <c>Info.Name</c>/<c>Capabilities</c>
    /// are what the rest of the host trusts about this instance, not whatever the manifest claimed.</summary>
    public Hello Hello { get; }

    /// <summary>The generated gRPC client, open for the lifetime of this <see cref="PcpClient"/>. Every
    /// later control-plane call (Validate, CheckConnection, GetSchema, ...) goes through this.</summary>
    public PzConnector.PzConnectorClient Grpc { get; }

    /// <summary>Dials <paramref name="process"/>'s control socket, performs the Handshake/Configure
    /// sequence under <see cref="ProtocolConstants.HandshakeTimeout"/>, and returns a ready client.
    /// See the <see cref="TimeSpan"/> overload for the full discipline.</summary>
    public static Task<PcpClient> ConnectAndConfigureAsync(
        ConnectorProcess process, ConnectorManifest? manifest, string instanceId,
        ConnectorConfig config, CancellationToken ct) =>
        ConnectAndConfigureAsync(process, manifest, instanceId, config, ProtocolConstants.HandshakeTimeout, ct);

    /// <summary>Same as the five-argument overload, with an injectable handshake timeout — the only
    /// reason this overload exists is so a test can force a short one instead of waiting out the real
    /// 15s <see cref="ProtocolConstants.HandshakeTimeout"/>.
    ///
    /// <para>Discipline: send <c>Handshake{protocol_major}</c>; the connector's Hello must echo an equal
    /// protocol major, and — when <paramref name="manifest"/> declared a non-empty capability list —
    /// Hello's reported capabilities must equal that list as a SET (order-insensitive; the manifest is
    /// advisory, the handshake is authoritative). A timeout, a connect failure, or either mismatch is a
    /// load error: <see cref="ConnectorHostException"/> PZ0356 with <see cref="ConnectorProcess.StderrTail"/>
    /// appended. Only once all of that agrees does <c>Configure{instance_id, config}</c> run; a Configure
    /// failure is connector-originated and maps through <see cref="MapRpcException"/> instead.</para></summary>
    public static async Task<PcpClient> ConnectAndConfigureAsync(
        ConnectorProcess process, ConnectorManifest? manifest, string instanceId,
        ConnectorConfig config, TimeSpan handshakeTimeout, CancellationToken ct)
    {
        // h2c: SocketsHttpHandler refuses to negotiate HTTP/2 over a plaintext transport (there is no
        // TLS/ALPN here to advertise it) unless this switch is set. Idempotent, so setting it on every
        // call is harmless.
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        var channel = GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
        {
            HttpHandler = new SocketsHttpHandler
            {
                ConnectCallback = async (_, cancellationToken) =>
                {
                    // The very first dial races the child process's own startup: Kestrel has not
                    // necessarily bound (created) the socket file yet by the time this callback runs,
                    // which surfaces as SocketError.AddressNotAvailable on .NET's unix-socket connect
                    // path -- not the ENOENT a raw connect(2) would give a C caller. Retry until the
                    // file appears, the process dies, or the caller's own deadline (handshakeTimeout,
                    // wired through this same token) cuts it off; no separate timeout is needed here.
                    while (true)
                    {
                        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                        try
                        {
                            await socket.ConnectAsync(new UnixDomainSocketEndPoint(process.SocketPath), cancellationToken)
                                .ConfigureAwait(false);
                            return new NetworkStream(socket, ownsSocket: true);
                        }
                        catch (SocketException) when (!process.HasExited)
                        {
                            socket.Dispose();
                            await Task.Delay(TimeSpan.FromMilliseconds(20), cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                            socket.Dispose();
                            throw;
                        }
                    }
                },
                // Measured in Task 5: the default idle reaper kills the pooled UDS connection out from
                // under a long-idle control plane (e.g. between a PlanRead and its matching
                // OpenReadStream), which then reads as the connector having died. There is exactly one
                // logical connection per instance, so pooling it forever costs nothing.
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            },
        });

        var grpc = new PzConnector.PzConnectorClient(channel);

        Hello hello;
        using (var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            handshakeCts.CancelAfter(handshakeTimeout);
            try
            {
                var request = new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major, HostInfo = new HostInfo() };
                request.HostInfo.Transports.Add(ProtocolConstants.TransportPipe);
                hello = await grpc.HandshakeAsync(request, cancellationToken: handshakeCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                channel.Dispose();
                throw HandshakeFailed(process, "handshake timed out waiting for Hello");
            }
            catch (RpcException ex)
            {
                channel.Dispose();
                throw HandshakeFailed(process, $"handshake RPC failed: {ex.Status.Detail}");
            }
        }

        if (hello.Info.ProtocolMajor != ProtocolVersion.Major)
        {
            channel.Dispose();
            throw HandshakeFailed(
                process,
                $"connector's Hello reported protocol major {hello.Info.ProtocolMajor} during handshake, but this host speaks {ProtocolVersion.Major}");
        }

        if (manifest is { Capabilities.Count: > 0 })
        {
            var declared = new HashSet<string>(manifest.Capabilities, StringComparer.Ordinal);
            var reported = CapabilityNames(hello.Capabilities);
            if (!declared.SetEquals(reported))
            {
                channel.Dispose();
                throw HandshakeFailed(
                    process,
                    "handshake-reported capabilities " +
                    $"({string.Join(", ", Sorted(reported))}) do not match the manifest's declared " +
                    $"capabilities ({string.Join(", ", Sorted(declared))})");
            }
        }

        var client = new PcpClient(process, channel, hello, grpc);
        try
        {
            var configureRequest = new ConfigureRequest { InstanceId = instanceId, Config = ToStruct(config.Values) };
            await grpc.ConfigureAsync(configureRequest, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            channel.Dispose();
            throw client.MapRpcException(ex);
        }

        return client;
    }

    /// <summary>Rebuilds the failure a connector meant to report from an <see cref="RpcException"/>.
    ///
    /// <para>THE TRAILER IS THE CONTRACT, not the gRPC status code: when
    /// <see cref="ProtocolConstants.ErrorDetailTrailerKey"/> is present, this is a connector-originated
    /// operational failure and comes back as the same <see cref="PzConnectorException"/> an in-process
    /// connector would have thrown — transience and retry-after intact. <c>RetryAfterMs == 0</c> means
    /// ABSENT (no retry-after was set), never <see cref="TimeSpan.Zero"/>, so it is never conflated with a
    /// connector that asked for an immediate retry.</para>
    ///
    /// <para>Absent the trailer, the connector broke the protocol itself (bad ticket, malformed stream, an
    /// RPC call outside the sequence it should be in) rather than reporting an operational failure — that
    /// surfaces as <see cref="ConnectorHostException"/> PZ0358 when the process has already exited
    /// (<see cref="ConnectorProcess.HasExited"/>), or PZ0357 otherwise, both with
    /// <see cref="ConnectorProcess.StderrTail"/> appended when non-empty.</para></summary>
    public Exception MapRpcException(RpcException ex)
    {
        var trailer = ex.Trailers.FirstOrDefault(entry =>
            string.Equals(entry.Key, ProtocolConstants.ErrorDetailTrailerKey, StringComparison.Ordinal));
        if (trailer is not null)
        {
            var detail = PzErrorDetail.Parser.ParseFrom(trailer.ValueBytes);
            TimeSpan? retryAfter = detail.RetryAfterMs == 0 ? null : TimeSpan.FromMilliseconds(detail.RetryAfterMs);
            return new PzConnectorException(detail.Message, detail.IsTransient, retryAfter);
        }

        var stderr = _process.StderrTail;
        var suffix = stderr.Length > 0 ? $"\nstderr:\n{stderr}" : string.Empty;
        return _process.HasExited
            ? new ConnectorHostException(
                "PZ0358",
                $"connector process exited mid-operation: {ex.Status.Detail}{suffix}",
                "check the connector's exit code and stderr logs, and confirm connector stability under the dataset being processed")
            : new ConnectorHostException(
                "PZ0357",
                $"connector protocol violation: {ex.Status.Detail}{suffix}",
                "check connector logs and confirm the connector and host ABI versions are compatible");
    }

    /// <summary>Shutdown ladder's first two rungs: a best-effort Shutdown RPC (the connector's cue to stop
    /// itself gracefully, distinct from being killed), then the grace-kill. Never throws on a connector
    /// that is already dead, hung, or refuses to answer — Shutdown is a courtesy, not a dependency, and
    /// the grace-kill below ends the process regardless.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            using var shutdownCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            await Grpc.ShutdownAsync(new ShutdownRequest(), cancellationToken: shutdownCts.Token).ConfigureAwait(false);
        }
        catch
        {
            // best-effort; the kill ladder below ends the process regardless of whether it heard this
        }

        _channel.Dispose();
        await _process.KillAfterGraceAsync(ProtocolConstants.ShutdownGrace, CancellationToken.None).ConfigureAwait(false);
        await _process.DisposeAsync().ConfigureAwait(false);
    }

    private static ConnectorHostException HandshakeFailed(ConnectorProcess process, string reason)
    {
        var stderr = process.StderrTail;
        var message = stderr.Length > 0
            ? $"connector handshake failed: {reason}\nstderr:\n{stderr}"
            : $"connector handshake failed: {reason}";
        return new ConnectorHostException(
            "PZ0356",
            message,
            "check the connector's startup logs and confirm its declared protocol major and capabilities match what it actually implements");
    }

    /// <summary>Decomposes a <see cref="Hello.Capabilities"/> flags value into the
    /// <see cref="ConnectorCapabilities"/> member names it is the OR of. Relies on every declared member
    /// being a single bit (true in this enum today), which is what lets <see cref="Enum.ToString()"/>
    /// decompose a [Flags] value into an exact name list instead of falling back to the raw number.</summary>
    private static HashSet<string> CapabilityNames(long flags) =>
        new(
            ((ConnectorCapabilities)unchecked((int)flags)).ToString().Split(", ", StringSplitOptions.RemoveEmptyEntries),
            StringComparer.Ordinal);

    private static IEnumerable<string> Sorted(IEnumerable<string> names) => names.OrderBy(n => n, StringComparer.Ordinal);

    /// <summary>Config crosses ONLY inside Configure's <c>google.protobuf.Struct</c> — never argv, env, or
    /// logs. Mirrors the fixture's reverse mapping (<c>StructMapping.ToDictionary</c>): string, bool,
    /// numeric (proto3 Struct has only <c>double</c>, so every number narrows through it), null, list, and
    /// nested map are the whole shape <see cref="ConnectorConfig.Values"/> is ever built from.</summary>
    private static Struct ToStruct(IReadOnlyDictionary<string, object?> values)
    {
        var result = new Struct();
        foreach (var (key, value) in values)
        {
            result.Fields[key] = ToValue(value);
        }

        return result;
    }

    private static Value ToValue(object? value) => value switch
    {
        null => Value.ForNull(),
        bool b => Value.ForBool(b),
        string s => Value.ForString(s),
        IReadOnlyDictionary<string, object?> map => Value.ForStruct(ToStruct(map)),
        IReadOnlyDictionary<string, string> strings => Value.ForStruct(
            ToStruct(strings.ToDictionary(pair => pair.Key, object? (pair) => pair.Value, StringComparer.Ordinal))),
        sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal =>
            Value.ForNumber(Convert.ToDouble(value, System.Globalization.CultureInfo.InvariantCulture)),
        IEnumerable<object?> list => Value.ForList([.. list.Select(ToValue)]),
        _ => Value.ForString(value.ToString() ?? string.Empty),
    };
}
