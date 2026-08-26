using System.Collections.Concurrent;
using System.Net.Sockets;
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
    private readonly ConcurrentDictionary<string, byte> _escalating = new(StringComparer.Ordinal);
    private readonly ConcurrentBag<Task> _escalations = [];
    private readonly TaskCompletionSource _ladderClaimedByEscalation =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _disposed;

    // 0 = unknown (no PzErrorDetail mapped yet), 1 = last mapped detail was transient, 2 = non-transient.
    // Written from MapRpcException, read from ProcessFailureMapping on shim call sites -- concurrent
    // partition reads share one PcpClient/Grpc client, so this needs a real memory barrier, not a plain
    // bool? field (a torn or stale read here would misclassify a crash's transience).
    private int _lastErrorTransient;

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

    /// <summary>How long <see cref="AttachCancelLadder"/> waits for the connector to acknowledge a
    /// <c>Cancel</c> before condemning the instance. Defaults to
    /// <see cref="ProtocolConstants.CancelGrace"/>; the setter is a test/host-construction seam for
    /// compressing the window, never a protocol knob a connector can influence.</summary>
    internal TimeSpan CancelGrace { get; set; } = ProtocolConstants.CancelGrace;

    /// <summary>How long the shutdown ladder waits for the process to exit on its own after the
    /// <c>Shutdown</c> RPC before killing the process tree. Defaults to
    /// <see cref="ProtocolConstants.ShutdownGrace"/>; same seam as <see cref="CancelGrace"/>.</summary>
    internal TimeSpan ShutdownGrace { get; set; } = ProtocolConstants.ShutdownGrace;

    /// <summary>Test seam only: completes once a cancellation escalation has WON the shutdown ladder —
    /// i.e. the connector failed to acknowledge Cancel and this client is now being condemned on a
    /// background task. Lets a test reach the exact state <see cref="DisposeAsync"/>'s join exists for
    /// (dispose arriving mid-ladder) with a gate rather than a sleep. Never set outside that path, and
    /// never awaited by production code.</summary>
    internal Task EscalationClaimedLadder => _ladderClaimedByEscalation.Task;

    /// <summary>Transience of the last connector-reported <c>PzErrorDetail</c> this client has mapped
    /// from a trailer-carrying <see cref="RpcException"/> (see <see cref="MapRpcException"/>), or null
    /// if none has been mapped yet on this client. Additive tracking for the ISource/ISink shims
    /// (ProcessSourceConnector/ProcessSinkConnector): a process that dies immediately after reporting a
    /// NON-transient failure must not have that death independently reclassified as transient just
    /// because it looks like a mid-operation crash — a crash right after "this is permanent" is not a
    /// new transient fact.</summary>
    public bool? LastErrorWasTransient => Volatile.Read(ref _lastErrorTransient) switch
    {
        1 => true,
        2 => false,
        _ => null,
    };

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
    /// protocol major, must report the name <paramref name="manifest"/> registers (when it names one),
    /// and — when <paramref name="manifest"/> declared a non-empty capability list — Hello's reported
    /// capabilities must equal that list as a SET (order-insensitive; the manifest is advisory, the
    /// handshake is authoritative). Every one of those gates runs BEFORE Configure, because Configure is
    /// where config values cross and a connector that is not the declared one must never see them. A
    /// timeout, a connect failure, or any of those mismatches is a
    /// load error: <see cref="ConnectorHostException"/> PZ0356 with <see cref="ConnectorProcess.StderrTail"/>
    /// appended. Only once all of that agrees does <c>Configure{instance_id, config}</c> run; a Configure
    /// failure is connector-originated and maps through <see cref="MapRpcException(RpcException, CancellationToken)"/>
    /// instead.
    ///
    /// <para>Caller cancellation (<paramref name="ct"/>) is never a load/protocol error: gRPC surfaces a
    /// cancelled caller token as <c>RpcException(StatusCode.Cancelled)</c>, not
    /// <see cref="OperationCanceledException"/>, so both the handshake and the Configure call re-check
    /// <c>ct.IsCancellationRequested</c> against the RPC's status code and rethrow a plain
    /// <see cref="OperationCanceledException"/> instead of wrapping it as PZ0356/PZ0357/PZ0358. The
    /// internal <paramref name="handshakeTimeout"/> firing is a different token (this method's own linked
    /// <c>handshakeCts</c>, not the caller's) and still maps to PZ0356.</para></summary>
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
                // The default idle reaper would kill the pooled UDS connection out from under a
                // long-idle control plane (e.g. between a PlanRead and its matching OpenReadStream),
                // which then reads as the connector having died. There is exactly one logical
                // connection per instance, so pooling it forever costs nothing.
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
            catch (RpcException ex) when (IsCallerCancellation(ex, ct))
            {
                channel.Dispose();
                throw new OperationCanceledException("connector handshake cancelled by caller", ex, ct);
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

        // Identity before configuration: a connector that is not the one the manifest registers must
        // never receive this connection's config. Both this and the capability gate below therefore sit
        // ahead of Configure -- the only place config values cross -- not after it.
        if (manifest?.Name is { Length: > 0 } declaredName &&
            !string.Equals(hello.Info.Name, declaredName, StringComparison.Ordinal))
        {
            channel.Dispose();
            throw HandshakeFailed(
                process,
                $"connector introduced itself as '{hello.Info.Name}' but its manifest registers the name '{declaredName}'");
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
            var configureRequest = new ConfigureRequest { InstanceId = instanceId, Config = MessageMapping.ToStruct(config.Values) };
            await grpc.ConfigureAsync(configureRequest, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            channel.Dispose();
            throw client.MapRpcException(ex, ct);
        }

        return client;
    }

    /// <summary>Rebuilds the failure a connector meant to report from an <see cref="RpcException"/>.
    ///
    /// <para>Caller cancellation first: when <paramref name="ct"/> is the token the caller cancelled AND
    /// <paramref name="ex"/>'s status is <see cref="StatusCode.Cancelled"/>/<see cref="StatusCode.DeadlineExceeded"/>,
    /// this was never a connector- or protocol-level failure at all — gRPC just reports a cancelled caller
    /// token this way rather than as <see cref="OperationCanceledException"/>. Rethrow the plain
    /// cancellation instead of wrapping it. A <c>Cancelled</c>/<c>DeadlineExceeded</c> status while
    /// <paramref name="ct"/> is NOT cancelled is a genuinely connector-side cancel, which still falls
    /// through to the protocol-violation mapping below — the caller asked for nothing here.</para>
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
    public Exception MapRpcException(RpcException ex, CancellationToken ct = default)
    {
        if (IsCallerCancellation(ex, ct))
        {
            return new OperationCanceledException("connector RPC cancelled by caller", ex, ct);
        }

        var trailer = ex.Trailers.FirstOrDefault(entry =>
            string.Equals(entry.Key, ProtocolConstants.ErrorDetailTrailerKey, StringComparison.Ordinal));
        if (trailer is not null)
        {
            var detail = PzErrorDetail.Parser.ParseFrom(trailer.ValueBytes);
            TimeSpan? retryAfter = detail.RetryAfterMs == 0 ? null : TimeSpan.FromMilliseconds(detail.RetryAfterMs);
            Volatile.Write(ref _lastErrorTransient, detail.IsTransient ? 1 : 2);
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

    /// <summary>Arms the cancellation ladder for one in-flight operation: when <paramref name="ct"/>
    /// fires, the connector is told <c>Cancel{opId}</c> and — if it does not answer within
    /// <see cref="CancelGrace"/> — condemned through the shutdown ladder (<c>Shutdown</c>,
    /// <see cref="ShutdownGrace"/>, kill the process tree). Dispose the returned registration when the
    /// operation ends so a completed operation cannot be escalated later.
    ///
    /// <para>Escalation runs OFF the caller's path on purpose: a cancelled caller must get its
    /// <see cref="OperationCanceledException"/> back promptly (the engine's cooperative-cancel
    /// contract), not wait out both grace windows while an uncooperative connector is reaped. The
    /// operation's own RPC/stream still unwinds as cancellation — nothing here converts it into a
    /// PZ03xx failure.</para>
    ///
    /// <para>Acknowledgement, not observed quiescence, is the escalation test: the host cancels its own
    /// side of a read the moment the token fires, so whether the connector actually STOPPED is not
    /// observable from here. A connector that answers Cancel is presumed to be cooperating; one that
    /// does not answer is presumed hung, and a hung connector instance is not reusable.</para></summary>
    internal CancellationTokenRegistration AttachCancelLadder(string opId, CancellationToken ct)
    {
        if (!ct.CanBeCanceled)
        {
            return default;
        }

        return ct.Register(static state =>
        {
            var (client, id) = ((PcpClient, string))state!;
            // One ladder per op id: several partitions of one PlanRead share it, and each of their
            // reads registers its own callback on the same token.
            if (client._escalating.TryAdd(id, 0))
            {
                client.StartEscalation(id);
            }
        }, (this, opId));
    }

    /// <summary>Starts one escalation and records it where <see cref="DisposeAsync"/> can join it.
    /// Recording happens before the escalation can reach its <c>_disposed</c> claim — that claim sits
    /// behind an awaited RPC, so control returns here (and the task lands in the bag) first, which is
    /// what makes the join in <see cref="DisposeAsync"/> exhaustive rather than best-effort.</summary>
    private void StartEscalation(string opId) => _escalations.Add(EscalateCancelAsync(opId));

    private async Task EscalateCancelAsync(string opId)
    {
        try
        {
            using var cancelCts = new CancellationTokenSource(CancelGrace);
            await Grpc.CancelAsync(new CancelRequest { OpId = opId }, cancellationToken: cancelCts.Token)
                .ConfigureAwait(false);
            return;
        }
        catch (ObjectDisposedException)
        {
            // The channel is already gone because the caller disposed this client first; the shutdown
            // ladder it ran is exactly what escalation would have done here.
            return;
        }
        catch
        {
            // No acknowledgement within the grace (or none possible at all): fall through and condemn.
        }

        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _ladderClaimedByEscalation.TrySetResult();
        try
        {
            await ShutdownLadderAsync().ConfigureAwait(false);
        }
        catch
        {
            // DisposeAsync joins this task, but the ladder is best-effort at every rung and must not
            // surface a teardown failure as the caller's exception.
        }
    }

    /// <summary>Shutdown ladder's first two rungs: a best-effort Shutdown RPC (the connector's cue to stop
    /// itself gracefully, distinct from being killed), then the grace-kill. Never throws on a connector
    /// that is already dead, hung, or refuses to answer — Shutdown is a courtesy, not a dependency, and
    /// the grace-kill below ends the process regardless. Idempotent (mirrors
    /// <see cref="ConnectorProcess.DisposeAsync"/>'s pattern): a second call is a no-op rather than
    /// re-sending Shutdown or re-running the kill ladder against an already-torn-down process — which is
    /// also what keeps this and <see cref="AttachCancelLadder"/>'s escalation from running it twice.
    ///
    /// <para>Whoever LOST that race still waits here: a cancel escalation runs on a background task, and
    /// returning from dispose while one is mid-ladder would let the host exit (Ctrl+C, run teardown)
    /// with the uncooperative child still alive and unowned. This method does not return until every
    /// escalation started on this client has finished.</para></summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            await ShutdownLadderAsync().ConfigureAwait(false);
        }

        await JoinEscalationsAsync().ConfigureAwait(false);
    }

    /// <summary>Joins every escalation started so far. The snapshot is safe to take once: an escalation
    /// starting after it finds <c>_disposed</c> already claimed and a disposed channel, so it returns
    /// without a ladder of its own rather than outliving this join.</summary>
    private async Task JoinEscalationsAsync()
    {
        try
        {
            await Task.WhenAll(_escalations.ToArray()).ConfigureAwait(false);
        }
        catch
        {
            // EscalateCancelAsync never lets an exception escape (see its own catch-alls); this is
            // defense in depth, not an expected path.
        }
    }

    private async Task ShutdownLadderAsync()
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
        await _process.KillAfterGraceAsync(ShutdownGrace, CancellationToken.None).ConfigureAwait(false);
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

    /// <summary>True when <paramref name="ex"/> is the RpcException shape gRPC gives a cancelled CALLER
    /// token (never <see cref="OperationCanceledException"/>) AND <paramref name="ct"/> -- the caller's
    /// own token, not any internal deadline -- is the one that fired. A default/uncancelled
    /// <paramref name="ct"/> always returns false, which is what lets an internal timeout (its own
    /// unrelated token) and a genuinely connector-side cancel both keep falling through to the
    /// protocol-violation mapping instead.</summary>
    private static bool IsCallerCancellation(RpcException ex, CancellationToken ct) =>
        ct.IsCancellationRequested && ex.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded;
}
