using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;
using Pz.Connectors.Protocol.V1;

namespace PcpFakeConnector;

/// <summary>The control plane of the fake connector: a real <c>PzConnector</c> service that delegates
/// every call to an in-process <see cref="LocalFilesConnector"/>.
///
/// <para>Wrapping a shipped builtin rather than a stub is what makes this fixture worth having: a
/// host-side test can run the same project through <c>localfiles</c> and through
/// <c>localfiles-pcp</c> and demand identical results, so the protocol is measured against real
/// connector behavior instead of against a mock that agrees with whatever the host does.</para>
///
/// <para>Credentials reach this service through <see cref="Configure"/> and nowhere else. The argv
/// switches this fixture honors are test switches — which failure to stage — never configuration.</para></summary>
internal sealed class PcpService(
    FixtureOptions options,
    TicketRegistry tickets,
    IHostApplicationLifetime lifetime) : PzConnector.PzConnectorBase
{
    /// <summary>The name this fixture registers under, distinct from the builtin's "localfiles" so a
    /// parity test can name both in one project.</summary>
    public const string ConnectorName = "localfiles-pcp";

    /// <summary>Static, connector-authored op label for every <c>--use-gate</c> partition read -- never
    /// a value derived from the dataset spec, per <c>IOperationGate</c>'s own contract.</summary>
    private const string GateOpLabel = "localfiles-pcp.read_partition";

    private readonly LocalFilesConnector _connector = new();
    private readonly SemaphoreSlim _openGate = new(1, 1);
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _ops = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, PlannedRead> _plans = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, WriteSessionState> _sessions = new(StringComparer.Ordinal);
    private readonly HostChannelPeer _gatePeer = new();

    private ConnectorConfig? _config;
    private ISource? _source;
    private ISink? _sink;

    private sealed record PlannedRead(DatasetSpec Spec, Schema Schema, IReadOnlyList<IDatasetPartition> Partitions);

    // ---- identity + config ------------------------------------------------------------------

    public override async Task<Hello> Handshake(HandshakeRequest request, ServerCallContext context)
    {
        if (options.HangHandshake)
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken).ConfigureAwait(false);
        }

        var capabilities = (long)_connector.Capabilities;
        var hello = new Hello
        {
            Info = new ConnectorInfoMsg
            {
                Name = ConnectorName,
                Version = _connector.Info.Version,
                ProtocolMajor = options.WrongProtocolMajor
                    ? ProtocolVersion.Major + 1
                    : _connector.Info.ProtocolMajor,
            },
            Capabilities = options.MisreportCapabilities
                ? capabilities | (long)ConnectorCapabilities.Merge
                : capabilities,
            ConnectionConfigSchema = _connector.ConnectionConfigSchema,
            DatasetConfigSchema = _connector.DatasetConfigSchema,
        };
        hello.Transports.Add(ProtocolConstants.TransportPipe);
        return hello;
    }

    public override Task<ConfigureResponse> Configure(ConfigureRequest request, ServerCallContext context)
    {
        _config = new ConnectorConfig(StructMapping.ToDictionary(request.Config));

        // One LogEvent per Configure, always -- fields carry the connection NAME (instance_id) and
        // this connector's own identity, never a config VALUE (credentials, paths, etc. never appear
        // here). The reverse channel is not open yet at this point in the RPC sequence (HostChannel is
        // opened by the host only after Configure returns), so this is buffered and flushed the moment
        // HostChannel attaches -- see HostChannelPeer.QueueConfiguredLog.
        var configured = new LogEvent { Level = 2, Message = "connector configured" }; // 2 = Information
        configured.Fields["instance_id"] = request.InstanceId;
        configured.Fields["connector"] = ConnectorName;
        _gatePeer.QueueConfiguredLog(configured);

        return Task.FromResult(new ConfigureResponse());
    }

    public override Task<ValidationResultMsg> Validate(ValidateRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            var result = await _connector
                .ValidateAsync(new ConnectorConfig(StructMapping.ToDictionary(request.Config)), context.CancellationToken)
                .ConfigureAwait(false);
            var message = new ValidationResultMsg();
            message.Errors.AddRange(result.Errors);
            return message;
        });

    public override Task<ConnectionCheckMsg> CheckConnection(CheckRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            if (options.FailCheckTransient)
            {
                throw new PzConnectorException(
                    "fixture: connection check refused on purpose", isTransient: true, TimeSpan.FromMilliseconds(250));
            }

            var check = await _connector
                .CheckConnectionAsync(new ConnectorConfig(StructMapping.ToDictionary(request.Config)), context.CancellationToken)
                .ConfigureAwait(false);
            var message = new ConnectionCheckMsg { Ok = check.Ok };
            if (check.Message is not null)
            {
                message.Message = check.Message;
            }

            return message;
        });

    // ---- source -----------------------------------------------------------------------------

    public override Task<DatasetSchemaMsg> GetSchema(GetSchemaRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            using var linked = LinkOp(request.OpId, context);
            var ct = linked.Token;
            var source = await OpenSourceAsync(ct).ConfigureAwait(false);
            var schema = await source.GetSchemaAsync(SpecMapping.ToDatasetSpec(request.Spec), ct).ConfigureAwait(false);
            return new DatasetSchemaMsg { ArrowSchemaIpc = await SerializeSchemaAsync(schema.Schema, ct).ConfigureAwait(false) };
        });

    public override Task<NativeScanResponse> TryNativeScan(NativeScanRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            using var linked = LinkOp(request.OpId, context);
            var ct = linked.Token;
            var source = await OpenSourceAsync(ct).ConfigureAwait(false);
            if (!source.TryGetNativeScan(SpecMapping.ToDatasetSpec(request.Spec), out var scan))
            {
                return new NativeScanResponse { Found = false };
            }

            var response = new NativeScanResponse
            {
                Found = true,
                SqlFragment = scan.SqlFragment,
                SchemaInferred = scan.SchemaInferred,
            };
            response.SetupStatements.AddRange(scan.SetupStatements);
            if (scan.Mechanism is not null)
            {
                response.Mechanism = scan.Mechanism;
            }

            if (scan.SniffFragment is not null)
            {
                response.SniffFragment = scan.SniffFragment;
            }

            return response;
        });

    public override async Task PlanRead(
        PlanReadRequest request, IServerStreamWriter<PartitionMsg> responseStream, ServerCallContext context)
    {
        try
        {
            using var linked = LinkOp(request.OpId, context);
            var ct = linked.Token;
            var spec = SpecMapping.ToDatasetSpec(request.Spec);
            var source = await OpenSourceAsync(ct).ConfigureAwait(false);
            var partitions = await source
                .PlanReadAsync(spec, SpecMapping.ToReadHints(request.Hints), ct)
                .ConfigureAwait(false);
            var schema = await source.GetSchemaAsync(spec, ct).ConfigureAwait(false);
            _plans[request.OpId] = new PlannedRead(spec, schema.Schema, partitions);

            for (var i = 0; i < partitions.Count; i++)
            {
                ct.ThrowIfCancellationRequested();
                var partition = partitions[i];
                await responseStream.WriteAsync(new PartitionMsg
                {
                    // Ordinals stand in wherever the connector declares no stable id, which is exactly
                    // the host-side rule: an id is meaningful only under StablePartitionIds.
                    PartitionId = partition is IIdentifiedPartition identified
                        ? identified.PartitionId
                        : i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    Checkpointing = partition is ICheckpointingPartition,
                    SyncState = partition is ISyncStatePartition,
                }).ConfigureAwait(false);
            }
        }
        catch (PzConnectorException ex)
        {
            throw ToRpcException(ex);
        }
    }

    public override Task<ReadStreamTicket> OpenReadStream(OpenReadRequest request, ServerCallContext context) =>
        Guarded(() =>
        {
            if (!_plans.TryGetValue(request.OpId, out var plan))
            {
                throw new RpcException(new Status(
                    StatusCode.FailedPrecondition, $"no plan for op '{request.OpId}'; call PlanRead first"));
            }

            var partition = ResolvePartition(plan, request.PartitionId);
            if (request.ResumeCheckpoint is { Length: > 0 } checkpoint &&
                partition is ICheckpointingPartition checkpointing)
            {
                checkpointing.TryResumeFrom(checkpoint);
            }

            // Wraps the checkpoint-resolved partition, not the other way around: checkpoint resume
            // targets the REAL partition's ICheckpointingPartition, which the gate wrapper does not
            // (and must not) implement.
            IDatasetPartition ticketPartition = options.UseGate
                ? new GatedReadPartition(partition, _gatePeer, GateOpLabel)
                : partition;

            var ticket = tickets.Mint(new ReadTicket(
                plan.Schema, ticketPartition, SpecMapping.ToBatchOptions(request.Options), OpToken(request.OpId)));
            return Task.FromResult(new ReadStreamTicket { Ticket = ByteString.CopyFrom(ticket) });
        });

    // ---- sink -------------------------------------------------------------------------------

    public override Task<NativeCopyResponse> TryNativeCopy(NativeCopyRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            using var linked = LinkOp(request.OpId, context);
            var ct = linked.Token;
            var sink = await OpenSinkAsync(ct).ConfigureAwait(false);
            if (!sink.TryGetNativeCopy(SpecMapping.ToOutputSpec(request.Spec), out var copy))
            {
                return new NativeCopyResponse { Found = false };
            }

            var response = new NativeCopyResponse { Found = true, CopySql = copy.CopySql };
            response.SetupStatements.AddRange(copy.SetupStatements);
            if (copy.Mechanism is not null)
            {
                response.Mechanism = copy.Mechanism;
            }

            response.Finalizations.AddRange(copy.Finalizations.Select(
                move => new FileMoveMsg { TempPath = move.TempPath, FinalPath = move.FinalPath }));
            return response;
        });

    public override Task<WriteSessionTicket> BeginWrite(BeginWriteRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            using var linked = LinkOp(request.OpId, context);
            var ct = linked.Token;
            var sink = await OpenSinkAsync(ct).ConfigureAwait(false);
            var schema = await DeserializeSchemaAsync(request.ArrowSchemaIpc, ct).ConfigureAwait(false);
            var session = await sink
                .BeginWriteAsync(SpecMapping.ToOutputSpec(request.Spec), schema, ct)
                .ConfigureAwait(false);
            var state = new WriteSessionState(Guid.NewGuid().ToString("n"), request.OpId, session);
            _sessions[state.SessionId] = state;
            return new WriteSessionTicket
            {
                SessionId = state.SessionId,
                Ticket = ByteString.CopyFrom(tickets.Mint(new WriteTicket(state))),
                // Without this every PCP sink looks like DiscardsAll to the host, whatever it actually
                // wraps -- the wrapped sink's own declaration crosses verbatim. --report-abort-semantics-none
                // is a test switch (LocalFiles itself is always DiscardsAll) proving the field genuinely
                // crosses the wire rather than the host merely echoing its own default back.
                AbortSemantics = options.ReportAbortSemanticsNone
                    ? AbortSemanticsMsg.AbortSemanticsNone
                    : SpecMapping.ToAbortSemanticsMsg(sink.AbortSemantics),
            };
        });

    public override Task<WriteResultMsg> CommitWrite(SessionRef request, ServerCallContext context) =>
        Guarded(async () =>
        {
            var state = TakeSession(request.SessionId);
            try
            {
                // The write data stream must be fully drained -- end-of-stream seen -- before the
                // commit runs, or a commit could land a prefix of the rows the host believes it sent.
                await state.Drained.Task.WaitAsync(context.CancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // The stream tore or the call deadline passed, so CommitAsync was never invoked and
                // the session is still abortable. Put it back where AbortWrite can find it: the ABI's
                // "never abort after commit attempted" rule is about an ATTEMPTED commit, and there
                // was none.
                _sessions.TryAdd(state.SessionId, state);
                throw;
            }

            // Drained is set from inside the pump, a few instructions before the pump task itself
            // completes. Closing and awaiting it is what guarantees no WriteBatchAsync is still in
            // flight when CommitAsync runs, and shuts out any later data connection for this session.
            await QuiesceAsync(state, context.CancellationToken).ConfigureAwait(false);
            await using (state.Session.ConfigureAwait(false))
            {
                var result = await state.Session.CommitAsync(context.CancellationToken).ConfigureAwait(false);
                return new WriteResultMsg { RowsWritten = result.RowsWritten, BatchesWritten = result.BatchesWritten };
            }
        });

    public override Task<AbortResponse> AbortWrite(SessionRef request, ServerCallContext context) =>
        Guarded(async () =>
        {
            // No drain wait: abort exists precisely for the case where the stream never completed. But
            // a pump may well be mid-WriteBatchAsync right now, so cancel it and wait for it to stop
            // before aborting -- disposing the session under a live writer is the use-after-dispose
            // this ordering exists to prevent.
            var state = TakeSession(request.SessionId);
            await state.Cancellation.CancelAsync().ConfigureAwait(false);
            await QuiesceAsync(state, context.CancellationToken).ConfigureAwait(false);
            await using (state.Session.ConfigureAwait(false))
            {
                await state.Session.AbortAsync(context.CancellationToken).ConfigureAwait(false);
            }

            return new AbortResponse();
        });

    // ---- cross-cutting ----------------------------------------------------------------------

    public override async Task<CancelResponse> Cancel(CancelRequest request, ServerCallContext context)
    {
        if (_ops.TryGetValue(request.OpId, out var cts))
        {
            await cts.CancelAsync().ConfigureAwait(false);
        }

        // A write pump reads from the host, not from the op's read path, so the op token alone never
        // reaches it. Cancelling the op must stop its writes too.
        foreach (var session in _sessions.Values)
        {
            if (string.Equals(session.OpId, request.OpId, StringComparison.Ordinal))
            {
                await session.Cancellation.CancelAsync().ConfigureAwait(false);
            }
        }

        return new CancelResponse();
    }

    public override Task<ShutdownResponse> Shutdown(ShutdownRequest request, ServerCallContext context)
    {
        // Signal only: the host process stops gracefully after this response is on the wire, which is
        // what keeps Shutdown distinguishable from a crash on the host side.
        lifetime.StopApplication();
        return Task.FromResult(new ShutdownResponse());
    }

    public override async Task HostChannel(
        IAsyncStreamReader<HostChannelDown> requestStream,
        IServerStreamWriter<HostChannelUp> responseStream,
        ServerCallContext context)
    {
        // Without --use-gate, LocalFiles consumes no host service and this fixture holds the reverse
        // channel open and silent: draining it still matters, since the host's pump must see a
        // well-formed channel that ends only when the host ends it -- or when the fixture does.
        // Without the ApplicationStopping link this loop outlives a graceful shutdown and holds it open
        // until the host's shutdown timeout, which is precisely the Shutdown-inside-the-grace budget it
        // must not spend.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken, lifetime.ApplicationStopping);
        _gatePeer.Attach(responseStream);
        try
        {
            while (await requestStream.MoveNext(linked.Token).ConfigureAwait(false))
            {
                // The only HostChannelDown case is GateGrant -- see the proto's own comment on why the
                // host writes this half and the connector writes everything else.
                _gatePeer.OnGateGrant(requestStream.Current.GateGrant.RequestId);
            }
        }
        catch (OperationCanceledException)
        {
            // The host closed the channel, the call deadline passed, or the fixture is shutting down.
        }
        finally
        {
            _gatePeer.Detach();
        }
    }

    // ---- helpers ----------------------------------------------------------------------------

    private static IDatasetPartition ResolvePartition(PlannedRead plan, string partitionId)
    {
        foreach (var partition in plan.Partitions)
        {
            if (partition is IIdentifiedPartition identified &&
                string.Equals(identified.PartitionId, partitionId, StringComparison.Ordinal))
            {
                return partition;
            }
        }

        if (int.TryParse(partitionId, System.Globalization.CultureInfo.InvariantCulture, out var ordinal) &&
            ordinal >= 0 && ordinal < plan.Partitions.Count)
        {
            return plan.Partitions[ordinal];
        }

        throw new RpcException(new Status(StatusCode.NotFound, $"unknown partition '{partitionId}'"));
    }

    /// <summary>Closes a write session to any further data-plane pumping and waits for the pump that
    /// already claimed it, if any. After this returns, nothing is writing into the session, so
    /// Commit/Abort/dispose are safe.</summary>
    private static async Task QuiesceAsync(WriteSessionState state, CancellationToken ct)
    {
        if (state.Close() is { } pump)
        {
            await pump.WaitAsync(ct).ConfigureAwait(false);
        }
    }

    private WriteSessionState TakeSession(string sessionId) =>
        _sessions.TryRemove(sessionId, out var state)
            ? state
            : throw new RpcException(new Status(
                StatusCode.NotFound, $"unknown or already-finished write session '{sessionId}'"));

    /// <summary>The op-scoped token a later <c>Cancel {opId}</c> trips. It deliberately outlives the
    /// RPC that created it: a read ticket's stream is served long after OpenReadStream returned, and
    /// Cancel is the only thing that may stop it. Op state lives until the process exits — a fixture
    /// serves one run.</summary>
    private CancellationToken OpToken(string opId) =>
        _ops.GetOrAdd(opId, _ => new CancellationTokenSource()).Token;

    /// <summary>What a control RPC observes: its own call cancellation plus the op's.</summary>
    private CancellationTokenSource LinkOp(string opId, ServerCallContext context) =>
        CancellationTokenSource.CreateLinkedTokenSource(OpToken(opId), context.CancellationToken);

    private async ValueTask<ISource> OpenSourceAsync(CancellationToken ct)
    {
        if (_source is not null)
        {
            return _source;
        }

        await _openGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _source ??= await ((ISourceConnector)_connector).OpenAsync(RequireConfig(), ct).ConfigureAwait(false);
        }
        finally
        {
            _openGate.Release();
        }
    }

    private async ValueTask<ISink> OpenSinkAsync(CancellationToken ct)
    {
        if (_sink is not null)
        {
            return _sink;
        }

        await _openGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return _sink ??= await ((ISinkConnector)_connector).OpenAsync(RequireConfig(), ct).ConfigureAwait(false);
        }
        finally
        {
            _openGate.Release();
        }
    }

    private ConnectorConfig RequireConfig() =>
        _config ?? throw new RpcException(new Status(
            StatusCode.FailedPrecondition, "connector is not configured; call Configure first"));

    private static async Task<ByteString> SerializeSchemaAsync(Schema schema, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        using (var writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true))
        {
            await writer.WriteStartAsync(ct).ConfigureAwait(false);
            await writer.WriteEndAsync(ct).ConfigureAwait(false);
        }

        return ByteString.CopyFrom(buffer.ToArray());
    }

    private static async ValueTask<Schema> DeserializeSchemaAsync(ByteString bytes, CancellationToken ct)
    {
        using var buffer = new MemoryStream(bytes.ToByteArray(), writable: false);
        using var reader = new ArrowStreamReader(buffer, leaveOpen: true);
        return await reader.GetSchema(ct).ConfigureAwait(false);
    }

    private static async Task<T> Guarded<T>(Func<Task<T>> body)
    {
        try
        {
            return await body().ConfigureAwait(false);
        }
        catch (PzConnectorException ex)
        {
            throw ToRpcException(ex);
        }
    }

    /// <summary>Operational failures cross as an <c>RpcException</c> carrying a serialized
    /// <see cref="PzErrorDetail"/> in the trailers, so the host can rebuild a real
    /// <see cref="PzConnectorException"/> — transience and retry-after intact — instead of guessing
    /// from a status code.
    ///
    /// <para>The TRAILER is the contract, not the status code. The codes chosen here are only for
    /// readable logs; a host must decide by the trailer's presence, because the protocol-violation
    /// statuses this service raises elsewhere (no config, unknown partition, unknown session) carry no
    /// trailer and are a different failure entirely.</para></summary>
    private static RpcException ToRpcException(PzConnectorException ex)
    {
        var detail = new PzErrorDetail
        {
            Code = string.Empty,
            Message = ex.Message,
            IsTransient = ex.IsTransient,
            RetryAfterMs = (long)(ex.RetryAfter?.TotalMilliseconds ?? 0),
            Hint = string.Empty,
        };
        var trailers = new Metadata { { ProtocolConstants.ErrorDetailTrailerKey, detail.ToByteArray() } };
        var status = new Status(ex.IsTransient ? StatusCode.Unavailable : StatusCode.FailedPrecondition, ex.Message);
        return new RpcException(status, trailers);
    }
}

/// <summary>The connector side of the PCP reverse channel: the fixture's own client for the host's
/// operation gate. One instance per <see cref="PcpService"/> (a fixture serves one connector instance
/// for the process's lifetime), reused across however many <c>HostChannel</c> calls the host makes
/// (there is normally exactly one, held open for the process's lifetime).
///
/// <para>Single-attempt only: a real connector SDK wrapping <c>IOperationGate</c>'s idempotent retry
/// semantics would loop -- wait for another <c>GateGrant</c> on the SAME request id if the host decides
/// to retry a transient failure -- but this fixture never exercises host-driven retry (nothing here
/// fails transiently), so <see cref="RunGatedAsync"/> sends exactly one <c>GateAcquire</c>/
/// <c>GateComplete</c> pair per call. Tracked as a fixture limitation, not a protocol one.</para></summary>
internal sealed class HostChannelPeer
{
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _pendingGrants = new(StringComparer.Ordinal);
    private readonly Lock _attachGate = new();

    // The single buffer-until-attach mechanism for EVERY outgoing message (log or gate): a TCS that
    // resolves once a HostChannel call is actually being served. Replaced with a fresh, unresolved one
    // on Detach so a message queued after the channel drops waits for the NEXT attach rather than
    // racing a disposed writer. Before this, only QueueConfiguredLog buffered -- RunGatedAsync's sends
    // threw synchronously on a not-yet-attached channel, which left GatedReadPartition.ReadCoreAsync
    // awaiting `started` forever whenever a read was dispatched before the host's pump call landed.
    private TaskCompletionSource<IServerStreamWriter<HostChannelUp>> _attached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Called once per <c>HostChannel</c> RPC, right as it starts serving. Unblocks every send
    /// (log or gate) that was already waiting on this channel to open.</summary>
    public void Attach(IServerStreamWriter<HostChannelUp> writer)
    {
        TaskCompletionSource<IServerStreamWriter<HostChannelUp>> signal;
        lock (_attachGate)
        {
            signal = _attached;
        }

        signal.TrySetResult(writer);
    }

    public void Detach()
    {
        lock (_attachGate)
        {
            _attached = new TaskCompletionSource<IServerStreamWriter<HostChannelUp>>(
                TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    /// <summary>Sends once the channel is attached -- immediately if it already is, otherwise once
    /// <see cref="Attach"/> next runs. Configure always runs before the host's pump connects, so the
    /// buffered path is the one this fixture actually exercises for a Configure-time log.</summary>
    public void QueueConfiguredLog(LogEvent log) => _ = SendBestEffortAsync(new HostChannelUp { Log = log });

    public void OnGateGrant(string requestId)
    {
        if (_pendingGrants.TryRemove(requestId, out var tcs))
        {
            tcs.TrySetResult();
        }
    }

    /// <summary>Runs <paramref name="body"/> gated behind one <c>GateAcquire</c>/<c>GateGrant</c>/
    /// <c>GateComplete</c> round trip. A transient <see cref="PzConnectorException"/> from
    /// <paramref name="body"/> is reported in <c>GateComplete.transient_error</c> (the host-side gate op
    /// throws it back out, per the host's own <c>HostChannelPump</c>); any other failure still gets a
    /// (error-free) <c>GateComplete</c> sent -- so the host's pending gate wait always resolves -- before
    /// the original exception is rethrown.</summary>
    public async Task RunGatedAsync(string opLabel, Func<CancellationToken, Task> body, CancellationToken ct)
    {
        var requestId = Guid.NewGuid().ToString("n");
        var granted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingGrants[requestId] = granted;
        try
        {
            await SendAsync(new HostChannelUp
            {
                GateAcquire = new GateAcquire { RequestId = requestId, OpLabel = opLabel, Idempotent = false },
            }, ct).ConfigureAwait(false);

            await granted.Task.WaitAsync(ct).ConfigureAwait(false);

            Exception? failure = null;
            try
            {
                await body(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failure = ex;
            }

            var complete = new HostChannelUp { GateComplete = new GateComplete { RequestId = requestId } };
            if (failure is PzConnectorException { IsTransient: true } transient)
            {
                complete.GateComplete.TransientError = new PzErrorDetail
                {
                    Code = string.Empty,
                    Message = transient.Message,
                    IsTransient = true,
                    RetryAfterMs = (long)(transient.RetryAfter?.TotalMilliseconds ?? 0),
                    Hint = string.Empty,
                };
            }

            await SendAsync(complete, ct).ConfigureAwait(false);

            if (failure is not null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }
        finally
        {
            _pendingGrants.TryRemove(requestId, out _);
        }
    }

    private async Task SendAsync(HostChannelUp msg, CancellationToken ct)
    {
        TaskCompletionSource<IServerStreamWriter<HostChannelUp>> signal;
        lock (_attachGate)
        {
            signal = _attached;
        }

        // Buffer-until-attach, not a synchronous throw: a message queued (GateAcquire, or a log) before
        // HostChannel's server method has run Attach() waits here instead of failing the caller outright.
        var writer = await signal.Task.WaitAsync(ct).ConfigureAwait(false);

        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await writer.WriteAsync(msg).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async Task SendBestEffortAsync(HostChannelUp msg)
    {
        try
        {
            await SendAsync(msg, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Logging is best-effort: a channel that closed between Attach and this flush loses the
            // line, same as any other logging path racing a shutdown.
        }
    }
}

/// <summary>Wraps one already-planned partition so its ENTIRE drain (open through end-of-stream) runs
/// under one <see cref="HostChannelPeer.RunGatedAsync"/> call -- "one GateAcquire/GateComplete per
/// partition read". Batches still flow through as the inner partition produces them (no buffering): the
/// gate's own "op" is a start/finish handshake running concurrently with the actual drain below, not the
/// drain itself.</summary>
internal sealed class GatedReadPartition(IDatasetPartition inner, HostChannelPeer peer, string opLabel)
    : IDatasetPartition
{
    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        ReadCoreAsync(options, ct);

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gated = peer.RunGatedAsync(opLabel, async ct2 =>
        {
            started.TrySetResult();
            await finished.Task.WaitAsync(ct2).ConfigureAwait(false);
        }, ct);

        await started.Task.ConfigureAwait(false);

        var enumerator = inner.ReadAsync(options, ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                RecordBatch? batch;
                try
                {
                    batch = await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
                }
                catch (PzConnectorException ex) when (ex.IsTransient)
                {
                    finished.TrySetException(ex);
                    throw;
                }
                catch
                {
                    finished.TrySetResult();
                    throw;
                }

                if (batch is null)
                {
                    break;
                }

                yield return batch;
            }

            finished.TrySetResult();
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);

            // Unconditional, not just on the success path: RunGatedAsync must be joined whether the
            // drain above succeeded or threw, so GateComplete is always flushed and `gated` is always
            // observed -- an unawaited faulted Task here would otherwise go unobserved on every
            // exception path (the transient-retry path aside, where it carries the exact exception
            // already being rethrown from this same finally's enclosing try -- see JoinGatedAsync).
            await JoinGatedAsync(gated).ConfigureAwait(false);
        }
    }

    /// <summary>Awaits <paramref name="gated"/> purely to observe it -- never to surface a NEW failure
    /// from this <c>finally</c> block, which would replace whatever exception the read loop above is
    /// already propagating (or, on the transient-retry path, would just be that identical exception
    /// again, since RunGatedAsync rethrows the same instance it was handed). A failure here that is
    /// genuinely new (e.g. GateComplete itself failed to send on a torn channel) has no better place to
    /// go from a fixture's read path than being dropped after being observed.</summary>
    private static async Task JoinGatedAsync(Task gated)
    {
        try
        {
            await gated.ConfigureAwait(false);
        }
        catch
        {
            // See summary above.
        }
    }
}

/// <summary><c>google.protobuf.Struct</c> is the only shape configuration ever arrives in, and this is
/// the whole of its meaning: string, double, bool, null, list, nested map.</summary>
internal static class StructMapping
{
    public static IReadOnlyDictionary<string, object?> ToDictionary(Struct? value)
    {
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        if (value is null)
        {
            return result;
        }

        foreach (var (key, item) in value.Fields)
        {
            result[key] = ToObject(item);
        }

        return result;
    }

    private static object? ToObject(Value value) => value.KindCase switch
    {
        Value.KindOneofCase.NumberValue => value.NumberValue,
        Value.KindOneofCase.StringValue => value.StringValue,
        Value.KindOneofCase.BoolValue => value.BoolValue,
        Value.KindOneofCase.StructValue => ToNestedMap(value.StructValue),
        Value.KindOneofCase.ListValue => value.ListValue.Values.Select(ToObject).ToList(),
        _ => null,
    };

    /// <summary>A nested map arrives with no .NET type attached, and the ABI reads one of them two
    /// different ways: <c>columns:</c> is an <c>IReadOnlyDictionary&lt;string, string&gt;</c> in-proc
    /// (Pz.Engine's SpecBuilder puts the declared contract in the options bag under that type), while
    /// everything else is read as <c>object?</c>-valued. An all-string map is therefore rebuilt as a
    /// value that answers to both shapes rather than picking one and silently making the other
    /// option invisible to the connector.
    ///
    /// <para>ORDER IS LOAD-BEARING AND ONLY C# GUARANTEES IT. LocalFiles binds a <c>columns:</c>
    /// contract to the csv header BY POSITION, so the order the host declared must survive the wire.
    /// It does here because Google.Protobuf's <c>MapField</c> enumerates in insertion order on the C#
    /// side, and the rebuild below walks the fields in that order. That is an implementation property
    /// of one runtime, not a protobuf guarantee: proto3 map entries are explicitly unordered, and a
    /// peer in another language may hand its map back in any order at all. A connector that needs
    /// ordered columns cannot get them from a <c>Struct</c> — this is a spec-level hazard, tracked for
    /// the protocol follow-up rather than papered over here.</para></summary>
    private static object ToNestedMap(Struct value)
    {
        var plain = new Dictionary<string, object?>(StringComparer.Ordinal);
        var strings = new Dictionary<string, string>(StringComparer.Ordinal);
        var allStrings = value.Fields.Count > 0;
        foreach (var (key, item) in value.Fields)
        {
            plain[key] = ToObject(item);
            if (item.KindCase == Value.KindOneofCase.StringValue)
            {
                strings[key] = item.StringValue;
            }
            else
            {
                allStrings = false;
            }
        }

        return allStrings ? new StringValuedMap(strings) : plain;
    }
}

/// <summary>A nested option map whose values are all strings, readable either as
/// <c>IReadOnlyDictionary&lt;string, object?&gt;</c> (inherited) or as
/// <c>IReadOnlyDictionary&lt;string, string&gt;</c> (explicit). See
/// <c>StructMapping.ToNestedMap</c> for why both are needed.</summary>
internal sealed class StringValuedMap : Dictionary<string, object?>, IReadOnlyDictionary<string, string>
{
    private readonly Dictionary<string, string> _strings;

    public StringValuedMap(Dictionary<string, string> strings)
        : base(strings.Count, StringComparer.Ordinal)
    {
        _strings = strings;
        foreach (var (key, value) in strings)
        {
            Add(key, value);
        }
    }

    string IReadOnlyDictionary<string, string>.this[string key] => _strings[key];

    IEnumerable<string> IReadOnlyDictionary<string, string>.Keys => _strings.Keys;

    IEnumerable<string> IReadOnlyDictionary<string, string>.Values => _strings.Values;

    bool IReadOnlyDictionary<string, string>.TryGetValue(string key, out string value) =>
        _strings.TryGetValue(key, out value!);

    IEnumerator<KeyValuePair<string, string>> IEnumerable<KeyValuePair<string, string>>.GetEnumerator() =>
        _strings.GetEnumerator();
}

/// <summary>Wire spec messages to their ABI records. Every optional field is carried, including the
/// two "was it set" booleans that keep null distinguishable from empty.</summary>
internal static class SpecMapping
{
    public static DatasetSpec ToDatasetSpec(DatasetSpecMsg spec) =>
        new(spec.Source, spec.Dataset, StructMapping.ToDictionary(spec.Options))
        {
            WatermarkCursor = spec.HasWatermarkCursor ? spec.WatermarkCursor : null,
            WatermarkValue = spec.HasWatermarkValue ? spec.WatermarkValue : null,
            WatermarkUpperBound = spec.HasWatermarkUpperBound ? spec.WatermarkUpperBound : null,
            WatermarkLowerInclusive = spec.WatermarkLowerInclusive,
            PriorSyncState = spec.HasPriorSyncState ? spec.PriorSyncState : null,
            ChangeCapture = spec.ChangeCapture,
            ChangeCaptureSlot = spec.HasChangeCaptureSlot ? spec.ChangeCaptureSlot : null,
        };

    public static OutputSpec ToOutputSpec(OutputSpecMsg spec) =>
        new(spec.Sink, spec.Output, spec.Mode, spec.SchemaPolicy, StructMapping.ToDictionary(spec.Options))
        {
            Keys = spec.Keys.ToArray(),
            OnDelete = spec.HasOnDelete ? spec.OnDelete : null,
            MaxTextLengths = spec.MaxTextLengthsSet
                ? spec.MaxTextLengths.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
                : null,
            Attempt = spec.Attempt is { } attempt
                ? new WriteAttempt(attempt.Node, attempt.Run, attempt.Ordinal)
                : null,
        };

    public static ReadHints ToReadHints(ReadHintsMsg? hints) => hints is null
        ? ReadHints.None
        : new ReadHints(
            hints.ColumnsSet ? hints.Columns.ToArray() : null,
            hints.HasPredicateSql ? hints.PredicateSql : null,
            hints.HasLimit ? hints.Limit : null);

    public static BatchOptions ToBatchOptions(BatchOptionsMsg? options) => options is null
        ? BatchOptions.Default
        : new BatchOptions(
            options.TargetBatchBytes > 0 ? options.TargetBatchBytes : BatchOptions.Default.TargetBatchBytes,
            options.MaxRowsPerBatch > 0 ? options.MaxRowsPerBatch : BatchOptions.Default.MaxRowsPerBatch);

    public static AbortSemanticsMsg ToAbortSemanticsMsg(AbortSemantics semantics) => semantics switch
    {
        AbortSemantics.DiscardsAll => AbortSemanticsMsg.AbortSemanticsDiscardsAll,
        AbortSemantics.BestEffort => AbortSemanticsMsg.AbortSemanticsBestEffort,
        AbortSemantics.None => AbortSemanticsMsg.AbortSemanticsNone,
        _ => throw new ArgumentOutOfRangeException(nameof(semantics), semantics, "unrecognized AbortSemantics"),
    };
}
