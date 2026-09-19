using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk;

/// <summary>The control plane of one connector process: every RPC of the PCP service mapped onto
/// the Abstractions connector it hosts. One instance per process; the configured ISource/ISink, the
/// open plans and the write sessions are process-wide state every later RPC resolves against.
///
/// <para>Optional behavior is answered from what the connector OBJECT implements, never assumed
/// from a declared capability: a source that is not INaturalReadShapeSource gets UNIMPLEMENTED, a
/// partition that is not ISyncStatePartition gets FAILED_PRECONDITION. A connector that declares a
/// capability it does not implement therefore fails `pz connector test`, which is the point.</para></summary>
internal sealed class PcpConnectorService(
    IConnector connector,
    TicketRegistry tickets,
    HostChannelPeer peer,
    ConnectorTelemetry telemetry,
    PcpServerHooks hooks,
    IHostApplicationLifetime lifetime) : PzConnector.PzConnectorBase, IDisposable
{
    private readonly SemaphoreSlim _openGate = new(1, 1);
    private readonly SemaphoreSlim _configureGate = new(1, 1);
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _ops = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, PlannedRead> _plans = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, WriteSessionState> _sessions = new(StringComparer.Ordinal);
    private readonly HostOperationGate _gate = new(peer);

    private volatile bool _handshaken;
    private ConnectorConfig? _config;
    private ISource? _source;
    private ISink? _sink;

    private sealed record PlannedRead(
        Schema Schema,
        IReadOnlyList<IDatasetPartition> Partitions,
        ConcurrentDictionary<string, SyncStateCapture> Captures,
        ConcurrentDictionary<string, StreamFailureCapture> Failures);

    // ---- identity + config ------------------------------------------------------------------

    public override async Task<Hello> Handshake(HandshakeRequest request, ServerCallContext context)
    {
        if (hooks.HangHandshake is { } hang)
        {
            await hang(context.CancellationToken).ConfigureAwait(false);
        }

        if (request.ProtocolMajor != connector.Info.ProtocolMajor)
        {
            // Refused here, not answered with our own major and left for the host to notice some
            // other way (the Rust SDK's server.rs does the same): a connector that sees the SAME
            // disagreement from its own side of the handshake should say so plainly too.
            throw new RpcException(new Status(
                StatusCode.FailedPrecondition,
                $"host speaks protocol major {request.ProtocolMajor}, this connector ({SdkInfo.Name}) speaks major {connector.Info.ProtocolMajor}"));
        }

        var hello = new Hello
        {
            Info = new ConnectorInfoMsg
            {
                Name = connector.Info.Name,
                Version = connector.Info.Version,
                ProtocolMajor = connector.Info.ProtocolMajor,
            },
            Capabilities = (long)ManifestWriter.DeclaredCapabilities(connector),
            ConnectionConfigSchema = connector.ConnectionConfigSchema,
            DatasetConfigSchema = connector.DatasetConfigSchema,
            // Empty when the connector does not implement the optional capability -- the host treats
            // that the same as a connector built before this field existed.
            OutputConfigSchema = connector is IOutputConfigSchema outputSchema ? outputSchema.OutputConfigSchema : "",
            Sdk = new SdkInfoMsg { Name = SdkInfo.Name, Version = SdkInfo.Version },
        };
        hello.Transports.Add(ProtocolConstants.TransportPipe);

        // The endpoint is the host's own; absent means the host is not exporting either. Built here
        // rather than at Configure so Validate/CheckConnection (which `pz connector test` calls
        // without Configure) are covered too.
        if (request.HostInfo is { HasOtelEndpoint: true } hostInfo)
        {
            telemetry.Start(hostInfo.OtelEndpoint, connector.Info, hostInfo.RunId);
        }

        _handshaken = true;
        return hello;
    }

    public override async Task<ConfigureResponse> Configure(ConfigureRequest request, ServerCallContext context)
    {
        if (!_handshaken)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Handshake must precede Configure"));
        }

        // The "already configured" check and the config write are one critical section: left apart,
        // two Configure calls racing the check both see _config as null and both proceed.
        await _configureGate.WaitAsync(context.CancellationToken).ConfigureAwait(false);
        try
        {
            if (_config is not null)
            {
                throw new RpcException(new Status(StatusCode.FailedPrecondition, "connector is already configured"));
            }

            if (hooks.PauseInsideConfigure is { } pause)
            {
                // Between the check and the write: a test that holds a call here and starts a SECOND
                // one proves the gate, not the ordering of the two statements below, is what keeps
                // them from both observing _config as null.
                await pause(context.CancellationToken).ConfigureAwait(false);
            }

            _config = new ConnectorConfig(StructMapping.ToDictionary(request.Config));
            telemetry.InstanceId = request.InstanceId;
            hooks.OnConfigure?.Invoke();

            // One log event per Configure, always -- fields carry the connection NAME (instance_id) and
            // the connector's own identity, never a config VALUE. The reverse channel is not open yet at
            // this point in the RPC sequence, so this is held and flushed the moment HostChannel attaches.
            var configured = new LogEvent { Level = (int)LogLevel.Information, Message = "connector configured" };
            configured.Fields["instance_id"] = request.InstanceId;
            configured.Fields["connector"] = connector.Info.Name;
            peer.QueueLog(configured);
        }
        finally
        {
            _configureGate.Release();
        }

        return new ConfigureResponse();
    }

    public override Task<ValidationResultMsg> Validate(ValidateRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            var config = StructMapping.ToDictionary(request.Config);
            if (ConformanceProbe.TryAnswerNumericOption(config, out var probeErrors))
            {
                var answer = new ValidationResultMsg();
                answer.Errors.AddRange(probeErrors);
                return answer;
            }

            var result = await connector
                .ValidateAsync(new ConnectorConfig(config), context.CancellationToken)
                .ConfigureAwait(false);
            var message = new ValidationResultMsg();
            message.Errors.AddRange(result.Errors);
            message.Warnings.AddRange(result.Warnings);
            return message;
        });

    public override Task<ConnectionCheckMsg> CheckConnection(CheckRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            var check = await connector
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
            return new DatasetSchemaMsg { ArrowSchemaIpc = await SchemaCodec.SerializeAsync(schema.Schema, ct).ConfigureAwait(false) };
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
            var hints = SpecMapping.ToReadHints(request.Hints);
            var source = await OpenSourceAsync(ct).ConfigureAwait(false);
            var partitions = await source.PlanReadAsync(spec, hints, ct).ConfigureAwait(false);
            var declared = await source.GetSchemaAsync(spec, ct).ConfigureAwait(false);
            // The plan is what every later RPC resolves against: the SAME partition instances the
            // PartitionMsg flags below were computed from are what OpenReadStream mints tickets for and
            // what GetReadState answers about. Its schema is the shape the batches will actually have --
            // the declared schema narrowed to the pruning hint -- because that is what the data plane
            // writes as the stream header.
            _plans[request.OpId] = new PlannedRead(
                ReadSchemaProjection.Apply(declared.Schema, hints, connector.Capabilities),
                partitions, new ConcurrentDictionary<string, SyncStateCapture>(StringComparer.Ordinal),
                new ConcurrentDictionary<string, StreamFailureCapture>(StringComparer.Ordinal));

            for (var i = 0; i < partitions.Count; i++)
            {
                ct.ThrowIfCancellationRequested();
                var partition = partitions[i];
                await responseStream.WriteAsync(new PartitionMsg
                {
                    PartitionId = PartitionIdOf(partition, i),
                    Checkpointing = partition is ICheckpointingPartition,
                    SyncState = partition is ISyncStatePartition,
                }).ConfigureAwait(false);
            }
        }
        catch (PzConnectorException ex)
        {
            throw ToRpcException(ex);
        }
        catch (Exception ex) when (ex is not RpcException and not OperationCanceledException)
        {
            throw ToUnhandledRpcException(ex);
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

            var ticket = tickets.Mint(new ReadTicket(
                plan.Schema,
                partition,
                SpecMapping.ToBatchOptions(request.Options),
                OpToken(request.OpId),
                // A fresh capture per stream, never GetOrAdd: a partition reopened after a completed
                // attempt (a retry, or a host that reads it again) must not answer GetReadState with
                // the PREVIOUS attempt's already-completed token before its own drain has even
                // started. Same rule Failures already followed on the line below.
                plan.Captures[request.PartitionId] = new SyncStateCapture(),
                plan.Failures[request.PartitionId] = new StreamFailureCapture(),
                Activity.Current?.Context ?? default));
            return Task.FromResult(new ReadStreamTicket { Ticket = ByteString.CopyFrom(ticket) });
        });

    public override Task<NaturalReadShapeResponse> GetNaturalReadShape(
        NaturalReadShapeRequest request, ServerCallContext context) =>
        Guarded(async () =>
        {
            using var linked = LinkOp(request.OpId, context);
            var source = await OpenSourceAsync(linked.Token).ConfigureAwait(false);
            if (source is not INaturalReadShapeSource natural)
            {
                throw new RpcException(new Status(
                    StatusCode.Unimplemented, "this connector's source does not implement INaturalReadShapeSource"));
            }

            return new NaturalReadShapeResponse
            {
                Shape = natural.GetNaturalReadShape(SpecMapping.ToDatasetSpec(request.Spec)) == NaturalReadShape.Feed
                    ? NaturalReadShapeResponse.Types.Shape.Feed
                    : NaturalReadShapeResponse.Types.Shape.Full,
            };
        });

    public override Task<ReadStateResponse> GetReadState(ReadStateRequest request, ServerCallContext context) =>
        Guarded(() =>
        {
            if (!_plans.TryGetValue(request.OpId, out var plan))
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"no plan for op '{request.OpId}'"));
            }

            // Same lookup OpenReadStream uses, so the two RPCs agree on what a partition id means.
            var partition = ResolvePartition(plan, request.PartitionId);
            if (partition is not ISyncStatePartition)
            {
                throw new RpcException(new Status(
                    StatusCode.FailedPrecondition, $"partition '{request.PartitionId}' did not declare sync_state"));
            }

            // Answered from what the data plane captured at end-of-stream; the partition is never
            // polled here, so a token can neither appear early nor change after the drain.
            var response = new ReadStateResponse();
            if (plan.Captures.TryGetValue(request.PartitionId, out var capture) && capture.TryGet(out var token))
            {
                response.Token = token;
            }

            return Task.FromResult(response);
        });

    /// <summary>Answers from what the data plane recorded; it never touches the partition or the
    /// session, so it can neither commit nor free anything, and an op, partition or session this
    /// service no longer knows is simply "no failure known".</summary>
    public override Task<StreamFailureResponse> GetStreamFailure(
        StreamFailureRequest request, ServerCallContext context)
    {
        var failure = request.StreamCase switch
        {
            StreamFailureRequest.StreamOneofCase.Read =>
                _plans.TryGetValue(request.Read.OpId, out var plan) &&
                plan.Failures.TryGetValue(request.Read.PartitionId, out var capture)
                    ? capture.Failure
                    : null,
            // A write's failure is what faulted its Drained gate: the sink's own exception, raised
            // from the pump. The session is still registered here because a failed write is aborted,
            // not committed, and the host asks before it aborts.
            StreamFailureRequest.StreamOneofCase.Write =>
                _sessions.TryGetValue(request.Write.SessionId, out var state) &&
                state.Drained.Task is { IsFaulted: true } drained
                    ? drained.Exception.InnerException as PzConnectorException
                    : null,
            _ => null,
        };

        var response = new StreamFailureResponse();
        if (failure is not null)
        {
            response.Failure = ToErrorDetail(failure);
        }

        return Task.FromResult(response);
    }

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
            var schema = await SchemaCodec.DeserializeAsync(request.ArrowSchemaIpc, ct).ConfigureAwait(false);
            var session = await sink
                .BeginWriteAsync(SpecMapping.ToOutputSpec(request.Spec), schema, ct)
                .ConfigureAwait(false);
            var state = new WriteSessionState(Guid.NewGuid().ToString("n"), request.OpId, session);
            _sessions[state.SessionId] = state;
            return new WriteSessionTicket
            {
                SessionId = state.SessionId,
                Ticket = ByteString.CopyFrom(tickets.Mint(new WriteTicket(state, Activity.Current?.Context ?? default))),
                // Without this every PCP sink would look like DiscardsAll to the host, whatever it
                // actually wraps -- the sink's own declaration crosses verbatim.
                AbortSemantics = SpecMapping.ToAbortSemanticsMsg(sink.AbortSemantics),
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
        if (hooks.IgnoreCancel)
        {
            // Never answers and never stops the op: the host's ladder must escalate on a deadline of
            // its own rather than trusting a connector to acknowledge a cancel.
            await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken).ConfigureAwait(false);
        }

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
        if (hooks.IgnoreShutdown)
        {
            // Acknowledges and then keeps running: the host must fall through to the kill rung after
            // its shutdown grace instead of trusting the acknowledgement.
            return Task.FromResult(new ShutdownResponse());
        }

        // Signal only: the process stops gracefully after this response is on the wire, which is what
        // keeps Shutdown distinguishable from a crash on the host side.
        lifetime.StopApplication();
        return Task.FromResult(new ShutdownResponse());
    }

    public override async Task HostChannel(
        IAsyncStreamReader<HostChannelDown> requestStream,
        IServerStreamWriter<HostChannelUp> responseStream,
        ServerCallContext context)
    {
        // Draining the channel matters even for a connector that consumes no host service: the host's
        // pump must see a well-formed channel that ends only when the host ends it -- or when this
        // process does. Without the ApplicationStopping link this loop outlives a graceful shutdown and
        // holds it open until the shutdown timeout, which is precisely the Shutdown-inside-the-grace
        // budget it must not spend.
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken, lifetime.ApplicationStopping);
        peer.Attach(responseStream);
        try
        {
            while (await requestStream.MoveNext(linked.Token).ConfigureAwait(false))
            {
                // The only HostChannelDown case is GateGrant -- see the proto's own comment on why the
                // host writes this half and the connector writes everything else.
                peer.OnGateGrant(requestStream.Current.GateGrant.RequestId);
            }
        }
        catch (OperationCanceledException)
        {
            // The host closed the channel, the call deadline passed, or this process is shutting down.
        }
        finally
        {
            peer.Detach();
        }
    }

    // ---- helpers ----------------------------------------------------------------------------

    /// <summary>Ordinals stand in wherever the connector declares no stable id, which is exactly the
    /// host-side rule: an id is meaningful only under StablePartitionIds.</summary>
    private static string PartitionIdOf(IDatasetPartition partition, int ordinal) =>
        partition is IIdentifiedPartition identified
            ? identified.PartitionId
            : ordinal.ToString(CultureInfo.InvariantCulture);

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

        if (int.TryParse(partitionId, CultureInfo.InvariantCulture, out var ordinal) &&
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
    /// Cancel is the only thing that may stop it. Op state lives until the process exits — a connector
    /// process serves one run.</summary>
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

        if (connector is not ISourceConnector sourceConnector)
        {
            throw new RpcException(new Status(StatusCode.Unimplemented, "this connector has no source side"));
        }

        await _openGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_source is not null)
            {
                return _source;
            }

            // The handover happens only on the branch that actually opened the source: the ABI gives
            // an opened ISource exactly one UseOperationGate, and two RPCs racing the first open both
            // pass the unlocked fast path above.
            var opened = await sourceConnector.OpenAsync(RequireConfig(), ct).ConfigureAwait(false);
            if (opened is IOperationGateAware aware)
            {
                aware.UseOperationGate(_gate);
            }

            _source = opened;
            return opened;
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

        if (connector is not ISinkConnector sinkConnector)
        {
            throw new RpcException(new Status(StatusCode.Unimplemented, "this connector has no sink side"));
        }

        await _openGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_sink is not null)
            {
                return _sink;
            }

            // One UseOperationGate per opened ISink, for the same reason as the source path above.
            var opened = await sinkConnector.OpenAsync(RequireConfig(), ct).ConfigureAwait(false);
            if (opened is IOperationGateAware aware)
            {
                aware.UseOperationGate(_gate);
            }

            _sink = opened;
            return opened;
        }
        finally
        {
            _openGate.Release();
        }
    }

    private ConnectorConfig RequireConfig() =>
        _config ?? throw new RpcException(new Status(
            StatusCode.FailedPrecondition, "connector is not configured; call Configure first"));

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
        catch (Exception ex) when (ex is not RpcException and not OperationCanceledException)
        {
            throw ToUnhandledRpcException(ex);
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
    private static PzErrorDetail ToErrorDetail(PzConnectorException ex) => new()
    {
        Code = ex.Code ?? string.Empty,
        Message = ex.Message,
        IsTransient = ex.IsTransient,
        RetryAfterMs = (long)(ex.RetryAfter?.TotalMilliseconds ?? 0),
        Hint = ex.Hint ?? string.Empty,
    };

    private static RpcException ToRpcException(PzConnectorException ex)
    {
        var detail = ToErrorDetail(ex);
        var trailers = new Metadata { { ProtocolConstants.ErrorDetailTrailerKey, detail.ToByteArray() } };
        var status = new Status(ex.IsTransient ? StatusCode.Unavailable : StatusCode.FailedPrecondition, ex.Message);
        return new RpcException(status, trailers);
    }

    /// <summary>A handler bug (a connector's own unhandled exception, not one it raised through
    /// <see cref="PzConnectorException"/>) still crosses as an operational failure rather than being
    /// left for gRPC's default mapping -- an uncaught exception in an ASP.NET Core gRPC service method
    /// surfaces to the caller as bare <c>StatusCode.Unknown</c> with no trailer, which
    /// <c>PcpClient.MapRpcException</c> cannot tell apart from a genuine protocol violation (a
    /// malformed call, an ABI mismatch) and reports as PZ0357 -- the wrong diagnosis for what is really
    /// a connector defect. Routing it through <see cref="ToRpcException"/> gives it the trailer that
    /// makes the host reconstruct a real, non-transient <see cref="PzConnectorException"/> instead.</summary>
    private static RpcException ToUnhandledRpcException(Exception ex) =>
        ToRpcException(new PzConnectorException($"unhandled {ex.GetType().Name}: {ex.Message}", isTransient: false));

    // ---- lifetime -----------------------------------------------------------------------------

    /// <summary>Registered as a singleton (<c>PcpServer.ServeAsync</c>), so the DI container calls this
    /// exactly once, when the <c>WebApplication</c> it belongs to is disposed at the end of that
    /// method -- i.e. at process shutdown, matching <see cref="OpToken"/>'s own "op state lives until
    /// the process exits" rule: nothing here is torn down mid-run, only at the point where nothing can
    /// reference it again. Releases every per-op <see cref="CancellationTokenSource"/> <see cref="_ops"/>
    /// accumulated (one per op id for the life of the process, never freed individually since an op's
    /// true end -- its data-plane streams draining -- is not observable from the control plane alone)
    /// and drops every planned read, neither of which was ever otherwise reclaimed.</summary>
    public void Dispose()
    {
        foreach (var cts in _ops.Values)
        {
            cts.Dispose();
        }

        _ops.Clear();
        _plans.Clear();
    }

    /// <summary>Test seam: <see cref="Dispose"/> is proven by observing these, not by inspection.</summary>
    internal int TrackedOperationCount => _ops.Count;

    /// <summary>Test seam, see <see cref="TrackedOperationCount"/>.</summary>
    internal int TrackedPlanCount => _plans.Count;

    /// <summary>Test seam, see <see cref="TrackedOperationCount"/>: hands back the actual
    /// <see cref="CancellationTokenSource"/> tracked for <paramref name="opId"/> so a test can assert
    /// it was disposed, not merely removed.</summary>
    internal bool TryGetOpCancellationSource(string opId, out CancellationTokenSource cts) =>
        _ops.TryGetValue(opId, out cts!);
}
