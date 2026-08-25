using Apache.Arrow;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The <see cref="ISinkConnector"/> shim over one PCP connector instance -- the write-side
/// twin of <see cref="ProcessSourceConnector"/>; see its doc for the shared construction/ownership
/// discipline (already-connected client, no RPC of its own in <see cref="OpenAsync"/>).</summary>
public sealed class ProcessSinkConnector(PcpClient client, ConnectorProcess process) : ISinkConnector
{
    private readonly ProcessConnectorCore _core = new(client, process);

    public ConnectorInfo Info => _core.Info;

    public ConnectorCapabilities Capabilities => _core.Capabilities;

    public string ConnectionConfigSchema => _core.ConnectionConfigSchema;

    public string DatasetConfigSchema => _core.DatasetConfigSchema;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        _core.ValidateAsync(config, ct);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        _core.CheckConnectionAsync(config, ct);

    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISink>(new ProcessSink(client, process));
}

internal sealed class ProcessSink(PcpClient client, ConnectorProcess process) : ISink, IOperationGateAware, IGatedShim
{
    /// <summary>See <see cref="ProcessSource.Gate"/>'s identical doc: held for whoever constructs this
    /// shim's <see cref="HostChannelPump"/> to read, not used to wrap any RPC this shim itself makes.</summary>
    public IOperationGate? Gate { get; private set; }

    public void UseOperationGate(IOperationGate gate) => Gate = gate;

    // ISink.AbortSemantics is per-sink in the ABI but genuinely only knowable once a write session's
    // ticket comes back (WriteSessionTicket.abort_semantics carries the wrapped sink's own
    // declaration -- see pz_connector.proto). DiscardsAll (the ABI's own default) until a session has
    // actually opened; the engine only ever reads this after BeginWriteAsync in practice
    // (SinkWriteExecutor reads it from inside a write-failure handler), so this is never stale when it
    // matters.
    private AbortSemantics _abortSemantics = AbortSemantics.DiscardsAll;

    public AbortSemantics AbortSemantics => _abortSemantics;

    public bool TryGetNativeCopy(
        OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        var request = new NativeCopyRequest { OpId = ProcessSource.NewOpId(), Spec = MessageMapping.ToOutputSpecMsg(spec) };
        using var cts = new CancellationTokenSource(ProcessFailureMapping.NativeOperationTimeout);
        NativeCopyResponse response;
        try
        {
            // Mirrors ProcessSource.TryGetNativeScan: ISink.TryGetNativeCopy is synchronous in the ABI,
            // so an internal deadline stands in for a caller CancellationToken.
            using var call = client.Grpc.TryNativeCopyAsync(request, cancellationToken: cts.Token);
            response = call.ResponseAsync.GetAwaiter().GetResult();
        }
        catch (RpcException ex)
        {
            // See ProcessSource.TryGetNativeScan's identical guard: the internal deadline is the only
            // source of cancellation on this call, so it must not fall through to MapControlPlane's
            // PZ0357 "protocol violation" -- that's the wrong diagnosis for a connector that is merely
            // slow to answer.
            throw cts.IsCancellationRequested
                ? ProcessFailureMapping.NativeOperationTimedOut("TryNativeCopy")
                : ProcessFailureMapping.MapControlPlane(client, process, ex, CancellationToken.None);
        }

        if (!response.Found)
        {
            copy = null;
            return false;
        }

        copy = MessageMapping.ToNativeCopy(response);
        return true;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var opId = ProcessSource.NewOpId();
        var request = new BeginWriteRequest
        {
            OpId = opId,
            Spec = MessageMapping.ToOutputSpecMsg(spec),
            ArrowSchemaIpc = await MessageMapping.SerializeSchemaAsync(schema, ct).ConfigureAwait(false),
        };

        WriteSessionTicket ticket;
        using (client.AttachCancelLadder(opId, ct))
        {
            try
            {
                ticket = await client.Grpc.BeginWriteAsync(request, cancellationToken: ct).ConfigureAwait(false);
            }
            catch (RpcException ex)
            {
                throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
            }
        }

        _abortSemantics = MessageMapping.ToAbortSemantics(ticket.AbortSemantics);

        IDataPlaneWriter writer;
        try
        {
            writer = await DataPlane
                .OpenWriteStreamAsync(process.DataSocketPath, ticket.Ticket.Memory, schema, ct)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Same abandoned session as below, reached by a different route: the caller cancelled
            // between BeginWrite succeeding connector-side and the writer existing here. Cleanup must
            // not be skipped just because the failure was a cancellation, but the cancellation itself
            // still has to reach the caller unchanged.
            await TryAbortAbandonedSessionAsync(ticket.SessionId).ConfigureAwait(false);
            throw;
        }
        catch (ConnectorHostException ex)
        {
            // BeginWrite already succeeded connector-side -- the session exists there, keyed by
            // ticket.SessionId -- but the local data-plane connect failed before any ISinkWriteSession
            // came back for the engine to call AbortAsync on. Nobody else will ever tell the connector
            // to clean this session up, so this is the one chance to.
            await TryAbortAbandonedSessionAsync(ticket.SessionId).ConfigureAwait(false);
            throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
        }

        return new ProcessSinkWriteSession(client, process, opId, ticket.SessionId, writer);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>Best-effort only: the <see cref="ConnectorHostException"/> already caught by the caller
    /// is the failure that matters, and swallowing whatever this does here (including the connector
    /// having already reaped an abandoned session on its own) keeps that the only thing reported.
    /// CancellationToken.None mirrors SinkWriteExecutor's own post-failure abort -- cleanup must not be
    /// skipped just because the token that caused the failure was itself cancelled.</summary>
    private async ValueTask TryAbortAbandonedSessionAsync(string sessionId)
    {
        try
        {
            await client.Grpc
                .AbortWriteAsync(new SessionRef { SessionId = sessionId }, cancellationToken: CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // best-effort; see summary above
        }
    }
}

/// <summary>One open write session: batches forward straight to the data-plane writer (serialized
/// inside <see cref="DataPlane.IDataPlaneWriter.WriteBatchAsync"/>, nothing retained here past the
/// call -- the ABI's "engine owns the batch until this call returns" rule holds unchanged for the
/// out-of-process path). Commit-at-most-once / never-abort-after-commit falls out of forwarding the
/// engine's own call ordering onto <see cref="CommitAsync"/>/<see cref="AbortAsync"/> -- nothing extra
/// is asserted or added here.</summary>
internal sealed class ProcessSinkWriteSession(
    PcpClient client, ConnectorProcess process, string opId, string sessionId, IDataPlaneWriter writer)
    : ISinkWriteSession
{
    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        // The session's op id, not a fresh one: the connector's write pump reads from the host rather
        // than from the op's own path, so Cancel{opId} is the only thing that reaches it.
        using var ladder = client.AttachCancelLadder(opId, ct);
        try
        {
            await writer.WriteBatchAsync(batch, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // DataPlaneWriter.WriteBatchAsync wraps nothing of its own -- a connector killed mid-write
            // surfaces as a raw broken-pipe IOException/SocketException off the underlying socket, not
            // a ConnectorHostException. The ABI still promises PzConnectorException here (transient,
            // stderr tail) regardless of which concrete exception the socket happened to throw.
            throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        using var ladder = client.AttachCancelLadder(opId, ct);
        try
        {
            // Arrow EOS + half-close, THEN the RPC: CommitWrite (fixture and spec alike) blocks on
            // seeing end-of-stream before it will run, so this ordering is the precondition, not an
            // optimization.
            await writer.CompleteAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Same gap as WriteBatchAsync above: CompleteAsync's WriteEndAsync/socket shutdown can
            // throw a raw IOException/SocketException on a dead connector, not a ConnectorHostException.
            throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
        }

        try
        {
            var response = await client.Grpc
                .CommitWriteAsync(new SessionRef { SessionId = sessionId }, cancellationToken: ct)
                .ConfigureAwait(false);
            return MessageMapping.ToWriteResult(response);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        // No cancel ladder here, unlike every other session call: abort IS the cleanup path, and
        // escalating a cancelled abort to a process kill would take away the connector's chance to
        // finish the best-effort work AbortSemantics promised the engine.
        //
        // Abandon the data socket first (no CompleteAsync -- no Arrow EOS: the drain AbortWrite would
        // otherwise wait for was never coming), then AbortWrite, which cancels whatever pump is
        // mid-flight connector-side rather than waiting on a drain.
        await writer.DisposeAsync().ConfigureAwait(false);
        try
        {
            await client.Grpc.AbortWriteAsync(new SessionRef { SessionId = sessionId }, cancellationToken: ct)
                .ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
    }

    public ValueTask DisposeAsync() => writer.DisposeAsync();
}
