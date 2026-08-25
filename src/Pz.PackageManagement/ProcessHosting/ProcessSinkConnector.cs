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

internal sealed class ProcessSink(PcpClient client, ConnectorProcess process) : ISink
{
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
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, CancellationToken.None);
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
        var request = new BeginWriteRequest
        {
            OpId = ProcessSource.NewOpId(),
            Spec = MessageMapping.ToOutputSpecMsg(spec),
            ArrowSchemaIpc = await MessageMapping.SerializeSchemaAsync(schema, ct).ConfigureAwait(false),
        };

        WriteSessionTicket ticket;
        try
        {
            ticket = await client.Grpc.BeginWriteAsync(request, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }

        _abortSemantics = MessageMapping.ToAbortSemantics(ticket.AbortSemantics);

        IDataPlaneWriter writer;
        try
        {
            writer = await DataPlane
                .OpenWriteStreamAsync(process.DataSocketPath, ticket.Ticket.Memory, schema, ct)
                .ConfigureAwait(false);
        }
        catch (ConnectorHostException ex)
        {
            throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
        }

        return new ProcessSinkWriteSession(client, process, ticket.SessionId, writer);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>One open write session: batches forward straight to the data-plane writer (serialized
/// inside <see cref="DataPlane.IDataPlaneWriter.WriteBatchAsync"/>, nothing retained here past the
/// call -- the ABI's "engine owns the batch until this call returns" rule holds unchanged for the
/// out-of-process path). Commit-at-most-once / never-abort-after-commit falls out of forwarding the
/// engine's own call ordering onto <see cref="CommitAsync"/>/<see cref="AbortAsync"/> -- nothing extra
/// is asserted or added here.</summary>
internal sealed class ProcessSinkWriteSession(
    PcpClient client, ConnectorProcess process, string sessionId, IDataPlaneWriter writer) : ISinkWriteSession
{
    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        try
        {
            await writer.WriteBatchAsync(batch, ct).ConfigureAwait(false);
        }
        catch (ConnectorHostException ex)
        {
            throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
        }
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        try
        {
            // Arrow EOS + half-close, THEN the RPC: CommitWrite (fixture and spec alike) blocks on
            // seeing end-of-stream before it will run, so this ordering is the precondition, not an
            // optimization.
            await writer.CompleteAsync(ct).ConfigureAwait(false);
        }
        catch (ConnectorHostException ex)
        {
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
