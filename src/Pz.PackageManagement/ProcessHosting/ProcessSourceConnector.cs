using System.Runtime.CompilerServices;
using Apache.Arrow;
using Grpc.Core;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The <see cref="ISourceConnector"/> shim over one PCP connector instance: every method here
/// forwards to a control-plane RPC (via <paramref name="client"/>'s <see cref="PcpClient.Grpc"/>) or a
/// data-plane socket (<see cref="DataPlane"/>) against <paramref name="process"/>. Constructed from an
/// already-connected, already-Configure()'d client (<c>PcpClient.ConnectAndConfigureAsync</c>)
/// -- <see cref="OpenAsync"/> makes no RPC of its own, since PCP has no separate "open" call: the
/// wrapped connector opens lazily, connector-side, on its first source-specific RPC.</summary>
public sealed class ProcessSourceConnector(PcpClient client, ConnectorProcess process) : ISourceConnector
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

    // `config` is the same connection config the client was already Configure()'d with (the engine
    // calls OpenAsync with `def.Source.Connection` every time, exactly what built the ConnectorConfig
    // ConnectAndConfigureAsync sent) -- there is nothing left to send here.
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult<ISource>(new ProcessSource(client, process));
}

internal sealed class ProcessSource(PcpClient client, ConnectorProcess process) : ISource, IOperationGateAware, IGatedShim
{
    /// <summary>Holds whatever gate the engine hands this instance -- see
    /// <see cref="IOperationGateAware"/>'s own contract (called once, after <c>OpenAsync</c> returns,
    /// before any plan/read call). Nothing in this shim wraps its OWN RPC calls in it: the gated
    /// operations here are the CONNECTOR's own remote calls, reported over the reverse channel and
    /// serviced by <see cref="HostChannelPump"/>, which is what actually reads this property.</summary>
    // Written by the engine thread that calls UseOperationGate, read by the HostChannelPump's pump loop
    // on a different thread -- same reasoning as PcpClient._lastErrorTransient: this handover needs a
    // real memory barrier, not a plain auto-property.
    private IOperationGate? _gate;

    public IOperationGate? Gate => Volatile.Read(ref _gate);

    public void UseOperationGate(IOperationGate gate) => Volatile.Write(ref _gate, gate);

    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var opId = NewOpId();
        var request = new GetSchemaRequest { OpId = opId, Spec = MessageMapping.ToDatasetSpecMsg(spec) };
        using var ladder = client.AttachCancelLadder(opId, ct);
        try
        {
            var response = await client.Grpc.GetSchemaAsync(request, cancellationToken: ct).ConfigureAwait(false);
            // Deserialization stays inside this try: a malformed arrow_schema_ipc payload is just as
            // much an operational failure as the RPC itself, and must cross the ABI boundary as
            // PzConnectorException, not a raw Apache.Arrow/IO exception.
            var schema = await MessageMapping.DeserializeSchemaAsync(response.ArrowSchemaIpc, ct).ConfigureAwait(false);
            return new DatasetSchema(schema);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
        catch (ObjectDisposedException)
        {
            // Ahead of the catch-all below, which would otherwise blame the connector's payload for a
            // channel a sibling operation's ladder disposed.
            throw ProcessFailureMapping.Condemned(process);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw ProcessFailureMapping.ToPzConnectorException(
                client, process, $"malformed schema from connector: {ex.Message}");
        }
    }

    public bool TryGetNativeScan(
        DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        // No cancel ladder: this call carries no caller token to escalate on, and its own deadline
        // firing already surfaces as a transient PzConnectorException. Condemning the whole instance
        // because a planner probe was slow would be a far bigger hammer than that failure warrants.
        var request = new NativeScanRequest { OpId = NewOpId(), Spec = MessageMapping.ToDatasetSpecMsg(spec) };
        using var cts = new CancellationTokenSource(ProcessFailureMapping.NativeOperationTimeout);
        NativeScanResponse response;
        try
        {
            // ISource.TryGetNativeScan is synchronous in the ABI (no CancellationToken parameter to
            // forward): the planner calls this once per edge, off the hot path, so blocking briefly
            // under an internal deadline is the documented cost of keeping this call's signature
            // synchronous end to end rather than threading a fake token through it.
            using var call = client.Grpc.TryNativeScanAsync(request, cancellationToken: cts.Token);
            response = call.ResponseAsync.GetAwaiter().GetResult();
        }
        catch (RpcException ex)
        {
            // The internal deadline firing looks identical to any other cancelled call to
            // MapControlPlane (ct is CancellationToken.None here, so IsCallerCancellation never
            // matches, and there is no error trailer for a client-side cancel) -- it would otherwise
            // fall through to PZ0357 "protocol violation", which is wrong for a connector that is
            // merely slow, not broken.
            throw cts.IsCancellationRequested
                ? ProcessFailureMapping.NativeOperationTimedOut("TryNativeScan")
                : ProcessFailureMapping.MapControlPlane(client, process, ex, CancellationToken.None);
        }
        catch (ObjectDisposedException)
        {
            throw ProcessFailureMapping.Condemned(process);
        }

        if (!response.Found)
        {
            scan = null;
            return false;
        }

        scan = MessageMapping.ToNativeScan(response);
        return true;
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(
        DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var opId = NewOpId();
        var request = new PlanReadRequest
        {
            OpId = opId,
            Spec = MessageMapping.ToDatasetSpecMsg(spec),
            Hints = MessageMapping.ToReadHintsMsg(hints),
        };

        // Every partition implements IIdentifiedPartition, or none do -- StablePartitionIds is a
        // connector-wide declaration (Hello.Capabilities), not a per-partition choice.
        var stableIds = client.Hello.Capabilities.HasCapability(ConnectorCapabilities.StablePartitionIds);
        var partitions = new List<IDatasetPartition>();
        using var ladder = client.AttachCancelLadder(opId, ct);
        try
        {
            using var call = client.Grpc.PlanRead(request, cancellationToken: ct);
            await foreach (var partition in call.ResponseStream.ReadAllAsync(ct).ConfigureAwait(false))
            {
                partitions.Add(stableIds
                    ? new ProcessIdentifiedPartition(client, process, opId, partition.PartitionId)
                    : new ProcessPartition(client, process, opId, partition.PartitionId));
            }
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
        catch (ObjectDisposedException)
        {
            throw ProcessFailureMapping.Condemned(process);
        }

        return partitions;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    internal static string NewOpId() => Guid.NewGuid().ToString("n");
}

/// <summary>One planned partition: <see cref="ReadAsync"/> opens its own data-plane stream via
/// <c>OpenReadStream</c> + <see cref="DataPlane.ReadStreamAsync"/>, reusing the <c>op_id</c> the plan
/// was made under (the connector resolves <c>OpenReadStream</c>'s partition id against that same op's
/// plan, per the fixture's <c>PlannedRead</c> lookup).</summary>
internal sealed class ProcessPartition(PcpClient client, ConnectorProcess process, string opId, string partitionId)
    : IDatasetPartition
{
    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        ReadCoreAsync(options, ct);

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        // Armed for the WHOLE drain, not just the OpenReadStream RPC: a partition read is the one
        // operation long enough for a caller to cancel mid-flight, and the connector only learns of it
        // through Cancel{opId} -- the host tearing down its end of the data socket is not a signal the
        // connector is required to interpret.
        using var ladder = client.AttachCancelLadder(opId, ct);
        ReadStreamTicket ticket;
        try
        {
            var request = new OpenReadRequest
            {
                OpId = opId,
                PartitionId = partitionId,
                Options = MessageMapping.ToBatchOptionsMsg(options),
            };
            ticket = await client.Grpc.OpenReadStreamAsync(request, cancellationToken: ct).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
        catch (ObjectDisposedException)
        {
            throw ProcessFailureMapping.Condemned(process);
        }

        await using var enumerator = DataPlane
            .ReadStreamAsync(process.DataSocketPath, ticket.Ticket.Memory, ct)
            .GetAsyncEnumerator(ct);
        while (true)
        {
            RecordBatch? batch;
            try
            {
                batch = await enumerator.MoveNextAsync().ConfigureAwait(false) ? enumerator.Current : null;
            }
            catch (ConnectorHostException ex)
            {
                // Layer 1: DataPlane's own EOS-marker tail check already turned a torn mid-stream read
                // into this. IDatasetPartition.ReadAsync's contract wants PzConnectorException
                // specifically -- the only exception type the engine's retry logic understands --
                // transient only when the process is confirmed gone; a still-alive connector that broke
                // the wire protocol is a bug a retry will not fix.
                throw ProcessFailureMapping.ToPzConnectorException(client, process, ex.Message);
            }

            if (batch is null)
            {
                break;
            }

            yield return batch;
        }

        // Layer 2: the stream ended with a proper EOS marker, but a connector must never exit before
        // the host-initiated Shutdown RPC -- observing it already gone here means it crashed during or
        // immediately after this read, and letting the `await foreach` complete cleanly would hide
        // that from the engine as if every row had truly landed.
        if (process.HasExited)
        {
            throw ProcessFailureMapping.ToPzConnectorException(
                client, process, "connector process exited after completing a partition read");
        }
    }
}

/// <summary>Composition, not inheritance, over <see cref="ProcessPartition"/>: only engaged when the
/// connector declared <see cref="ConnectorCapabilities.StablePartitionIds"/>, per
/// <see cref="IIdentifiedPartition"/>'s contract.</summary>
internal sealed class ProcessIdentifiedPartition(PcpClient client, ConnectorProcess process, string opId, string partitionId)
    : IIdentifiedPartition
{
    private readonly ProcessPartition _inner = new(client, process, opId, partitionId);

    public string PartitionId => partitionId;

    public IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, CancellationToken ct) =>
        _inner.ReadAsync(options, ct);
}

/// <summary>Shared IConnector-surface plumbing (Info/Capabilities/schemas/Validate/CheckConnection) --
/// identical on the source and sink shim, so both hold one of these instead of duplicating it.</summary>
internal readonly struct ProcessConnectorCore(PcpClient client, ConnectorProcess process)
{
    public ConnectorInfo Info => MessageMapping.ToConnectorInfo(client.Hello.Info);

    public ConnectorCapabilities Capabilities =>
        ProcessCapabilities.Mask((ConnectorCapabilities)unchecked((int)client.Hello.Capabilities));

    public string ConnectionConfigSchema => client.Hello.ConnectionConfigSchema;

    public string DatasetConfigSchema => client.Hello.DatasetConfigSchema;

    public async ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            var response = await client.Grpc
                .ValidateAsync(new ValidateRequest { Config = MessageMapping.ToStruct(config.Values) }, cancellationToken: ct)
                .ConfigureAwait(false);
            return MessageMapping.ToValidationResult(response);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
        catch (ObjectDisposedException)
        {
            throw ProcessFailureMapping.Condemned(process);
        }
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        try
        {
            var response = await client.Grpc
                .CheckConnectionAsync(new CheckRequest { Config = MessageMapping.ToStruct(config.Values) }, cancellationToken: ct)
                .ConfigureAwait(false);
            return MessageMapping.ToConnectionCheck(response);
        }
        catch (RpcException ex)
        {
            throw ProcessFailureMapping.MapControlPlane(client, process, ex, ct);
        }
        catch (ObjectDisposedException)
        {
            throw ProcessFailureMapping.Condemned(process);
        }
    }
}

/// <summary>What an ISource/ISink shim over a PCP connector is allowed to claim it can do.
///
/// <para>The shims forward the wrapped connector's Hello capability flags verbatim, including flags
/// whose ABI interfaces they do not implement -- there is no PCP wiring behind
/// <see cref="ICheckpointingPartition"/>, <see cref="ICheckpointingSinkSession"/>,
/// <see cref="ISyncStatePartition"/>, or change capture, and the reverse channel's WriteAck/ReadState
/// messages are read and dropped. Surfacing those flags unmasked would make the planner accept a
/// checkpointed / sync-state / cdc dataset on this connector and then silently degrade it to a plain
/// full read. Masking them here is what makes the planner REFUSE such a dataset (PZ0319/PZ0338/...)
/// instead, which is the correct answer until each one is actually implemented over the wire.</para></summary>
internal static class ProcessCapabilities
{
    private const ConnectorCapabilities Unimplemented =
        ConnectorCapabilities.CheckpointableReads | ConnectorCapabilities.CheckpointableWrites |
        ConnectorCapabilities.SyncState | ConnectorCapabilities.ChangeCapture;

    public static ConnectorCapabilities Mask(ConnectorCapabilities declared) => declared & ~Unimplemented;
}

/// <summary>What the host's deferred gate reads off an opened shim: the <see cref="IOperationGate"/>
/// the engine handed it through <see cref="IOperationGateAware"/>, or null before it did. Exists
/// because <see cref="HostChannelPump"/> is opened right after Configure -- before OpenAsync has even
/// returned the shim the engine will later gate.</summary>
internal interface IGatedShim
{
    IOperationGate? Gate { get; }
}

/// <summary>Where every shim method (source and sink alike) turns a failed RPC or data-plane read into
/// the exception the ABI actually promises.</summary>
internal static class ProcessFailureMapping
{
    /// <summary>ISource.TryGetNativeScan/ISink.TryGetNativeCopy are synchronous in the ABI -- no
    /// CancellationToken to forward -- so this bounds the blocking RPC call they make instead, so a
    /// hung connector cannot hang the planner forever.</summary>
    public static readonly TimeSpan NativeOperationTimeout = TimeSpan.FromSeconds(30);

    /// <summary>The RPC that produced <paramref name="ex"/> never had a caller CancellationToken to
    /// answer "was this me?" with -- <see cref="NativeOperationTimeout"/> firing is the only source of
    /// cancellation on that call, so the exception is unambiguously a hung connector, not a protocol
    /// violation. Transient: nothing here says the connector is broken, only slow.</summary>
    public static PzConnectorException NativeOperationTimedOut(string rpcName) =>
        new($"connector did not answer {rpcName} within {NativeOperationTimeout}", isTransient: true);

    /// <summary>Rebuilds the exception a control-plane RPC failure should surface as.
    /// <see cref="PcpClient.MapRpcException"/>'s own PZ0358 (mid-operation death, no error trailer) is
    /// a Pz.PackageManagement-internal host exception; every method here promises
    /// <see cref="PzConnectorException"/> for an operational failure instead (it is the only exception
    /// type the engine's retry logic understands), so that one case is converted here. Transient unless
    /// the connector's last reported failure on this client was explicitly non-transient -- a crash
    /// right after "this is permanent" is not a new transient fact.</summary>
    public static Exception MapControlPlane(PcpClient client, ConnectorProcess process, RpcException ex, CancellationToken ct)
    {
        var mapped = client.MapRpcException(ex, ct);
        return mapped is ConnectorHostException { Code: "PZ0358" } hostEx
            ? new PzConnectorException(hostEx.Message, isTransient: client.LastErrorWasTransient != false)
            : mapped;
    }

    /// <summary>The instance was torn down underneath an operation that was still using it: a sibling
    /// operation's cancellation ladder condemned the process and disposed the shared
    /// <see cref="GrpcChannel"/>, so <see cref="PcpClient.Grpc"/> throws
    /// <see cref="ObjectDisposedException"/> rather than any <see cref="RpcException"/> this file would
    /// otherwise map. That raw exception must not cross the ABI boundary — the engine only understands
    /// <see cref="PzConnectorException"/> here. Transient: nothing about the WORK failed, only the
    /// instance carrying it, and a fresh instance would run it.</summary>
    public static PzConnectorException Condemned(ConnectorProcess process)
    {
        var stderr = process.StderrTail;
        var suffix = stderr.Length > 0 ? $"\nstderr:\n{stderr}" : string.Empty;
        return new PzConnectorException(
            "connector instance was shut down while this operation was in flight " +
            $"(a cancellation ladder condemned the process){suffix}",
            isTransient: true);
    }

    /// <summary>Same conversion for a data-plane failure that never went through an
    /// <see cref="RpcException"/> at all -- <see cref="DataPlane"/> has no <see cref="PcpClient"/>/
    /// <see cref="ConnectorProcess"/> access to do this itself, so callers append the stderr tail here.
    /// Covers a torn stream (layer 1) and a clean completion the process did not survive (layer 2).</summary>
    public static PzConnectorException ToPzConnectorException(PcpClient client, ConnectorProcess process, string cause)
    {
        var stderr = process.StderrTail;
        var suffix = stderr.Length > 0 ? $"\nstderr:\n{stderr}" : string.Empty;
        var transient = process.HasExited && client.LastErrorWasTransient != false;
        return new PzConnectorException($"{cause}{suffix}", isTransient: transient);
    }
}

file static class HelloCapabilitiesExtensions
{
    /// <summary>Decomposes <c>Hello.Capabilities</c> (an int64 OR of <see cref="ConnectorCapabilities"/>
    /// flags, carried verbatim per the proto) without relying on <c>Enum.HasFlag</c>'s boxing.</summary>
    public static bool HasCapability(this long capabilities, ConnectorCapabilities flag) =>
        (capabilities & (long)flag) == (long)flag;
}
