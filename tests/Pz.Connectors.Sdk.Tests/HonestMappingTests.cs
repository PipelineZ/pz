using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class HonestMappingTests
{
    private static readonly ActivitySource Source = new("test");

    [Fact]
    public async Task Hello_reports_the_connector_verbatim()
    {
        var service = NewService(new FakeSourceConnector(ConnectorCapabilities.NativeScan | ConnectorCapabilities.SyncState, feed: true));
        var hello = await service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());

        Assert.Equal("fake", hello.Info.Name);
        Assert.Equal(ProtocolVersion.Major, hello.Info.ProtocolMajor);
        Assert.Equal((long)(ConnectorCapabilities.NativeScan | ConnectorCapabilities.SyncState), hello.Capabilities);
    }

    [Fact]
    public async Task Handshake_refuses_a_host_speaking_a_different_protocol_major()
    {
        var service = NewService(new FakeSourceConnector(ConnectorCapabilities.None, feed: false));
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major + 1 }, Context()));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains((ProtocolVersion.Major + 1).ToString(), ex.Status.Detail, StringComparison.Ordinal);
        Assert.Contains(ProtocolVersion.Major.ToString(), ex.Status.Detail, StringComparison.Ordinal);
    }

    /// <summary>Pins the fix: the check-then-set on <c>_config</c> used to be two separate field
    /// reads with no synchronization, so two Configure calls racing each other could both observe
    /// "not configured yet" and both proceed. A hook pauses the first call INSIDE the critical
    /// section (after it has already set <c>_config</c>) so the second call's outcome is observed
    /// deterministically rather than by timing.</summary>
    [Fact]
    public async Task Configure_serializes_concurrent_calls_so_only_the_first_ones_config_survives()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hooks = new PcpServerHooks
        {
            PauseInsideConfigure = async ct =>
            {
                entered.TrySetResult();
                await release.Task.WaitAsync(ct).ConfigureAwait(false);
            },
        };
        var service = NewService(new FakeSourceConnector(ConnectorCapabilities.None, feed: false), hooks: hooks);
        await service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());

        var first = service.Configure(new ConfigureRequest { InstanceId = "a", Config = new Struct() }, Context());
        await entered.Task;

        // Issued while the first call is still parked inside the critical section: it must block on
        // the same gate rather than racing the first to see _config as null.
        var second = service.Configure(new ConfigureRequest { InstanceId = "b", Config = new Struct() }, Context());

        release.TrySetResult();
        await first;

        var secondFailure = await Assert.ThrowsAsync<RpcException>(() => second);
        Assert.Equal(StatusCode.FailedPrecondition, secondFailure.StatusCode);
    }

    /// <summary>Pins the fix: per-op CancellationTokenSources and plans used to live in <c>_ops</c>/
    /// <c>_plans</c> for the whole process with nothing ever disposing or clearing them.
    /// <c>PcpConnectorService</c> is a DI singleton (<c>PcpServer.ServeAsync</c>), so the container
    /// calls this exactly once, at process shutdown -- proven here directly rather than by spinning up
    /// a real socket-bound server.</summary>
    [Fact]
    public async Task Dispose_releases_every_tracked_operation_and_plan()
    {
        var connector = new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: true);
        var service = await ConfiguredAsync(connector);
        var partitions = await PlanAsync(service, "op");
        Assert.NotEmpty(partitions);

        Assert.True(service.TryGetOpCancellationSource("op", out var cts));
        Assert.Equal(1, service.TrackedOperationCount);
        Assert.Equal(1, service.TrackedPlanCount);

        service.Dispose();

        Assert.Equal(0, service.TrackedOperationCount);
        Assert.Equal(0, service.TrackedPlanCount);
        Assert.Throws<ObjectDisposedException>(() => cts.Token);
    }

    [Fact]
    public async Task A_source_without_INaturalReadShapeSource_answers_unimplemented()
    {
        var service = await ConfiguredAsync(new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: false));
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.GetNaturalReadShape(new NaturalReadShapeRequest { OpId = "op", Spec = Spec() }, Context()));
        Assert.Equal(StatusCode.Unimplemented, ex.StatusCode);
    }

    [Fact]
    public async Task A_feed_source_answers_feed()
    {
        var service = await ConfiguredAsync(new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: true));
        var shape = await service.GetNaturalReadShape(new NaturalReadShapeRequest { OpId = "op", Spec = Spec() }, Context());
        Assert.Equal(NaturalReadShapeResponse.Types.Shape.Feed, shape.Shape);
    }

    [Fact]
    public async Task GetReadState_on_a_partition_without_sync_state_is_a_failed_precondition()
    {
        var service = await ConfiguredAsync(new FakeSourceConnector(ConnectorCapabilities.None, feed: false));
        var partitions = await PlanAsync(service, "op");
        Assert.False(Assert.Single(partitions).SyncState);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context()));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task GetReadState_on_an_unknown_op_or_partition_is_not_found()
    {
        var service = await ConfiguredAsync(new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: true));
        await PlanAsync(service, "op");

        var unknownOp = await Assert.ThrowsAsync<RpcException>(() =>
            service.GetReadState(new ReadStateRequest { OpId = "nope", PartitionId = "0" }, Context()));
        Assert.Equal(StatusCode.NotFound, unknownOp.StatusCode);

        var unknownPartition = await Assert.ThrowsAsync<RpcException>(() =>
            service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "9" }, Context()));
        Assert.Equal(StatusCode.NotFound, unknownPartition.StatusCode);
    }

    [Fact]
    public async Task GetReadState_answers_from_the_capture_and_never_polls_the_partition_itself()
    {
        var connector = new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: true);
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(connector, tickets);
        var planned = await PlanAsync(service, "op");
        Assert.True(Assert.Single(planned).SyncState);

        // Nothing captured yet: no token, and no poll of the partition either.
        var before = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.False(before.HasToken);

        var ticket = await service.OpenReadStream(new OpenReadRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.Equal(16, ticket.Ticket.Length);
        Assert.True(tickets.TryBurn(ticket.Ticket.ToByteArray(), out var entryObj));
        var entry = Assert.IsType<ReadTicket>(entryObj);
        var partition = Assert.IsType<SyncPartition>(entry.Partition);
        Assert.Equal(0, partition.Polls);

        using var stream = new MemoryStream();
        await DataPlaneListener.ServeReadAsync(stream, entry, Source, CancellationToken.None);
        Assert.Equal(1, partition.Polls);

        var after = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.Equal("0+3", after.Token);
        Assert.Equal(1, partition.Polls);
    }

    /// <summary>Pins the fix: OpenReadStream used to hand out the SAME SyncStateCapture for every open
    /// of one (op, partition), so a partition reopened after a completed attempt (a retry, or a host
    /// that reads it again) answered GetReadState with the PREVIOUS attempt's already-completed token
    /// before its own drain had even started -- the exact "fresh capture per stream" rule the sibling
    /// StreamFailureCapture line already followed.</summary>
    [Fact]
    public async Task Reopening_a_partition_replaces_its_sync_state_capture_instead_of_answering_with_the_stale_one()
    {
        var connector = new FakeSourceConnector(ConnectorCapabilities.SyncState, feed: true);
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(connector, tickets);
        await PlanAsync(service, "op");

        var firstTicket = await service.OpenReadStream(new OpenReadRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.True(tickets.TryBurn(firstTicket.Ticket.ToByteArray(), out var firstEntryObj));
        using (var stream = new MemoryStream())
        {
            await DataPlaneListener.ServeReadAsync(stream, Assert.IsType<ReadTicket>(firstEntryObj), Source, CancellationToken.None);
        }

        var afterFirst = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.True(afterFirst.HasToken);

        // Reopen the same partition before draining it again.
        var secondTicket = await service.OpenReadStream(new OpenReadRequest { OpId = "op", PartitionId = "0" }, Context());
        var beforeSecondDrain = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.False(
            beforeSecondDrain.HasToken,
            "a fresh open must not answer with the PREVIOUS attempt's already-completed token");

        Assert.True(tickets.TryBurn(secondTicket.Ticket.ToByteArray(), out var secondEntryObj));
        using (var stream = new MemoryStream())
        {
            await DataPlaneListener.ServeReadAsync(stream, Assert.IsType<ReadTicket>(secondEntryObj), Source, CancellationToken.None);
        }

        var afterSecond = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.True(afterSecond.HasToken);
    }

    [Fact]
    public async Task The_gate_is_handed_to_an_opened_source_exactly_once()
    {
        var connector = new GateCountingSourceConnector();
        var service = await ConfiguredAsync(connector);

        // Both RPCs are past the "source already open" fast path before either can finish opening, so
        // both reach the open gate: the ABI allows exactly one UseOperationGate per opened ISource.
        var first = service.GetSchema(new GetSchemaRequest { OpId = "op", Spec = Spec() }, Context());
        await connector.Entered;
        var second = PlanAsync(service, "op");
        connector.ReleaseOpen();
        await first;
        await second;

        Assert.Equal(1, connector.Opens);
        Assert.Equal(1, connector.Source.GateHandovers);
    }

    [Fact]
    public async Task TryNativeScan_is_not_found_when_the_source_offers_none()
    {
        var service = await ConfiguredAsync(new FakeSourceConnector(ConnectorCapabilities.None, feed: false));
        var scan = await service.TryNativeScan(new NativeScanRequest { OpId = "op", Spec = Spec() }, Context());
        Assert.False(scan.Found);
    }

    [Fact]
    public async Task Configure_before_handshake_is_a_failed_precondition()
    {
        var service = NewService(new FakeSourceConnector(ConnectorCapabilities.None, feed: false));
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.Configure(new ConfigureRequest { InstanceId = "i", Config = new Struct() }, Context()));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task OnConfigure_fires_exactly_once_on_a_successful_Configure()
    {
        var calls = 0;
        var service = new PcpConnectorService(
            new FakeSourceConnector(ConnectorCapabilities.None, feed: false),
            new TicketRegistry(),
            new HostChannelPeer(),
            new ConnectorTelemetry(new PzConnectorHostOptions()),
            new PcpServerHooks { OnConfigure = () => Interlocked.Increment(ref calls) },
            new NullLifetime());

        await service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());
        Assert.Equal(0, calls);

        await service.Configure(new ConfigureRequest { InstanceId = "test", Config = new Struct() }, Context());
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task OnConfigure_does_not_fire_on_a_Configure_rejected_before_handshake()
    {
        var calls = 0;
        var service = new PcpConnectorService(
            new FakeSourceConnector(ConnectorCapabilities.None, feed: false),
            new TicketRegistry(),
            new HostChannelPeer(),
            new ConnectorTelemetry(new PzConnectorHostOptions()),
            new PcpServerHooks { OnConfigure = () => Interlocked.Increment(ref calls) },
            new NullLifetime());

        await Assert.ThrowsAsync<RpcException>(() =>
            service.Configure(new ConfigureRequest { InstanceId = "i", Config = new Struct() }, Context()));

        Assert.Equal(0, calls);
    }

    // --- helpers ---

    /// <summary>The marker interface cannot cross the wire, so the SDK says it with the capability bit
    /// on the connector's behalf -- in the handshake and in the manifest alike, since the host refuses a
    /// Hello whose capabilities differ from the manifest's.</summary>
    [Fact]
    public async Task A_native_only_source_is_declared_NativeOnlyRead_without_its_author_saying_so()
    {
        var connector = new NativeOnlyFakeSource();

        var hello = await NewService(connector)
            .Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());
        var manifest = ManifestWriter.Render(connector, new SortedDictionary<string, string>(StringComparer.Ordinal));

        Assert.Equal((long)(ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeOnlyRead), hello.Capabilities);
        Assert.Contains("\"NativeOnlyRead\"", manifest, StringComparison.Ordinal);
    }

    // A warning belongs to a config that is still valid, so it has to cross beside an empty error list.
    [Fact]
    public async Task Validate_carries_the_connectors_warnings_beside_its_errors()
    {
        var service = NewService(new WarningFakeSource());

        var answer = await service.Validate(new ValidateRequest { Config = new Struct() }, Context());

        Assert.Empty(answer.Errors);
        Assert.Equal(["host key is not pinned"], answer.Warnings);
    }

    private sealed class WarningFakeSource : ISourceConnector
    {
        public ConnectorInfo Info => new("fake", "1.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(ValidationResult.Success with { Warnings = ["host key is not pinned"] });
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true, null));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            throw new NotSupportedException();
    }

    private sealed class NativeOnlyFakeSource : ISourceConnector, INativeOnlySource
    {
        public ConnectorInfo Info => new("fake", "1.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ValidationResult([]));
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true, null));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            throw new NotSupportedException();
    }

    private static PcpConnectorService NewService(
        IConnector connector, TicketRegistry? tickets = null, PcpServerHooks? hooks = null) =>
        new(connector, tickets ?? new TicketRegistry(), new HostChannelPeer(), new ConnectorTelemetry(new PzConnectorHostOptions()),
            hooks ?? PcpServerHooks.None, new NullLifetime());

    private static async Task<PcpConnectorService> ConfiguredAsync(
        IConnector connector, TicketRegistry? tickets = null, PcpServerHooks? hooks = null)
    {
        var service = NewService(connector, tickets, hooks);
        await service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());
        await service.Configure(new ConfigureRequest { InstanceId = "test", Config = new Struct() }, Context());
        return service;
    }

    private static async Task<List<PartitionMsg>> PlanAsync(PcpConnectorService service, string opId)
    {
        var writer = new ListStreamWriter<PartitionMsg>();
        await service.PlanRead(new PlanReadRequest { OpId = opId, Spec = Spec() }, writer, Context());
        return writer.Written;
    }

    private static DatasetSpecMsg Spec() => new() { Source = "files", Dataset = "orders", Options = new Struct() };

    private static ServerCallContext Context() => new TestServerCallContext(CancellationToken.None);

    private sealed class NullLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication()
        {
        }
    }
}
