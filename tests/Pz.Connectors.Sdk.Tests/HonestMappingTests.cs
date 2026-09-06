using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class HonestMappingTests
{
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
        var service = await ConfiguredAsync(connector);
        var planned = await PlanAsync(service, "op");
        Assert.True(Assert.Single(planned).SyncState);

        // Nothing captured yet: no token, and no poll of the partition either.
        var before = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.False(before.HasToken);

        var ticket = await service.OpenReadStream(new OpenReadRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.Equal(16, ticket.Ticket.Length);
        var entry = Assert.IsType<ReadTicket>(BurnTicket(service, ticket.Ticket.ToByteArray()));
        var partition = Assert.IsType<SyncPartition>(entry.Partition);
        Assert.Equal(0, partition.Polls);

        using var stream = new MemoryStream();
        await DataPlaneListener.ServeReadAsync(stream, entry, CancellationToken.None);
        Assert.Equal(1, partition.Polls);

        var after = await service.GetReadState(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.Equal("0+3", after.Token);
        Assert.Equal(1, partition.Polls);
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

    // --- helpers ---

    private static PcpConnectorService NewService(IConnector connector) =>
        new(connector, new TicketRegistry(), new HostChannelPeer(), PcpServerHooks.None, new NullLifetime());

    private static async Task<PcpConnectorService> ConfiguredAsync(IConnector connector)
    {
        var service = NewService(connector);
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

    private static TicketEntry BurnTicket(PcpConnectorService service, byte[] ticket)
    {
        Assert.True(service.Tickets.TryBurn(ticket, out var entry));
        return entry;
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
