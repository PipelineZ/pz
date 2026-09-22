using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Exercises the write path <see cref="FakeConnectors"/>' source fakes never reach:
/// BeginWrite mints a ticket, the data plane pump drains an Arrow IPC stream into the sink's own
/// <see cref="ISinkWriteSession"/> exactly as a real socket connection would (<see
/// cref="DataPlaneListener.ServeWriteAsync(Stream,WriteTicket,ActivitySource,Apache.Arrow.Memory.MemoryAllocator,CancellationToken)"/>),
/// and CommitWrite/AbortWrite report back whatever the session itself recorded. The write-side mirror
/// of <c>HonestMappingTests.GetReadState_answers_from_the_capture_and_never_polls_the_partition_itself</c>.</summary>
public sealed class WriteRoundTripTests
{
    private static readonly ActivitySource Source = new("test");
    private static readonly Schema RowSchema = new Schema.Builder().Field(new Apache.Arrow.Field("id", Int64Type.Default, false)).Build();

    [Fact]
    public async Task A_write_stream_drains_into_the_session_and_commits_the_sessions_own_count()
    {
        var connector = new FakeSinkConnector();
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(connector, tickets);

        var begun = await service.BeginWrite(await BeginWriteRequestAsync("op"), Context());
        Assert.True(tickets.TryBurn(begun.Ticket.ToByteArray(), out var entry));
        var write = Assert.IsType<WriteTicket>(entry);

        using (var stream = new MemoryStream())
        {
            await WriteBatchesAsync(stream, rowsPerBatch: [2, 1]);
            stream.Position = 0;
            await DataPlaneListener.ServeWriteAsync(stream, write, Source, PooledNativeAllocator.Shared, CancellationToken.None);
        }

        // Committing only after the drain matches CommitWrite's own contract: the control plane awaits
        // WriteSessionState.Drained before it ever calls CommitAsync, so the pump above -- not the RPC
        // below -- is what must have already delivered every batch to the session.
        Assert.NotNull(connector.Sink.LastSession);
        Assert.Equal(2, connector.Sink.LastSession!.Batches.Count);
        Assert.Equal(3, connector.Sink.LastSession.Rows);
        Assert.False(connector.Sink.LastSession.Committed);

        var result = await service.CommitWrite(new SessionRef { SessionId = begun.SessionId }, Context());

        Assert.Equal(3, result.RowsWritten);
        Assert.Equal(2, result.BatchesWritten);
        Assert.True(connector.Sink.LastSession.Committed);
    }

    [Fact]
    public async Task AbortWrite_never_reaches_a_session_the_data_plane_never_drained()
    {
        var connector = new FakeSinkConnector();
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(connector, tickets);

        var begun = await service.BeginWrite(await BeginWriteRequestAsync("op"), Context());

        // No data connection at all: the session was opened but nothing was ever written to it, the
        // shape a torn or never-started upload leaves behind.
        await service.AbortWrite(new SessionRef { SessionId = begun.SessionId }, Context());

        Assert.NotNull(connector.Sink.LastSession);
        Assert.True(connector.Sink.LastSession!.Aborted);
        Assert.False(connector.Sink.LastSession.Committed);
        Assert.Empty(connector.Sink.LastSession.Batches);
    }

    /// <summary>PcpConnectorService opens a connector's sink at most once per process
    /// (<c>_sink</c> is process-wide state, exactly like <c>_source</c>): two BeginWrite calls for two
    /// different ops must share the one already-opened sink rather than opening it again for the
    /// second op or output.</summary>
    [Fact]
    public async Task Two_write_sessions_from_different_ops_reuse_the_one_opened_sink()
    {
        var connector = new FakeSinkConnector();
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(connector, tickets);

        var first = await service.BeginWrite(await BeginWriteRequestAsync("op-a"), Context());
        var second = await service.BeginWrite(await BeginWriteRequestAsync("op-b"), Context());

        Assert.NotEqual(first.SessionId, second.SessionId);
        Assert.Equal(1, connector.Opens);
    }

    private static async Task WriteBatchesAsync(Stream stream, IReadOnlyList<int> rowsPerBatch)
    {
        using var writer = new ArrowStreamWriter(stream, RowSchema, leaveOpen: true);
        await writer.WriteStartAsync();
        var id = 0;
        foreach (var rows in rowsPerBatch)
        {
            var builder = new Int64Array.Builder();
            for (var i = 0; i < rows; i++)
            {
                builder.Append(id++);
            }

            using var batch = new RecordBatch(RowSchema, [builder.Build()], rows);
            await writer.WriteRecordBatchAsync(batch);
        }

        await writer.WriteEndAsync();
        await stream.FlushAsync();
    }

    private static async Task<BeginWriteRequest> BeginWriteRequestAsync(string opId) => new()
    {
        OpId = opId,
        Spec = new OutputSpecMsg { Sink = "lake", Output = "orders_copy", Mode = "replace", SchemaPolicy = "fail_on_change", Options = new Struct() },
        ArrowSchemaIpc = await SchemaCodec.SerializeAsync(RowSchema, CancellationToken.None),
    };

    private static PcpConnectorService NewService(IConnector connector, TicketRegistry tickets) =>
        new(connector, tickets, new HostChannelPeer(), new ConnectorTelemetry(new PzConnectorHostOptions()),
            PcpServerHooks.None, new NullLifetime());

    private static async Task<PcpConnectorService> ConfiguredAsync(IConnector connector, TicketRegistry tickets)
    {
        var service = NewService(connector, tickets);
        await service.Handshake(new HandshakeRequest { ProtocolMajor = ProtocolVersion.Major }, Context());
        await service.Configure(new ConfigureRequest { InstanceId = "test", Config = new Struct() }, Context());
        return service;
    }

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
