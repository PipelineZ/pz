using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Protocol.V1;
using AbsStats = Pz.Connectors.Abstractions.ConnectorStats;
using PbStats = Pz.Connectors.Protocol.V1.ConnectorStats;

namespace Pz.Connectors.Sdk.Tests;

public sealed class StatsReportingTests
{
    private static readonly ActivitySource Source = new("test");
    private static readonly Schema RowSchema = PlainSource.RowSchema;

    [Fact]
    public async Task Read_stats_reported_inside_the_partition_iterator_are_answered_by_GetReadStats()
    {
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(new StatsSourceConnector(report: true), tickets);
        await PlanAsync(service);
        await ServeAsync(service, tickets);

        var response = await service.GetReadStats(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());

        Assert.NotNull(response.Stats);
        Assert.Equal(3, response.Stats!.Requests);
    }

    [Fact]
    public async Task GetReadStats_for_a_partition_that_reported_nothing_leaves_stats_unset()
    {
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(new StatsSourceConnector(report: false), tickets);
        await PlanAsync(service);
        await ServeAsync(service, tickets);

        var response = await service.GetReadStats(new ReadStateRequest { OpId = "op", PartitionId = "0" }, Context());

        Assert.Null(response.Stats);
    }

    [Fact]
    public async Task GetReadStats_for_an_unknown_op_or_partition_is_empty_not_an_error()
    {
        var service = await ConfiguredAsync(new StatsSourceConnector(report: true), new TicketRegistry());
        await PlanAsync(service);

        Assert.Null((await service.GetReadStats(new ReadStateRequest { OpId = "nope", PartitionId = "0" }, Context())).Stats);
        Assert.Null((await service.GetReadStats(new ReadStateRequest { OpId = "op", PartitionId = "9" }, Context())).Stats);
    }

    [Fact]
    public async Task Commit_returns_the_sessions_stats()
    {
        var tickets = new TicketRegistry();
        var service = await ConfiguredAsync(new StatsSinkConnector(), tickets);
        var begun = await service.BeginWrite(await BeginWriteRequestAsync(), Context());
        await DrainWriteAsync(tickets, begun);

        var result = await service.CommitWrite(new SessionRef { SessionId = begun.SessionId }, Context());

        Assert.NotNull(result.Stats);
        Assert.Equal(4, result.Stats!.Requests);
    }

    [Fact]
    public async Task Abort_returns_the_sessions_stats()
    {
        var service = await ConfiguredAsync(new StatsSinkConnector(), new TicketRegistry());
        var begun = await service.BeginWrite(await BeginWriteRequestAsync(), Context());

        var result = await service.AbortWrite(new SessionRef { SessionId = begun.SessionId }, Context());

        Assert.NotNull(result.Stats);
        Assert.Equal(2, result.Stats!.Requests);
    }

    [Fact]
    public void Mapping_round_trips_every_field()
    {
        var stats = new RemoteStats(5, 1024, 77, 2, 1.5, "ru", [new("a", 1.0), new("b", 2.5)], 3);

        var back = ConnectorStatsMapping.FromProto(ConnectorStatsMapping.ToProto(stats)!);

        Assert.Equal(stats with { Extras = [] }, back with { Extras = [] });
        Assert.Equal(stats.Extras, back.Extras);
    }

    [Fact]
    public void Mapping_an_empty_cost_unit_is_a_null_cost()
    {
        var back = ConnectorStatsMapping.FromProto(new PbStats { Requests = 1, CostUnits = 9 });

        Assert.Null(back.CostUnit);
        Assert.Null(back.CostUnits);
    }

    [Fact]
    public void Mapping_an_empty_snapshot_is_null()
    {
        Assert.Null(ConnectorStatsMapping.ToProto(new ConnectorStatsAccumulator().Snapshot()));
    }

    [Fact]
    public void Mapping_carries_drops_and_sends_a_drops_only_message()
    {
        var accumulator = new ConnectorStatsAccumulator();
        accumulator.AddExtra("Bad Name", 1);

        var proto = ConnectorStatsMapping.ToProto(accumulator);

        Assert.NotNull(proto);
        Assert.Equal((uint)StatsDrop.InvalidExtraName, proto!.Drops);
        Assert.Equal(0, proto.Requests);

        accumulator.AddRequests(2);
        accumulator.AddExtra("x.y", -1);
        var both = ConnectorStatsMapping.ToProto(accumulator)!;
        Assert.Equal(2, both.Requests);
        Assert.Equal((uint)(StatsDrop.InvalidExtraName | StatsDrop.NegativeOrNonFinite), both.Drops);
    }

    // --- helpers ---

    private static async Task ServeAsync(PcpConnectorService service, TicketRegistry tickets)
    {
        var ticket = await service.OpenReadStream(new OpenReadRequest { OpId = "op", PartitionId = "0" }, Context());
        Assert.True(tickets.TryBurn(ticket.Ticket.ToByteArray(), out var entry));
        using var stream = new MemoryStream();
        await DataPlaneListener.ServeReadAsync(stream, Assert.IsType<ReadTicket>(entry), Source, CancellationToken.None);
    }

    private static async Task DrainWriteAsync(TicketRegistry tickets, WriteSessionTicket begun)
    {
        Assert.True(tickets.TryBurn(begun.Ticket.ToByteArray(), out var entry));
        using var stream = new MemoryStream();
        using (var writer = new ArrowStreamWriter(stream, RowSchema, leaveOpen: true))
        {
            await writer.WriteStartAsync();
            for (var i = 0; i < 2; i++)
            {
                using var batch = new RecordBatch(RowSchema, [new Int64Array.Builder().Append(i).Build()], 1);
                await writer.WriteRecordBatchAsync(batch);
            }

            await writer.WriteEndAsync();
        }

        stream.Position = 0;
        await DataPlaneListener.ServeWriteAsync(
            stream, Assert.IsType<WriteTicket>(entry), Source, PooledNativeAllocator.Shared, CancellationToken.None);
    }

    private static async Task<BeginWriteRequest> BeginWriteRequestAsync() => new()
    {
        OpId = "op",
        Spec = new OutputSpecMsg { Sink = "lake", Output = "o", Mode = "replace", SchemaPolicy = "fail_on_change", Options = new Struct() },
        ArrowSchemaIpc = await SchemaCodec.SerializeAsync(RowSchema, CancellationToken.None),
    };

    private static async Task PlanAsync(PcpConnectorService service)
    {
        var writer = new ListStreamWriter<PartitionMsg>();
        await service.PlanRead(
            new PlanReadRequest { OpId = "op", Spec = new DatasetSpecMsg { Source = "files", Dataset = "orders", Options = new Struct() } },
            writer, Context());
    }

    private static async Task<PcpConnectorService> ConfiguredAsync(IConnector connector, TicketRegistry registry)
    {
        var service = new PcpConnectorService(
            connector, registry, new HostChannelPeer(), new ConnectorTelemetry(new PzConnectorHostOptions()),
            PcpServerHooks.None, new NullLifetime());
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

    private sealed class StatsSourceConnector(bool report) : ISourceConnector
    {
        public ConnectorInfo Info => new("stats", "1.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ValidationResult([]));
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true, null));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult<ISource>(new StatsSource(report));
    }

    private sealed class StatsSource(bool report) : PlainSource
    {
        public override ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            ValueTask.FromResult<IReadOnlyList<IDatasetPartition>>([new StatsPartition(report)]);
    }

    private sealed class StatsPartition(bool report) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(
            BatchOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            for (var i = 0; i < 3; i++)
            {
                await Task.Yield();
                if (report)
                {
                    AbsStats.Current.AddRequests(1);
                }

                yield return new RecordBatch(RowSchema, [new Int64Array.Builder().Append(i).Build()], 1);
            }
        }
    }

    private sealed class StatsSinkConnector : ISinkConnector
    {
        public ConnectorInfo Info => new("stats-sink", "1.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ValidationResult([]));
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true, null));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult<ISink>(new StatsSink());
    }

    private sealed class StatsSink : ISink
    {
        public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = null;
            return false;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
        {
            AbsStats.Current.AddRequests(1);
            return ValueTask.FromResult<ISinkWriteSession>(new StatsSession());
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class StatsSession : ISinkWriteSession
    {
        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            AbsStats.Current.AddRequests(1);
            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
        {
            AbsStats.Current.AddRequests(1);
            return ValueTask.FromResult(new WriteResult(2, 2));
        }

        public ValueTask AbortAsync(CancellationToken ct)
        {
            AbsStats.Current.AddRequests(1);
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
