using Pz.Connectors.Abstractions;
using Pz.PackageManagement.ProcessHosting;
using PbStats = Pz.Connectors.Protocol.V1.ConnectorStats;

namespace Pz.PackageManagement.Tests.ProcessHosting;

public sealed class HostStatsTests
{
    [Fact]
    public void FromProto_maps_every_field_and_sorts_extras_ordinally()
    {
        var proto = new PbStats
        {
            Requests = 3,
            WireBytes = 300,
            RemoteMs = 40,
            Throttled = 2,
            CostUnits = 0.75,
            CostUnit = "ru",
            SpansDropped = 5,
        };
        proto.Extras["b.two"] = 2;
        proto.Extras["a.one"] = 1;

        var stats = HostStats.FromProto(proto);

        Assert.Equal(3, stats.Requests);
        Assert.Equal(300, stats.WireBytes);
        Assert.Equal(40, stats.RemoteMs);
        Assert.Equal(2, stats.Throttled);
        Assert.Equal(0.75, stats.CostUnits);
        Assert.Equal("ru", stats.CostUnit);
        Assert.Equal(5, stats.SpansDropped);
        Assert.Equal(["a.one", "b.two"], stats.Extras.Select(e => e.Key));
    }

    [Fact]
    public void FromProto_treats_an_empty_cost_unit_as_no_cost()
    {
        var stats = HostStats.FromProto(new PbStats { Requests = 1, CostUnits = 9 });

        Assert.Null(stats.CostUnit);
        Assert.Null(stats.CostUnits);
    }

    [Fact]
    public void Merge_ors_the_reported_drops_into_the_ambient_accumulator()
    {
        var accumulator = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(accumulator))
        {
            HostStats.Merge(new PbStats { Drops = (uint)StatsDrop.InvalidExtraName });
            HostStats.Merge(new PbStats { Requests = 1, Drops = (uint)StatsDrop.TooManyExtras });
        }

        Assert.Equal(StatsDrop.InvalidExtraName | StatsDrop.TooManyExtras, accumulator.Drops);
        Assert.Equal(1, accumulator.Snapshot().Requests);
    }

    [Fact]
    public void Merge_adds_into_the_ambient_accumulator()
    {
        var accumulator = new ConnectorStatsAccumulator();
        using (ConnectorStats.Use(accumulator))
        {
            HostStats.Merge(new PbStats { Requests = 2, CostUnits = 0.5, CostUnit = "ru" });
            HostStats.Merge(new PbStats { Requests = 1, CostUnits = 0.25, CostUnit = "ru", SpansDropped = 4 });
            HostStats.Merge(null);
        }

        var snapshot = accumulator.Snapshot();
        Assert.Equal(3, snapshot.Requests);
        Assert.Equal(0.75, snapshot.CostUnits);
        Assert.Equal("ru", snapshot.CostUnit);
        Assert.Equal(4, snapshot.SpansDropped);
    }

    [Fact]
    public void Merge_replays_through_a_foreign_sink_without_spans_dropped()
    {
        var sink = new RecordingStats();
        using (ConnectorStats.Use(sink))
        {
            var proto = new PbStats { Requests = 2, WireBytes = 10, RemoteMs = 3, Throttled = 1, CostUnits = 1.5, CostUnit = "ru", SpansDropped = 9 };
            proto.Extras["x.y"] = 4;
            HostStats.Merge(proto);
        }

        Assert.Equal(
            ["requests=2", "wire=10", "ms=3", "throttled=1", "cost=1.5ru", "extra=x.y:4"],
            sink.Calls);
    }

    private sealed class RecordingStats : IConnectorStats
    {
        public List<string> Calls { get; } = [];

        public void AddRequests(long n) => Calls.Add($"requests={n}");

        public void AddWireBytes(long n) => Calls.Add($"wire={n}");

        public void AddRemoteMs(long ms) => Calls.Add($"ms={ms}");

        public void AddThrottled(long n) => Calls.Add($"throttled={n}");

        public void AddCost(double units, string unit) => Calls.Add($"cost={units}{unit}");

        public void AddExtra(string name, double value) => Calls.Add($"extra={name}:{value}");
    }
}
