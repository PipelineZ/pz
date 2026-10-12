using System.Diagnostics.Metrics;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Diagnostics.Otel;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Dispatch;

public sealed class RemoteMetricsTests
{
    private sealed record Measurement(string Name, double Value, Dictionary<string, object?> Tags);

    private static DagNode Node() => new(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src",
        [], null, new SourceDatasetDef(
            new ConnectionDef("cosmos_a", "cosmos", new Dictionary<string, object?>(), [], "connections.yml"),
            new DatasetDef("orders", new Dictionary<string, object?>(), null)));

    private static NodeResult Result(RemoteStats? remote) =>
        new(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src", NodeStatus.Success, 1,
            TimeSpan.FromSeconds(1), null, Facts: new NodeFacts("cosmos", "cosmos_a", NodeFacts.Arrow, null, 1, remote));

    private static List<Measurement> Capture(Action act)
    {
        var seen = new List<Measurement>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == PzMeters.Name && instrument.Name.StartsWith("pz.remote.", StringComparison.Ordinal))
                l.EnableMeasurementEvents(instrument);
        };

        void Handler<T>(Instrument i, T value, ReadOnlySpan<KeyValuePair<string, object?>> tags) where T : struct, IConvertible
        {
            var d = new Dictionary<string, object?>();
            foreach (var t in tags) d[t.Key] = t.Value;
            lock (seen) seen.Add(new Measurement(i.Name, value.ToDouble(null), d));
        }

        listener.SetMeasurementEventCallback<long>((i, v, t, _) => Handler(i, v, t));
        listener.SetMeasurementEventCallback<double>((i, v, t, _) => Handler(i, v, t));
        listener.Start();
        act();
        return seen;
    }

    [Fact]
    public void Each_instrument_fires_once_with_instance_and_connector_tags()
    {
        var stats = new RemoteStats(42, 1830421, 950, 3, 12.5, "ru", [], 0);

        var seen = Capture(() => RemoteMetrics.Record(Node(), Result(stats)));

        Assert.Equal(5, seen.Count);
        foreach (var m in seen)
        {
            Assert.Equal("conn:cosmos_a", m.Tags["pz.instance"]);
            Assert.Equal("cosmos", m.Tags["pz.connector"]);
        }

        Assert.Equal(42, seen.Single(m => m.Name == "pz.remote.requests").Value);
        Assert.Equal(1830421, seen.Single(m => m.Name == "pz.remote.wire_bytes").Value);
        Assert.Equal(3, seen.Single(m => m.Name == "pz.remote.throttled").Value);
        Assert.Equal(950, seen.Single(m => m.Name == "pz.remote.duration").Value);
        var cost = seen.Single(m => m.Name == "pz.remote.cost");
        Assert.Equal(12.5, cost.Value);
        Assert.Equal("ru", cost.Tags["pz.remote.cost_unit"]);
    }

    [Fact]
    public void Nothing_fires_without_remote_stats()
    {
        Assert.Empty(Capture(() => RemoteMetrics.Record(Node(), Result(null))));
    }

    [Fact]
    public void Zero_valued_counts_are_not_recorded()
    {
        var stats = new RemoteStats(5, 0, 0, 0, null, null, [], 0);

        var seen = Capture(() => RemoteMetrics.Record(Node(), Result(stats)));

        Assert.Equal("pz.remote.requests", Assert.Single(seen).Name);
    }
}
