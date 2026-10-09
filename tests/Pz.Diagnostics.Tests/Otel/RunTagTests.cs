using System.Diagnostics.Metrics;
using Pz.Diagnostics.Otel;

namespace Pz.Diagnostics.Tests.Otel;

/// <summary>Meter listeners are process-global, so tests that listen to <see cref="PzMeters"/> never run beside each other.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OtelGlobalCollection
{
    public const string Name = "OtelGlobal";
}

[Collection(OtelGlobalCollection.Name)]
public sealed class RunTagTests
{
    [Fact]
    public async Task Measurements_inside_a_run_carry_its_id_across_awaits()
    {
        var tags = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (i, l) => { if (i.Meter.Name == PzMeters.Name && i.Name == "pz.rows_moved") l.EnableMeasurementEvents(i); };
        listener.SetMeasurementEventCallback<long>((_, _, t, _) => { lock (tags) tags.Add(t.ToArray()); });
        listener.Start();

        using (PzMeters.BeginRun("r-1"))
        {
            await Task.Yield();
            await Task.Run(() => PzMeters.RowsMoved.Add(3, new KeyValuePair<string, object?>("pz.node.kind", "SourceLoad")));
        }
        PzMeters.RowsMoved.Add(1);

        Assert.Contains(tags[0], t => t.Key == "pz.run.id" && (string?)t.Value == "r-1");
        Assert.Contains(tags[0], t => t.Key == "pz.node.kind");
        Assert.DoesNotContain(tags[1], t => t.Key == "pz.run.id");
    }

    [Fact]
    public void Histograms_carry_the_run_id_too()
    {
        var tags = new List<KeyValuePair<string, object?>[]>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (i, l) => { if (i.Meter.Name == PzMeters.Name && i.Name == "pz.node.duration") l.EnableMeasurementEvents(i); };
        listener.SetMeasurementEventCallback<double>((_, _, t, _) => { lock (tags) tags.Add(t.ToArray()); });
        listener.Start();

        using (PzMeters.BeginRun("r-2"))
            PzMeters.NodeDuration.Record(5, new KeyValuePair<string, object?>("pz.node.kind", "SinkWrite"));

        Assert.Contains(Assert.Single(tags), t => t.Key == "pz.run.id" && (string?)t.Value == "r-2");
    }
}
