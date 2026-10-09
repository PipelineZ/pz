using System.Diagnostics.Metrics;

namespace Pz.Diagnostics.Otel;

/// <summary>A counter that adds the ambient run's <c>pz.run.id</c> to every measurement. Azure Monitor keeps no resource
/// attributes on metrics, so the run id must be a point attribute to filter one run's metrics.</summary>
public sealed class RunTaggedCounter<T>(Counter<T> instrument) where T : struct
{
    public Counter<T> Instrument { get; } = instrument;

    public void Add(T value)
    {
        if (PzMeters.CurrentRunId is { } run) Instrument.Add(value, new KeyValuePair<string, object?>("pz.run.id", run));
        else Instrument.Add(value);
    }

    public void Add(T value, KeyValuePair<string, object?> tag)
    {
        if (PzMeters.CurrentRunId is { } run) Instrument.Add(value, tag, new KeyValuePair<string, object?>("pz.run.id", run));
        else Instrument.Add(value, tag);
    }
}

/// <summary>The histogram counterpart of <see cref="RunTaggedCounter{T}"/>.</summary>
public sealed class RunTaggedHistogram<T>(Histogram<T> instrument) where T : struct
{
    public Histogram<T> Instrument { get; } = instrument;

    public void Record(T value)
    {
        if (PzMeters.CurrentRunId is { } run) Instrument.Record(value, new KeyValuePair<string, object?>("pz.run.id", run));
        else Instrument.Record(value);
    }

    public void Record(T value, KeyValuePair<string, object?> tag)
    {
        if (PzMeters.CurrentRunId is { } run) Instrument.Record(value, tag, new KeyValuePair<string, object?>("pz.run.id", run));
        else Instrument.Record(value, tag);
    }
}
