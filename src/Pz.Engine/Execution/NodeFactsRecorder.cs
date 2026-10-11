using System.Diagnostics;
using Pz.Diagnostics.Otel;

namespace Pz.Engine.Execution;

/// <summary>Per-node facts an executor learns while running: the tier it took and each stage's wall time.
/// Stage times are measured from the <see cref="TimeProvider"/>, not from the stage span, so they exist whether
/// or not anything is listening for spans. Reset at the start of every attempt so a retried node reports its
/// last attempt only.</summary>
public sealed class NodeFactsRecorder(TimeProvider? time = null)
{
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly Lock _gate = new();
    private TimeSpan? _extract, _ingest, _egress, _write;

    public string? Strategy { get; set; }

    /// <summary>Starts the named stage's span (a child of whatever is ambient, as before) and its clock.</summary>
    public StageScope Stage(string name)
    {
        if (name is not ("extract" or "ingest" or "egress" or "write"))
        {
            throw new ArgumentOutOfRangeException(nameof(name), name, "unknown stage");
        }

        return new StageScope(this, name, _time.GetTimestamp(), PzActivitySource.Instance.StartActivity(name));
    }

    public void Reset()
    {
        lock (_gate)
        {
            _extract = _ingest = _egress = _write = null;
            Strategy = null;
        }
    }

    public StageTimings? Stages()
    {
        lock (_gate)
        {
            return _extract is null && _ingest is null && _egress is null && _write is null
                ? null
                : new StageTimings(_extract, _ingest, _egress, _write);
        }
    }

    internal void Record(string name, long startTimestamp)
    {
        var elapsed = _time.GetElapsedTime(startTimestamp);
        lock (_gate)
        {
            switch (name)
            {
                case "extract": _extract = elapsed; break;
                case "ingest": _ingest = elapsed; break;
                case "egress": _egress = elapsed; break;
                default: _write = elapsed; break;
            }
        }
    }
}

/// <summary>One stage of one attempt. Disposing records the stage's time once and stops its span.</summary>
public sealed class StageScope : IDisposable
{
    private readonly NodeFactsRecorder _owner;
    private readonly string _name;
    private readonly long _start;
    private int _disposed;

    internal StageScope(NodeFactsRecorder owner, string name, long start, Activity? activity)
    {
        _owner = owner;
        _name = name;
        _start = start;
        Activity = activity;
    }

    public Activity? Activity { get; }

    public void SetRows(long rows) => Activity?.SetTag("pz.stage.rows", rows);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _owner.Record(_name, _start);
        Activity?.Dispose();
    }
}
