using System.Diagnostics;
using Pz.Diagnostics.Otel;

namespace Pz.Engine.Execution;

/// <summary>Per-node facts an executor learns while running: the tier it took and each stage's wall time and
/// start offset. Stage times are measured from the <see cref="TimeProvider"/>, not from the stage span, so they
/// exist whether or not anything is listening for spans. Offsets count from the node's start
/// (<see cref="MarkNodeStart"/>, the moment <c>node_started</c> is published), not the attempt's, so a host can
/// place a stage on the node's own bar. Reset at the start of every attempt so a retried node reports its last
/// attempt only; the node start survives the reset.</summary>
public sealed class NodeFactsRecorder(TimeProvider? time = null)
{
    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly Lock _gate = new();
    private TimeSpan? _extract, _ingest, _egress, _write;
    private TimeSpan? _extractStart, _ingestStart, _egressStart, _writeStart;
    private long _nodeStart = (time ?? TimeProvider.System).GetTimestamp();


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

    /// <summary>Anchors stage offsets at the node's start. Without it they count from the recorder's creation.</summary>
    public void MarkNodeStart()
    {
        lock (_gate)
        {
            _nodeStart = _time.GetTimestamp();
        }
    }

    public void Reset()
    {
        lock (_gate)
        {
            _extract = _ingest = _egress = _write = null;
            _extractStart = _ingestStart = _egressStart = _writeStart = null;
            Strategy = null;
        }
    }

    public StageTimings? Stages()
    {
        lock (_gate)
        {
            return _extract is null && _ingest is null && _egress is null && _write is null
                ? null
                : new StageTimings(_extract, _ingest, _egress, _write,
                    _extractStart, _ingestStart, _egressStart, _writeStart);
        }
    }

    internal void Record(string name, long startTimestamp)
    {
        var elapsed = _time.GetElapsedTime(startTimestamp);
        lock (_gate)
        {
            var offset = _time.GetElapsedTime(_nodeStart, startTimestamp);
            if (offset < TimeSpan.Zero) offset = TimeSpan.Zero;
            switch (name)
            {
                case "extract": _extract = elapsed; _extractStart = offset; break;
                case "ingest": _ingest = elapsed; _ingestStart = offset; break;
                case "egress": _egress = elapsed; _egressStart = offset; break;
                default: _write = elapsed; _writeStart = offset; break;
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
