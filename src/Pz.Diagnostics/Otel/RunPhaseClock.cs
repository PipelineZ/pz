using System.Diagnostics;
using Pz.Diagnostics.Events;

namespace Pz.Diagnostics.Otel;

/// <summary>Times a run's phases. Created before the run span exists (load, compile and validate finish first),
/// so phases that end before <see cref="AttachRunSpan"/> get spans afterwards with their real start and end times;
/// later phases get live spans. Phases run one at a time.</summary>
public sealed class RunPhaseClock
{
    public const string Load = "load";
    public const string Compile = "compile";
    public const string Validate = "validate";
    public const string RestoreCheck = "restore_check";
    public const string Plan = "plan";
    public const string Execute = "execute";
    public const string Finalize = "finalize";

    private static readonly string[] Known = [Load, Compile, Validate, RestoreCheck, Plan, Execute, Finalize];

    private readonly TimeProvider _time;
    private readonly long _origin;
    private readonly Lock _gate = new();
    private readonly Dictionary<string, (long Start, long? End)> _phases = [];
    private Activity? _run;
    private string? _current;
    private string? _lastBegun;

    public RunPhaseClock(TimeProvider time)
    {
        _time = time;
        _origin = time.GetTimestamp();
        StartedAt = time.GetUtcNow();
    }

    public DateTimeOffset StartedAt { get; }

    /// <summary>The phase currently open, or null between phases.</summary>
    public string? Current
    {
        get { lock (_gate) return _current; }
    }

    /// <summary>The most recently begun phase, kept after it ends.</summary>
    public string? LastBegun
    {
        get { lock (_gate) return _lastBegun; }
    }

    public IDisposable Begin(string phase)
    {
        if (Array.IndexOf(Known, phase) < 0)
            throw new ArgumentOutOfRangeException(nameof(phase), phase, "unknown run phase");

        Activity? run;
        lock (_gate)
        {
            _phases[phase] = (_time.GetTimestamp(), null);
            _current = phase;
            _lastBegun = phase;
            run = _run;
        }

        // Started outside the lock: listener callbacks must not run under it.
        var span = run is null
            ? null
            : PzActivitySource.Instance.StartActivity("phase." + phase, ActivityKind.Internal, run.Context);

        return new Scope(this, phase, span);
    }

    /// <summary>Gives every phase already finished a span under <paramref name="run"/>, back-dated to when it ran.</summary>
    public void AttachRunSpan(Activity? run)
    {
        if (run is null) return;
        List<(string Name, long Start, long End)> finished = [];
        lock (_gate)
        {
            _run = run;
            foreach (var (name, (start, end)) in _phases)
            {
                if (end is { } e) finished.Add((name, start, e));
            }
        }

        // Started outside the lock: listener callbacks must not run under it.
        foreach (var (name, start, end) in finished)
        {
            var span = PzActivitySource.Instance.StartActivity("phase." + name, ActivityKind.Internal, run.Context,
                startTime: At(start));
            if (span is null) continue;
            span.SetEndTime(At(end).UtcDateTime);
            span.Stop();
        }
    }

    public RunPhasesPayload Snapshot()
    {
        lock (_gate)
        {
            long? Ms(string name) => _phases.TryGetValue(name, out var p)
                ? (long)_time.GetElapsedTime(p.Start, p.End ?? _time.GetTimestamp()).TotalMilliseconds
                : null;
            return new RunPhasesPayload(Ms(Load), Ms(Compile), Ms(Validate), Ms(RestoreCheck), Ms(Plan), Ms(Execute),
                Ms(Finalize));
        }
    }

    private DateTimeOffset At(long timestamp) => StartedAt + _time.GetElapsedTime(_origin, timestamp);

    private void End(string phase)
    {
        lock (_gate)
        {
            if (_phases.TryGetValue(phase, out var p) && p.End is null) _phases[phase] = (p.Start, _time.GetTimestamp());
            if (_current == phase) _current = null;
        }
    }

    private sealed class Scope(RunPhaseClock owner, string phase, Activity? span) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            owner.End(phase);
            span?.Dispose();
        }
    }
}
