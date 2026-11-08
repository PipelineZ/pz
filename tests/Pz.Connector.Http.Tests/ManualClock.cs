using System.Threading.Channels;

namespace Pz.Connector.Http.Tests;

/// <summary>A settable fake clock; <see cref="GetUtcNow"/> only moves through <see cref="Advance"/>.
/// Timers (what <c>Task.Delay(delay, clock, ct)</c> creates) fire only when <see cref="Advance"/>
/// reaches their due time; <see cref="NextTimerAsync"/> hands the test each new timer's due delay so
/// it can advance exactly that far once the code under test is parked on it.</summary>
internal sealed class ManualClock : TimeProvider
{
    private readonly object _lock = new();
    private readonly List<ManualTimer> _timers = [];
    private readonly Channel<TimeSpan> _created = Channel.CreateUnbounded<TimeSpan>();
    private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

    public override DateTimeOffset GetUtcNow()
    {
        lock (_lock)
        {
            return _utcNow;
        }
    }

    public void Advance(TimeSpan by)
    {
        List<ManualTimer> due;
        lock (_lock)
        {
            _utcNow += by;
            due = _timers.Where(t => t.DueAt <= _utcNow).ToList();
            foreach (var timer in due)
            {
                _timers.Remove(timer);
            }
        }

        foreach (var timer in due)
        {
            timer.Fire();
        }
    }

    /// <summary>A due delay whose timers are scheduled as usual but never reported by
    /// <see cref="NextTimerAsync"/> — for timers a test must not advance to on purpose.</summary>
    public TimeSpan? Unannounced { get; set; }

    /// <summary>The due delay of the next timer created (in creation order).</summary>
    public Task<TimeSpan> NextTimerAsync() => _created.Reader.ReadAsync().AsTask();

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        var timer = new ManualTimer(this, callback, state);
        timer.Change(dueTime, period);
        return timer;
    }

    private void Schedule(ManualTimer timer, TimeSpan dueTime)
    {
        lock (_lock)
        {
            _timers.Remove(timer);
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                return;
            }

            timer.DueAt = _utcNow + dueTime;
            _timers.Add(timer);
        }

        if (dueTime != Unannounced)
        {
            _created.Writer.TryWrite(dueTime);
        }
    }

    private void Unschedule(ManualTimer timer)
    {
        lock (_lock)
        {
            _timers.Remove(timer);
        }
    }

    /// <summary>One-shot only: a period is ignored (Task.Delay never uses one).</summary>
    private sealed class ManualTimer(ManualClock clock, TimerCallback callback, object? state) : ITimer
    {
        public DateTimeOffset DueAt { get; set; }

        public void Fire() => callback(state);

        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            clock.Schedule(this, dueTime);
            return true;
        }

        public void Dispose() => clock.Unschedule(this);

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
