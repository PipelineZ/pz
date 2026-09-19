namespace Pz.State.Http.Tests;

/// <summary>Records every delay asked of it and, unless told to hold, lets it elapse at once -- so a
/// retry test proves WHICH delay was requested without ever waiting for it.</summary>
internal sealed class RecordingTimeProvider(bool hold = false) : TimeProvider
{
    private readonly List<TimeSpan> _delays = [];

    public IReadOnlyList<TimeSpan> Delays
    {
        get { lock (_delays) { return [.. _delays]; } }
    }

    /// <summary>Runs as a delay starts -- the one moment a test can act "while the caller is waiting".</summary>
    public Action? OnDelay { get; set; }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        lock (_delays) { _delays.Add(dueTime); }
        OnDelay?.Invoke();
        if (!hold)
        {
            ThreadPool.QueueUserWorkItem(_ => callback(state));
        }

        return new NoTimer();
    }

    private sealed class NoTimer : ITimer
    {
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
