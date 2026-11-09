namespace Pz.Connector.Http;

/// <summary>The not-before time one top-level read shares across all of its requests (pages, the
/// for_each list, items, downloads). A 429/503 <c>Retry-After</c> answering any of them moves it
/// forward, and every later request waits it out, so a throttled API is not hit again by a sibling
/// request while the one that was throttled is still backing off. It only ever moves forward, and one
/// hint holds the read for at most <see cref="MaxPause"/>. Waits run on the injected
/// <see cref="TimeProvider"/> and are cancellable; nothing waits once the time has passed.</summary>
internal sealed class ThrottlePause(TimeProvider time)
{
    /// <summary>The longest pause a single <c>Retry-After</c> can impose.</summary>
    private static readonly TimeSpan MaxPause = TimeSpan.FromSeconds(600);

    // UTC ticks of the not-before time; 0 means none was ever set. Read and moved with Interlocked
    // because concurrent requests of one read note and wait on it at the same time.
    private long _notBeforeTicks;

    /// <summary>Moves the not-before time to <c>max(current, now + min(retryAfter, 600 s))</c>. A zero
    /// or negative hint (a <c>Retry-After</c> date already past) changes nothing.</summary>
    public void Note(TimeSpan retryAfter)
    {
        if (retryAfter <= TimeSpan.Zero)
        {
            return;
        }

        var until = (time.GetUtcNow() + (retryAfter < MaxPause ? retryAfter : MaxPause)).UtcTicks;
        var current = Interlocked.Read(ref _notBeforeTicks);
        while (until > current)
        {
            var seen = Interlocked.CompareExchange(ref _notBeforeTicks, until, current);
            if (seen == current)
            {
                return;
            }

            current = seen;
        }
    }

    /// <summary>Completes once the not-before time has passed: synchronously when it already has, so an
    /// unthrottled read never starts a timer. A pause moved further while waiting is waited out too.</summary>
    public Task WaitAsync(CancellationToken ct)
    {
        var remaining = Remaining();
        return remaining > TimeSpan.Zero ? WaitCoreAsync(remaining, ct) : Task.CompletedTask;
    }

    private async Task WaitCoreAsync(TimeSpan remaining, CancellationToken ct)
    {
        do
        {
            await Task.Delay(remaining, time, ct).ConfigureAwait(false);
            remaining = Remaining();
        }
        while (remaining > TimeSpan.Zero);
    }

    private TimeSpan Remaining()
        => TimeSpan.FromTicks(Interlocked.Read(ref _notBeforeTicks) - time.GetUtcNow().UtcTicks);
}
