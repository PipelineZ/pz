using Pz.Core.Model;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Resilience;

/// <summary>The Closed -&gt; Open -&gt; HalfOpen breaker state machine. Uses the
/// <see cref="ManualTimeProvider"/> double — no wall-clock sleeps, every assertion is against
/// deterministic ticks advanced by the test.
///
/// <para>Epoch-ticket coverage: <see cref="TryEnter"/> hands back a ticket alongside admission; only a
/// current-epoch ticket's <c>Record*</c> call can affect state. Tests below exercise both the happy path
/// (probe's own ticket resolves half_open) and the stale-ticket paths (a caller admitted under an earlier
/// epoch reporting an outcome after the breaker has already moved on).</para></summary>
public class CircuitBreakerTests
{
    /// <summary><c>_epoch</c> must start at 1, not 0 -- otherwise a breaker's
    /// very first Closed-state admission (before any trip ever occurs) would hand back ticket 0, which is
    /// bitwise identical to <see cref="CircuitBreaker.TryEnter"/>'s denied-sentinel value. A caller that
    /// (incorrectly, but plausibly) treated ticket 0 as "no admission happened" would then silently drop a
    /// legitimate first-ever RecordSuccess/RecordTransientFailure call. Starting at 1 makes 0 unconditionally
    /// inert as a ticket value.</summary>
    [Fact]
    public void First_ever_admission_ticket_is_never_the_denied_sentinel_zero()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        Assert.NotEqual(0, ticket);
    }

    [Fact]
    public void Stays_closed_below_threshold_and_trips_at_exactly_threshold()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(3, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null); // 1 of 3
        Assert.True(breaker.TryEnter(out var stillClosed1, out ticket));
        Assert.Equal(TimeSpan.Zero, stillClosed1);

        breaker.RecordTransientFailure(ticket, null); // 2 of 3 (threshold - 1): must still be closed
        Assert.True(breaker.TryEnter(out var stillClosed2, out ticket));
        Assert.Equal(TimeSpan.Zero, stillClosed2);

        breaker.RecordTransientFailure(ticket, null); // 3 of 3: trips

        Assert.False(breaker.TryEnter(out var afterTrip, out _));
        Assert.Equal(TimeSpan.FromMinutes(1), afterTrip);
    }

    [Fact]
    public void Success_mid_streak_resets_the_consecutive_failure_count()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(3, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null);
        breaker.RecordTransientFailure(ticket, null);
        breaker.RecordSuccess(ticket); // resets the streak to 0
        breaker.RecordTransientFailure(ticket, null);
        breaker.RecordTransientFailure(ticket, null); // only 2 consecutive since the reset -- below threshold

        Assert.True(breaker.TryEnter(out var retryIn, out _));
        Assert.Equal(TimeSpan.Zero, retryIn);
    }

    [Fact]
    public void Open_rejects_with_the_correct_remaining_cool_down()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null); // threshold 1 -> trips immediately
        time.Advance(TimeSpan.FromSeconds(20));

        Assert.False(breaker.TryEnter(out var retryIn, out _));
        Assert.Equal(TimeSpan.FromSeconds(40), retryIn);
    }

    [Fact]
    public void Exactly_one_of_two_try_enters_wins_the_probe_after_cool_down()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null);
        time.Advance(TimeSpan.FromMinutes(1));

        var first = breaker.TryEnter(out var firstRetry, out var probeTicket);
        var second = breaker.TryEnter(out var secondRetry, out _);

        Assert.True(first);
        Assert.Equal(TimeSpan.Zero, firstRetry);
        Assert.NotEqual(0, probeTicket); // the probe carries a real, post-trip epoch ticket

        Assert.False(second);
        // HalfOpen has no timer of its own -- the documented hint is the full CoolDown, not a remaining slice.
        Assert.Equal(TimeSpan.FromMinutes(1), secondRetry);
    }

    [Fact]
    public void Probe_success_closes_the_breaker_and_resets_the_count()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null);
        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // wins the probe -> HalfOpen

        breaker.RecordSuccess(probeTicket);

        Assert.True(breaker.TryEnter(out var retryIn, out _)); // Closed again
        Assert.Equal(TimeSpan.Zero, retryIn);
    }

    [Fact]
    public void Probe_failure_reopens_for_a_fresh_cool_down()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null);
        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // wins the probe -> HalfOpen

        breaker.RecordTransientFailure(probeTicket, null); // probe fails -> fresh Open

        Assert.False(breaker.TryEnter(out var retryIn, out _));
        Assert.Equal(TimeSpan.FromMinutes(1), retryIn); // fresh full cool-down, not a stale remainder

        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out var afterFreshCoolDown, out _));
        Assert.Equal(TimeSpan.Zero, afterFreshCoolDown);
    }

    [Fact]
    public void Retry_after_floor_longer_than_cool_down_extends_the_open_duration()
    {
        var time = new ManualTimeProvider();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, TimeSpan.FromMinutes(10)); // floor(10m) > config.CoolDown(1m)

        time.Advance(TimeSpan.FromMinutes(1)); // cool_down alone would have elapsed by now
        Assert.False(breaker.TryEnter(out var retryIn, out _));
        Assert.Equal(TimeSpan.FromMinutes(9), retryIn);

        time.Advance(TimeSpan.FromMinutes(9));
        Assert.True(breaker.TryEnter(out var afterFloor, out _));
        Assert.Equal(TimeSpan.Zero, afterFloor);
    }

    [Fact]
    public void State_change_callback_fires_with_correct_triples_in_order_and_not_under_the_lock()
    {
        var time = new ManualTimeProvider();
        var events = new List<(string Old, string New, string Trigger, TimeSpan CoolDown)>();
        CircuitBreaker? breaker = null;

        breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time,
            (oldState, newState, trigger, coolDown) =>
            {
                events.Add((oldState, newState, trigger, coolDown));

                // Reentrant call from inside the callback: this would deadlock a plain lock if
                // onStateChanged fired while the lock was still held.
                breaker!.TryEnter(out _, out _);
            });

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null); // closed -> open
        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // open -> half_open (wins the probe)
        breaker.RecordSuccess(probeTicket); // half_open -> closed

        Assert.Equal(3, events.Count);
        // CoolDown is only meaningful for a transition INTO open (the value the
        // executor gate will wait on next) -- every other transition carries TimeSpan.Zero, documented on
        // BreakerRegistry/CircuitBreaker's onStateChanged doc comments.
        Assert.Equal(("closed", "open", "1 consecutive transient failure", TimeSpan.FromMinutes(1)), events[0]);
        Assert.Equal(("open", "half_open", "cool-down elapsed", TimeSpan.Zero), events[1]);
        Assert.Equal(("half_open", "closed", "probe succeeded", TimeSpan.Zero), events[2]);
    }

    [Fact]
    public void Threshold_above_one_pluralizes_the_trip_trigger()
    {
        var time = new ManualTimeProvider();
        var events = new List<(string Old, string New, string Trigger, TimeSpan CoolDown)>();
        var breaker = new CircuitBreaker(new BreakerConfig(2, TimeSpan.FromMinutes(1)), time,
            (oldState, newState, trigger, coolDown) => events.Add((oldState, newState, trigger, coolDown)));

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null); // 1 of 2
        breaker.RecordTransientFailure(ticket, null); // 2 of 2: trips

        Assert.Single(events);
        Assert.Equal(("closed", "open", "2 consecutive transient failures", TimeSpan.FromMinutes(1)), events[0]);
    }

    [Fact]
    public void Stale_success_in_half_open_is_a_no_op()
    {
        var time = new ManualTimeProvider();
        var events = new List<(string Old, string New, string Trigger, TimeSpan CoolDown)>();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time,
            (oldState, newState, trigger, coolDown) => events.Add((oldState, newState, trigger, coolDown)));

        // The ORIGINAL caller: admitted while Closed, carries the pre-trip epoch.
        Assert.True(breaker.TryEnter(out _, out var staleTicket));

        // A different Closed-epoch caller trips the breaker.
        Assert.True(breaker.TryEnter(out _, out var otherTicket));
        breaker.RecordTransientFailure(otherTicket, null); // closed -> open; staleTicket's epoch is now stale

        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // open -> half_open (wins the probe)
        events.Clear();

        breaker.RecordSuccess(staleTicket); // stale ticket: must be a no-op

        Assert.Empty(events); // no state-change callback fired
        // If the stale success had spuriously closed the breaker, this TryEnter would succeed (Closed
        // admits unconditionally). It doesn't -- the breaker is still half_open with the probe unresolved.
        Assert.False(breaker.TryEnter(out _, out _));

        // The probe itself is unaffected and can still resolve normally.
        breaker.RecordSuccess(probeTicket);
        Assert.Single(events);
        Assert.Equal(("half_open", "closed", "probe succeeded", TimeSpan.Zero), events[0]);
    }

    [Fact]
    public void Stale_failure_in_half_open_is_a_no_op()
    {
        var time = new ManualTimeProvider();
        var events = new List<(string Old, string New, string Trigger, TimeSpan CoolDown)>();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time,
            (oldState, newState, trigger, coolDown) => events.Add((oldState, newState, trigger, coolDown)));

        Assert.True(breaker.TryEnter(out _, out var staleTicket)); // original caller, admitted while closed

        Assert.True(breaker.TryEnter(out _, out var otherTicket));
        breaker.RecordTransientFailure(otherTicket, null); // closed -> open; staleTicket's epoch is now stale

        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // open -> half_open (wins the probe)
        events.Clear();

        breaker.RecordTransientFailure(staleTicket, null); // stale ticket: must NOT reopen the breaker

        Assert.Empty(events); // no reopen callback -- the probe is unaffected

        // The probe itself is still live and resolves normally.
        breaker.RecordSuccess(probeTicket);
        Assert.Single(events);
        Assert.Equal(("half_open", "closed", "probe succeeded", TimeSpan.Zero), events[0]);
    }

    [Fact]
    public void Probe_failure_reopens_with_a_bumped_epoch_and_ignores_a_followup_stale_ticket()
    {
        var time = new ManualTimeProvider();
        var events = new List<(string Old, string New, string Trigger, TimeSpan CoolDown)>();
        var breaker = new CircuitBreaker(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time,
            (oldState, newState, trigger, coolDown) => events.Add((oldState, newState, trigger, coolDown)));

        Assert.True(breaker.TryEnter(out _, out var ticket));
        breaker.RecordTransientFailure(ticket, null); // closed -> open
        time.Advance(TimeSpan.FromMinutes(1));
        Assert.True(breaker.TryEnter(out _, out var probeTicket)); // open -> half_open
        events.Clear();

        breaker.RecordTransientFailure(probeTicket, null); // probe fails, using its OWN ticket -> reopens

        Assert.Single(events);
        Assert.Equal(("half_open", "open", "probe failed", TimeSpan.FromMinutes(1)), events[0]);

        // The epoch bumped on this reopen -- the same probeTicket is now stale and must be a no-op.
        events.Clear();
        breaker.RecordTransientFailure(probeTicket, null);
        breaker.RecordSuccess(probeTicket);

        Assert.Empty(events);
        Assert.False(breaker.TryEnter(out var retryIn, out _)); // still open, fresh cool-down
        Assert.Equal(TimeSpan.FromMinutes(1), retryIn);
    }
}
