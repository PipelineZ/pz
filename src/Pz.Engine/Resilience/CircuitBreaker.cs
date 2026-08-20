using Pz.Core.Model;

namespace Pz.Engine.Resilience;

/// <summary>One instance's Closed -&gt; Open -&gt; HalfOpen machine. Engine-owned —
/// connectors never see it (architecture §14's single-retry-owner rule extends to breaking). Thread-safe
/// via a plain lock; all time through <see cref="TimeProvider"/> (<c>GetTimestamp</c>/<c>GetElapsedTime</c>
/// — never <c>DateTime</c>/<c>Stopwatch</c>). State changes surface
/// through <c>onStateChanged(old, new, trigger, coolDown)</c> — wired to the event bus by the registry.
/// <c>coolDown</c> is only meaningful for a transition INTO <see cref="OpenState"/> (the wait
/// the executor gate will honor next, the greater of <see cref="BreakerConfig.CoolDown"/> and any
/// <c>RetryAfter</c> floor); every other transition (Open -&gt; HalfOpen, HalfOpen -&gt; Closed) passes
/// <see cref="TimeSpan.Zero"/> instead — there is no fresh wait to report for those.
///
/// <para><b>Half-open probe design:</b> HalfOpen grants exactly ONE probe. The <see cref="TryEnter"/> call
/// that observes the cool-down has elapsed is the one that performs the Open -&gt; HalfOpen transition —
/// that same call's `true` return IS the probe grant, so there is no separate "take the slot" step to race.
/// Any other <see cref="TryEnter"/> landing in HalfOpen before the probe resolves (via
/// <see cref="RecordSuccess"/>/<see cref="RecordTransientFailure"/>) gets `false`. HalfOpen deliberately
/// runs no timer of its own — the probe either resolves (closing or reopening the breaker) or it doesn't;
/// there is no "elapsed" to subtract from anything. So a denied caller in HalfOpen is told to wait the
/// full <see cref="BreakerConfig.CoolDown"/> again, the same order-of-magnitude hint Open gives a caller
/// arriving right at the start of its cool-down — an honest "try again later", not a fabricated countdown.</para>
///
/// <para><b>Epoch tickets — probe-only resolution:</b> <see cref="TryEnter(out TimeSpan, out long)"/> hands
/// back a <c>ticket</c> alongside admission: the current <c>_epoch</c>, an internal counter that increments
/// on EVERY transition to <see cref="OpenState"/>. That ticket must be passed back into
/// <see cref="RecordSuccess"/>/<see cref="RecordTransientFailure"/>; a ticket is stale whenever it no longer
/// equals <c>_epoch</c>, and BOTH <c>Record*</c> methods treat a stale ticket as an unconditional no-op —
/// which covers, among other cases, a failure report arriving after the breaker already re-opened.
/// Consequences that follow from this by construction:
/// callers admitted while Closed carry that epoch's ticket, so once a trip bumps the epoch their eventual
/// outcome is silently ignored — a stale success can no longer mask an outage by spuriously closing the
/// breaker, and a stale failure can no longer prevent recovery by spuriously reopening it. The HalfOpen
/// probe's ticket is the post-trip epoch set at the moment of the Open -&gt; HalfOpen transition, and since
/// every non-probe <see cref="TryEnter"/> in HalfOpen is refused (see above), the probe's ticket is the ONLY
/// live ticket while HalfOpen — so "only the probe's own outcome can resolve HalfOpen" holds without any
/// extra bookkeeping. A current-epoch success while Closed resets <c>_consecutiveFailures</c>.
/// A denied <see cref="TryEnter(out TimeSpan, out long)"/> sets <c>ticket = 0</c>; that value is
/// undefined/reserved and callers must never pass it to a <c>Record*</c> method (there was no admission to
/// report an outcome for). <c>_epoch</c> starts at 1 (not 0) specifically so this sentinel stays
/// unconditionally inert — a genuine Closed-state admission's ticket (the current epoch) can never collide
/// with the denied sentinel, even for a breaker that has never tripped.</para>
///
/// <para><b>Thread safety:</b> all state is guarded by a plain <see cref="Lock"/>.
/// <c>onStateChanged</c> is always invoked OUTSIDE the lock — a
/// subscriber publishing an event (or, worse, calling back into this breaker) must never be able to
/// deadlock against it.</para></summary>
public sealed class CircuitBreaker(BreakerConfig config, TimeProvider time,
    Action<string, string, string, TimeSpan>? onStateChanged = null)
{
    private const string ClosedState = "closed";
    private const string OpenState = "open";
    private const string HalfOpenState = "half_open";

    private readonly Lock _gate = new();
    private string _state = ClosedState;
    private int _consecutiveFailures;
    private long _openedAt;
    private TimeSpan _coolDown;

    // Starts at 1, not 0, so ticket 0 -- TryEnter's denied sentinel --
    // is unconditionally inert and can never collide with a genuine Closed-state admission ticket (which
    // would otherwise BE 0 for every breaker before its first-ever trip).
    private long _epoch = 1;

    /// <summary><c>ticket</c> is the caller's epoch: pass it back unchanged into
    /// <see cref="RecordSuccess"/>/<see cref="RecordTransientFailure"/> to report this admission's outcome.
    /// When <c>entered</c> is `false`, <c>ticket</c> is `0` (undefined/reserved) — do not use it.</summary>
    public bool TryEnter(out TimeSpan retryIn, out long ticket)
    {
        (string Old, string New, string Trigger, TimeSpan CoolDown)? transition = null;
        bool entered;
        var wait = TimeSpan.Zero;
        var grantedTicket = 0L;

        lock (_gate)
        {
            switch (_state)
            {
                case ClosedState:
                    entered = true;
                    grantedTicket = _epoch;
                    break;

                case OpenState:
                    var elapsed = time.GetElapsedTime(_openedAt);
                    if (elapsed >= _coolDown)
                    {
                        // Not a transition INTO open -- carries TimeSpan.Zero (see class doc).
                        transition = (OpenState, HalfOpenState, "cool-down elapsed", TimeSpan.Zero);
                        _state = HalfOpenState;
                        entered = true;
                        grantedTicket = _epoch; // post-trip epoch: the probe's ticket.
                    }
                    else
                    {
                        wait = _coolDown - elapsed;
                        entered = false;
                    }
                    break;

                default: // HalfOpenState: the probe slot was already granted to the TryEnter that got here.
                    wait = _coolDown;
                    entered = false;
                    break;
            }
        }

        retryIn = entered ? TimeSpan.Zero : wait;
        ticket = entered ? grantedTicket : 0;

        if (transition is { } t)
        {
            onStateChanged?.Invoke(t.Old, t.New, t.Trigger, t.CoolDown);
        }

        return entered;
    }

    public void RecordSuccess(long ticket)
    {
        (string Old, string New, string Trigger, TimeSpan CoolDown)? transition = null;

        lock (_gate)
        {
            if (ticket == _epoch)
            {
                _consecutiveFailures = 0;
                if (_state == HalfOpenState)
                {
                    // Not a transition INTO open -- carries TimeSpan.Zero (see class doc).
                    transition = (HalfOpenState, ClosedState, "probe succeeded", TimeSpan.Zero);
                    _state = ClosedState;
                }
            }
            // else: stale ticket -- see the class doc's "Epoch tickets" section. No-op by design.
        }

        if (transition is { } t)
        {
            onStateChanged?.Invoke(t.Old, t.New, t.Trigger, t.CoolDown);
        }
    }

    public void RecordTransientFailure(long ticket, TimeSpan? retryAfterFloor)
    {
        (string Old, string New, string Trigger, TimeSpan CoolDown)? transition = null;

        lock (_gate)
        {
            if (ticket == _epoch)
            {
                switch (_state)
                {
                    case HalfOpenState:
                        transition = TransitionToOpen(retryAfterFloor, "probe failed");
                        break;

                    case OpenState:
                        // Unreachable in practice: any transition to Open bumps _epoch atomically with the
                        // state change, so a ticket that still matches _epoch cannot observe OpenState.
                        // Kept for defensive clarity alongside the ClosedState/HalfOpenState cases.
                        break;

                    default: // ClosedState
                        _consecutiveFailures++;
                        if (_consecutiveFailures >= config.FailureThreshold)
                        {
                            var word = config.FailureThreshold == 1 ? "failure" : "failures";
                            transition = TransitionToOpen(retryAfterFloor,
                                $"{config.FailureThreshold} consecutive transient {word}");
                        }
                        break;
                }
            }
            // else: stale ticket -- see the class doc's "Epoch tickets" section. No-op by design.
        }

        if (transition is { } t)
        {
            onStateChanged?.Invoke(t.Old, t.New, t.Trigger, t.CoolDown);
        }
    }

    // Must be called while holding _gate. The returned CoolDown is _coolDown as just (re)computed below --
    // the actual value the executor gate's next TryEnter will wait on for this transition INTO open.
    private (string Old, string New, string Trigger, TimeSpan CoolDown) TransitionToOpen(TimeSpan? retryAfterFloor, string trigger)
    {
        var old = _state;
        var floor = retryAfterFloor ?? TimeSpan.Zero;
        _coolDown = config.CoolDown > floor ? config.CoolDown : floor;
        _openedAt = time.GetTimestamp();
        _consecutiveFailures = 0;
        _epoch++;
        _state = OpenState;
        return (old, OpenState, trigger, _coolDown);
    }
}
