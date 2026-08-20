using System.Diagnostics;
using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Checks;
using Pz.Engine.Resilience;

namespace Pz.Engine.Execution;

/// <summary>Dispatches by NodeKind; wraps executor exceptions into a Failed NodeResult carrying
/// PZ0501 (message = underlying exception message; OperationCanceledException is NOT wrapped — it
/// propagates so the dispatcher can distinguish cancellation from failure).
/// <see cref="RunOrchestrator"/> is the SOLE publisher of <see cref="IRunEvents.NodeCompleted"/> — this
/// executor must not also fire it, or every node's completion would be reported twice.
///
/// Retries live HERE, wrapped around the inner executor call, because this
/// is the one place a thrown <see cref="PzConnectorException"/> still carries <c>IsTransient</c>/
/// <c>RetryAfter</c> — any outer decorator would only ever see the already-wrapped PZ0501
/// <see cref="NodeResult"/> below, with transience erased. Only <see cref="PzConnectorException"/> with
/// <see cref="PzConnectorException.IsTransient"/> true retries (up to the resolved policy's
/// <c>MaxAttempts</c> — a non-null <paramref name="policy"/> ctor argument is an explicit override used
/// for every node; when null (production), <see cref="RetryPolicyResolver.Resolve(DagNode)"/> resolves
/// per node from its owning source/sink instance config, cascading to <see cref="RetryPolicy.Default"/>);
/// every other exception — including a transient one that has exhausted its
/// attempts — falls through to the same PZ0501-wrapping path. Cancellation
/// (<see cref="OperationCanceledException"/>), whether from the node itself or from the backoff delay,
/// still propagates uncaught. <see cref="NodeStarted"/> fires exactly once per node regardless of retry
/// count — a retry is silent at this boundary; <see cref="IRunEvents.RetryScheduled"/> is the visible
/// signal, fired once per scheduled retry, before its delay. The final <see cref="NodeResult.Duration"/>
/// covers every attempt (the stopwatch starts once, before the first try).
///
/// Retry safety (full narrative in https://pipelinez.dev/events/'s "Retry safety" section, next to
/// `retry_scheduled`): only <see cref="NodeKind.SourceLoad"/> and <see cref="NodeKind.SinkWrite"/> nodes
/// are ever retried in practice, and both are safe to retry. SourceLoad retry safety has two regimes.
/// Legacy (no StablePartitionIds): ingest is all-or-nothing — IngestArrowAsync drops the target staging
/// table on any failure, so a retried attempt always starts from a clean table. Partition mode:
/// completed partitions' rows persist in the main staging table across attempts BY DESIGN, with the
/// pz_meta ledger recording exactly which partitions they came from (updated atomically with the data);
/// a retried attempt consults the ledger and re-runs only what is missing, so the table is never partial
/// with respect to what the ledger claims. SinkWrite is safe because
/// <see cref="SinkWriteExecutor"/> opens a fresh write session per attempt, aborts that session on any
/// write-phase failure before the exception propagates (commit is attempted at most once, after every
/// batch has been written), and connectors that write durable files do so via temp-write-then-atomic-move
/// (mirroring <see cref="Pz.Engine.Artifacts.RunResultsWriter"/>'s own pattern) — so a failed attempt
/// never leaves a partially-visible output for the next attempt to build on. <see cref="NodeKind.Pipeline"/>
/// (and <see cref="NodeKind.Check"/>) nodes are never retried in practice: their failures come from plain
/// DuckDB SQL exceptions, which are not <see cref="PzConnectorException"/>, so the `when (ex.IsTransient
/// ...)` guard below never matches them — they fall straight through to the PZ0501-wrapping path.
///
/// A per-instance <see cref="Resilience.CircuitBreaker"/> (resolved once,
/// via <see cref="RunContext.Breakers"/>) gates every attempt, including the first -- <c>null</c> (no
/// <c>engine.breaker:</c> configured, or a Pipeline/Check node with no owning instance) makes the gate a
/// complete no-op. When
/// non-null, the gate sits at the TOP of every loop iteration (not just the first): a probe that fails
/// transiently and is still eligible for a policy retry reopens the breaker via
/// <see cref="Resilience.CircuitBreaker.RecordTransientFailure"/> BEFORE that retry's next iteration begins,
/// so the next iteration's gate re-enters <see cref="Resilience.CircuitBreaker.TryEnter"/> exactly like a
/// fresh node dispatch would. <c>openWaits</c> is scoped to the whole node execution (declared outside the
/// attempt loop), not reset per attempt, so <see cref="MaxOpenWaits"/> bounds TOTAL open-wait time across
/// every attempt to <c>MaxOpenWaits × cool-down</c> -- the no-hang guarantee.</summary>
public sealed class KindDispatchingExecutor(
    RetryPolicy? policy = null, Random? jitter = null, Func<TimeSpan, CancellationToken, Task>? delay = null)
    : INodeExecutor
{
    /// <summary>Bounds total gate-wait time to <c>MaxOpenWaits × cool-down</c> per node execution (across
    /// every attempt) -- past this, the node gives up with PZ0506 rather than waiting indefinitely for a
    /// breaker that may stay open far longer than any one run should block on.</summary>
    private const int MaxOpenWaits = 2;

    private readonly SourceLoadExecutor _sourceLoad = new();
    private readonly PipelineExecutor _pipeline = new();
    private readonly CheckExecutor _check = new();
    private readonly SinkWriteExecutor _sinkWrite = new();
    private readonly RetryPolicy? _policyOverride = policy;
    private readonly Random _jitter = jitter ?? Random.Shared;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay = delay ?? Task.Delay;

    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        ctx.Events.SafeNodeStarted(node);
        var stopwatch = Stopwatch.StartNew();
        var executor = Resolve(node.Kind);

        // A non-null ctor policy is an explicit override (tests, callers that
        // want one policy for everything); otherwise the node's owning source/sink instance decides.
        var policy = _policyOverride ?? RetryPolicyResolver.Resolve(node);

        var breaker = ctx.Breakers?.For(node);

        var attempt = 1;
        var openWaits = 0;
        while (true)
        {
            var ticket = 0L;
            if (breaker is not null)
            {
                while (!breaker.TryEnter(out var retryIn, out ticket))
                {
                    if (++openWaits > MaxOpenWaits)
                    {
                        return BreakerOpenResult(node, stopwatch, retryIn);
                    }

                    // Same non-attempt-consuming wait as the retry backoff below: an OperationCanceledException
                    // from this delay (e.g. the run being cancelled mid-wait) must propagate uncaught.
                    await _delay(retryIn, ct).ConfigureAwait(false);
                }
            }

            try
            {
                var inner = await executor.ExecuteAsync(node, ctx, ct).ConfigureAwait(false);
                breaker?.RecordSuccess(ticket);
                return inner with { Duration = stopwatch.Elapsed };
            }
            catch (OperationCanceledException)
            {
                // Propagates uncaught: the dispatcher must be able to distinguish cancellation from a
                // genuine node failure, so this is never turned into a Failed NodeResult, retried, or
                // otherwise swallowed.
                throw;
            }
            catch (PzConnectorException ex) when (ex.IsTransient && attempt < policy.MaxAttempts)
            {
                breaker?.RecordTransientFailure(ticket, ex.RetryAfter);

                var delayDuration = policy.ComputeDelay(attempt, ex.RetryAfter, _jitter);

                // Defensive: ex.Message is a connector-authored string, but nothing
                // stops a connector from echoing a raw engine error (e.g. wrapping a DuckDB
                // parser/binder failure) into it -- exactly the "LINE <n>: ..." verbatim-statement-echo
                // shape SanitizeEngineMessage exists to strip (see SourceLoadExecutor/SinkWriteExecutor's
                // native-path usage). RetryScheduled's `reason` reaches NDJSON/console verbatim, so it
                // must never carry an unsanitized message through to those sinks.
                var reason = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
                ctx.Events.SafeRetryScheduled(node, attempt, policy.MaxAttempts, delayDuration, reason);

                // Not wrapped in try/catch: an OperationCanceledException from the backoff wait (e.g. the
                // run being cancelled mid-delay) must propagate exactly like one from the node itself,
                // never surface as a Failed NodeResult.
                await _delay(delayDuration, ct).ConfigureAwait(false);
                attempt++;
            }
            catch (PzConnectorException ex) when (ex.IsTransient)
            {
                // Attempts exhausted, but still a transient failure: report it to the breaker exactly
                // like the retry-eligible case above, just without a scheduled retry to follow.
                breaker?.RecordTransientFailure(ticket, ex.RetryAfter);
                return TerminalFailure(node, ctx, stopwatch, ex);
            }
            catch (Exception ex)
            {
                // Non-transient (or a foreign exception entirely): no breaker interaction -- only
                // PzConnectorException.IsTransient failures are the breaker's concern.
                return TerminalFailure(node, ctx, stopwatch, ex);
            }
        }
    }

    /// <summary>A raw/foreign exception's ex.Message may echo a raw engine error verbatim (a connector wrapping a DuckDB
    /// parser/binder failure), and this terminal wrap is the message run_results.json/NDJSON ultimately
    /// surface, so it must never carry that unsanitized text through. A Pz-family exception (see
    /// <see cref="MessageRedaction.Redact(Exception)"/>'s trust boundary doc) is passed through unredacted
    /// instead, since its message is already developer-authored and sanitized at the native site before
    /// wrapping.</summary>
    private static NodeResult TerminalFailure(DagNode node, RunContext ctx, Stopwatch stopwatch, Exception ex)
    {
        // Consume (TryRemove) the executor's delivery side-band so the one
        // terminal Failed result carries it; removal also keeps a later unrelated failure of a
        // different node from ever seeing stale state.
        ctx.DeliveryFailures.TryRemove(node.Id, out var delivery);
        // Guidance is derived from the REDACTED message, never the raw
        // exception -- this wrap is exactly the trust boundary described above, and the appended strings
        // are developer-authored constants, so nothing unsanitized can re-enter through them.
        var error = new PzError(PzErrorCode.NodeFailed,
            NodeFailureGuidance.Annotate(MessageRedaction.Redact(ex)), null, null, null);
        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, stopwatch.Elapsed, error,
            Delivery: delivery);
    }

    /// <summary>The bounded give-up path (PZ0506): the breaker for <paramref name="node"/>'s owning
    /// instance stayed Open (or HalfOpen-but-denied) through every one of <see cref="MaxOpenWaits"/> gate
    /// waits -- the connector was never invoked for this attempt at all.</summary>
    private static NodeResult BreakerOpenResult(DagNode node, Stopwatch stopwatch, TimeSpan retryIn)
    {
        var instanceKey = InstanceKey.For(node) ?? "unknown";
        var error = new PzError(PzErrorCode.BreakerOpen,
            $"circuit breaker for instance '{instanceKey}' is open (cool-down {FormatRetryIn(retryIn)}); the connector was not invoked.",
            null, null,
            "the connector's circuit breaker is open; retry with 'pz retry' after the cool-down, or raise engine.breaker thresholds");
        return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, stopwatch.Elapsed, error);
    }

    /// <summary>Renders config-style seconds (`120s`), not <see cref="TimeSpan"/>'s
    /// default `ToString()` (`00:01:00`) -- mirrors <c>ConsoleRenderer.FormatCoolDown</c>'s derivation
    /// from <see cref="Pz.Diagnostics.Events.BreakerStateChangedEvent.CoolDownMs"/> (whole seconds print
    /// bare; a sub-second remainder keeps up to 3 decimal places).</summary>
    private static string FormatRetryIn(TimeSpan retryIn) =>
        $"{retryIn.TotalSeconds.ToString("0.###", CultureInfo.InvariantCulture)}s";

    private INodeExecutor Resolve(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => _sourceLoad,
        NodeKind.Pipeline => _pipeline,
        NodeKind.Check => _check,
        NodeKind.SinkWrite => _sinkWrite,
        _ => throw new NotSupportedException($"no executor registered for node kind '{kind}'"),
    };
}
