using System.Diagnostics.CodeAnalysis;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;
using Pz.Engine.Dispatch;
using Pz.Engine.Tests.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>The engine retry loop lives inside <see cref="KindDispatchingExecutor"/>
/// (see its doc comment for why). Every test here injects a zero-delay seam (a no-op
/// <c>Func&lt;TimeSpan, CancellationToken, Task&gt;</c>) so backoff never actually waits — retry
/// correctness is proven via attempt/event counts, not wall-clock timing.</summary>
public sealed class RetryingExecutionTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private InMemoryConnector _mem = null!;
    private RunContext _ctx = null!;
    private RecordingEvents _events = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _mem = new InMemoryConnector();
        var reg = new ConnectorRegistry();
        reg.AddSource("inmemory", _mem);
        reg.AddSink("inmemory", _mem);
        _events = new RecordingEvents();
        _ctx = new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), _events);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceLoadNode(long rows, Dictionary<string, object?>? extra = null, RetryDef? retry = null,
        string dataset = "numbers")
    {
        var options = new Dictionary<string, object?> { ["rows"] = rows };
        foreach (var (k, v) in extra ?? []) options[k] = v;
        // "mem" is fixed (the breaker/instance-key tests below rely on every node sharing one source
        // instance) -- `dataset` defaults to "numbers", but a test that
        // dispatches multiple SUCCESSFUL loads of the same source in sequence must vary it,
        // since a completed load leaves its staging table behind (only a FAILED attempt drops it) and a
        // second CREATE TABLE against the same source+dataset name collides.
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, options, null)], "sources/mem.yml", retry);
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, $"src_mem__{dataset}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    /// <summary>Seam for native-tier retry classification: a SourceLoad node whose connector is the
    /// native-scan-only <see cref="ConfigurableNativeSource"/> below, distinct from
    /// <see cref="SourceLoadNode"/>'s fixed "mem"/inmemory (universal-path) instance.</summary>
    private static DagNode NativeSourceLoadNode(string dataset = "t")
    {
        var source = new ConnectionDef("nativemem", "nativestub", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, new Dictionary<string, object?>(), null)], "sources/nativemem.yml");
        return new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SourceLoad, $"src_nativemem__{dataset}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static KindDispatchingExecutor NoDelayExecutor(RetryPolicy? policy = null, Random? jitter = null) =>
        new(policy, jitter ?? new FixedRandom(0.5), delay: (_, _) => Task.CompletedTask);

    [Fact]
    public async Task Transient_fails_twice_then_succeeds()
    {
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(2),
        });

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1_000, result.RowsMoved);
        Assert.Equal(2, _events.RetryScheduledCalls.Count);
        Assert.Equal(1, _events.RetryScheduledCalls[0].Attempt);
        Assert.Equal(2, _events.RetryScheduledCalls[1].Attempt);
        Assert.All(_events.RetryScheduledCalls, call => Assert.Equal(3, call.MaxAttempts));
        Assert.Equal(1, _events.NodeStartedCount); // never double-fired across retries
    }

    /// <summary>Proves <see cref="DuckTransientErrors.IsTransient"/> flows into
    /// the retry loop end-to-end on the NATIVE path -- not just the universal/InMemory-connector path the
    /// rest of this file exercises. The stub's native CTAS fragment fails once with a "connection reset"-
    /// shaped DuckDB error, then succeeds on the second attempt -- one scheduled retry, then success,
    /// exactly like <see cref="Transient_fails_twice_then_succeeds"/>'s universal-path analogue.</summary>
    [Fact]
    public async Task Native_transient_failure_is_retried_once_then_succeeds()
    {
        var node = NativeSourceLoadNode();
        _ctx.Connectors.AddSource("nativestub", new ConfigurableNativeSource(
            "(values (cast(1 as bigint)), (cast(2 as bigint))) t(x)",
            failAttempts: 1, failMessage: "IO Error: Connection reset by peer"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new EngineConfig()));
        var ctx = _ctx with { Plan = plan };

        var result = await NoDelayExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(2, result.RowsMoved);
        var call = Assert.Single(_events.RetryScheduledCalls);
        Assert.Equal(1, call.Attempt);
        Assert.Contains("connection reset", call.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Per_instance_retry_config_governs_attempts_when_no_override()
    {
        // Default policy allows only 3 attempts; the instance block allows 5 — the 4-failure connector
        // succeeds on attempt 5 iff the per-instance policy is the one actually resolved.
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(4),
        }, retry: new RetryDef(5, null, null));

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(4, _events.RetryScheduledCalls.Count);
        Assert.All(_events.RetryScheduledCalls, call => Assert.Equal(5, call.MaxAttempts));
    }

    [Fact]
    public async Task Explicit_constructor_policy_still_overrides_instance_config()
    {
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(1),
        }, retry: new RetryDef(9, null, null));

        var executor = NoDelayExecutor(new RetryPolicy(2, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30)));
        var result = await executor.ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var call = Assert.Single(_events.RetryScheduledCalls);
        Assert.Equal(2, call.MaxAttempts); // ctor override wins over the instance's 9
    }

    [Fact]
    public async Task Permanent_error_never_retries()
    {
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = false,
        });

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.Empty(_events.RetryScheduledCalls);
        Assert.Equal(1, _events.NodeStartedCount);
    }

    [Fact]
    public async Task Exhausting_all_attempts_still_fails_with_PZ0501()
    {
        // Always faults (no retry-limit counter) -- with MaxAttempts=3 this is 2 scheduled retries and
        // a final Failed result, never an unhandled exception escaping the executor.
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });

        var result = await NoDelayExecutor(new RetryPolicy(3, TimeSpan.Zero, TimeSpan.Zero)).ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.Equal(2, _events.RetryScheduledCalls.Count);
    }

    [Fact]
    public async Task Retry_scheduled_reason_is_sanitized()
    {
        // Simulates a connector whose PzConnectorException message
        // echoes a raw DuckDB-shaped engine error -- a "LINE <n>: ..." statement-echo block, which can
        // carry a secret literal verbatim (e.g. a malformed `CREATE SECRET ... 'SECRET_VALUE'`). The
        // retry loop must sanitize this before it reaches `retry_scheduled`'s `reason` field.
        const string engineEcho =
            "Binder Error: syntax error at or near \"CREATE\"\n" +
            "LINE 1: CREATE SECRET s (TYPE s3, KEY_ID 'AKID', SECRET 'SECRET_VALUE')\n" +
            "                                                        ^";
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(1),
            ["fail_message"] = engineEcho,
        });

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var reason = Assert.Single(_events.RetryScheduledCalls).Reason;
        Assert.DoesNotContain("SECRET_VALUE", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("LINE 1", reason, StringComparison.Ordinal);
        Assert.Contains("Binder Error", reason, StringComparison.Ordinal);
    }

    /// <summary>The terminal PZ0501 wrap (a permanent, non-transient failure -- no retry involved at
    /// all) must be sanitized exactly like the retry-reason path above. Both derive from the same
    /// underlying <c>ex.Message</c>, so the terminal wrap must stay consistent with the retry path.
    ///
    /// <c>fail_foreign</c> makes InMemoryPartition throw a raw
    /// <see cref="InvalidOperationException"/> instead of its usual <see cref="PzConnectorException"/>.
    /// PzConnectorException is a Pz-family type that MessageRedaction.Redact(Exception) passes
    /// through unredacted (see its trust boundary doc) -- this test exists specifically to prove a
    /// FOREIGN exception's message is still redacted, so it must not feed a Pz-family one.</summary>
    [Fact]
    public async Task Node_failure_message_is_redacted()
    {
        const string engineEcho =
            "Binder Error: syntax error at or near \"CREATE\"\n" +
            "LINE 1: CREATE SECRET s (TYPE s3, KEY_ID 'AKID', SECRET 'SECRET_VALUE')\n" +
            "                                                        ^";
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = false,
            ["fail_message"] = engineEcho,
            ["fail_foreign"] = true,
        });

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.DoesNotContain("SECRET_VALUE", result.Error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("LINE 1", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("Binder Error", result.Error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Cancellation_during_delay_propagates()
    {
        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });

        // The delay seam itself simulates "cancelled mid-backoff" -- independent of the CancellationToken
        // passed to ExecuteAsync, which stays live so the first (real) attempt runs normally.
        var executor = new KindDispatchingExecutor(
            RetryPolicy.Default, new FixedRandom(0.5), delay: (_, _) => throw new OperationCanceledException());

        await Assert.ThrowsAsync<OperationCanceledException>(() => executor.ExecuteAsync(node, _ctx, default));

        // Must not have been turned into a Failed NodeResult: NodeCompleted is the orchestrator's job
        // and this test calls the executor directly, so the only observable proof is the propagated
        // exception itself plus exactly one RetryScheduled call before the delay threw.
        Assert.Single(_events.RetryScheduledCalls);
    }

    /// <summary>The executor gate around <see cref="KindDispatchingExecutor"/>'s
    /// retry loop. Every breaker in these tests is keyed off the "mem" source instance used by
    /// <see cref="SourceLoadNode"/> above (instance key <c>"conn:mem"</c>), so any two nodes built by
    /// that helper -- regardless of NodeId/rows/fault options -- share the SAME <see cref="CircuitBreaker"/>
    /// once resolved through a common <see cref="BreakerRegistry"/>.</summary>
    [Fact]
    public async Task Threshold_trip_blocks_a_second_node_without_invoking_the_connector()
    {
        // Wires BreakerRegistry's onStateChanged the same way RunCommand does in
        // production, so this test also proves the closed -> open transition publishes a
        // BreakerStateChanged event -- not just that the gate gives up with PZ0506 below.
        var registry = new BreakerRegistry(new BreakerConfig(2, TimeSpan.FromMinutes(1)), new ManualTimeProvider(),
            (instance, oldState, newState, trigger, coolDown) =>
                _events.BreakerStateChanged(instance, oldState, newState, trigger, coolDown));

        // Node A: two consecutive transient failures (MaxAttempts=2 -> one scheduled retry, then the
        // second failure is attempts-exhausted) trip the "conn:mem" breaker at threshold 2.
        var nodeA = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });
        var ctxA = _ctx with { Breakers = registry };
        var resultA = await NoDelayExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero))
            .ExecuteAsync(nodeA, ctxA, default);

        Assert.Equal(NodeStatus.Failed, resultA.Status);
        Assert.Equal("PZ0501", resultA.Error!.Code); // node A's own exhausted-retry failure, not a gate give-up
        Assert.Single(_events.RetryScheduledCalls); // attempt 1's retry only -- attempt 2 exhausts and trips

        var change = Assert.Single(_events.BreakerStateChangedCalls);
        Assert.Equal("conn:mem", change.Instance);
        Assert.Equal("closed", change.OldState);
        Assert.Equal("open", change.NewState);
        Assert.Equal("2 consecutive transient failures", change.Trigger);
        Assert.Equal(TimeSpan.FromMinutes(1), change.CoolDown);

        // Node B: same source instance, dispatched immediately while the breaker is Open. With the
        // no-delay seam (time never advances), the gate waits MaxOpenWaits (2) cycles then gives up --
        // the connector is never invoked for node B at all.
        var eventsB = new RecordingEvents();
        var nodeB = SourceLoadNode(500);
        var ctxB = _ctx with { Events = eventsB, Breakers = registry };
        var resultB = await NoDelayExecutor().ExecuteAsync(nodeB, ctxB, default);

        Assert.Equal(NodeStatus.Failed, resultB.Status);
        Assert.Equal("PZ0506", resultB.Error!.Code);
        Assert.Contains("mem", resultB.Error.Message);
        // The cool-down renders config-style seconds ("60s"), not TimeSpan's default
        // "00:01:00" -- mirrors ConsoleRenderer.FormatCoolDown's derivation. The no-delay/no-time-advance
        // seam means every TryEnter through the bounded give-up sees the same fresh 1-minute cool-down.
        Assert.Contains("cool-down 60s", resultB.Error.Message);
        Assert.DoesNotContain("00:01:00", resultB.Error.Message);
        Assert.Equal(0, resultB.RowsMoved);
        Assert.Empty(eventsB.RetryScheduledCalls); // proves the connector was never invoked for node B
    }

    [Fact]
    public async Task Open_wait_does_not_consume_attempts_and_probe_success_closes_the_breaker()
    {
        var fakeTime = new ManualTimeProvider();
        var registry = new BreakerRegistry(new BreakerConfig(2, TimeSpan.FromMinutes(1)), fakeTime);
        var recordedDelays = new List<TimeSpan>();
        var driveTimeExecutor = new KindDispatchingExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero),
            new FixedRandom(0.5), delay: (d, _) =>
            {
                recordedDelays.Add(d);
                fakeTime.Advance(d);
                return Task.CompletedTask;
            });

        // Node A: trips the threshold-2 breaker exactly like the test above.
        var nodeA = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });
        var ctxA = _ctx with { Breakers = registry };
        var resultA = await NoDelayExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero))
            .ExecuteAsync(nodeA, ctxA, default);
        Assert.Equal(NodeStatus.Failed, resultA.Status);

        // Node B: dispatched while Open. The gate's open-wait loop drives fake time forward (via the
        // delay seam) until the cool-down elapses, admitting node B as the half-open probe -- which
        // succeeds outright, closing the breaker. No retry is ever scheduled for node B, and its
        // NodeStarted fires exactly once: the open-wait loop never touched attempt/event bookkeeping.
        var eventsB = new RecordingEvents();
        var nodeB = SourceLoadNode(500);
        var ctxB = _ctx with { Events = eventsB, Breakers = registry };
        var resultB = await driveTimeExecutor.ExecuteAsync(nodeB, ctxB, default);

        Assert.Equal(NodeStatus.Success, resultB.Status);
        Assert.Equal(1, eventsB.NodeStartedCount);
        Assert.Empty(eventsB.RetryScheduledCalls);
        Assert.Contains(TimeSpan.FromMinutes(1), recordedDelays); // the open-wait requested the full cool-down

        // The breaker is now Closed: a node dispatched right after that fails once then succeeds proves
        // ordinary retry attempt-numbering (starting at 1) is unaffected by however many gate-wait cycles
        // preceded it for node B -- the shared openWaits counter never leaks into `attempt`.
        var eventsC = new RecordingEvents();
        var nodeC = SourceLoadNode(200, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(1),
        }, dataset: "numbersC"); // node B's successful load left "numbers"'s staging table behind
        var ctxC = _ctx with { Events = eventsC, Breakers = registry };
        var resultC = await NoDelayExecutor(new RetryPolicy(3, TimeSpan.Zero, TimeSpan.Zero))
            .ExecuteAsync(nodeC, ctxC, default);

        Assert.Equal(NodeStatus.Success, resultC.Status);
        var retryCall = Assert.Single(eventsC.RetryScheduledCalls);
        Assert.Equal(1, retryCall.Attempt);
    }

    /// <summary><see cref="Open_wait_does_not_consume_attempts_and_probe_success_closes_the_breaker"/>
    /// above asserts <c>Attempt == 1</c> on node C -- a node dispatched in a SEPARATE <c>ExecuteAsync</c>
    /// call after the breaker had already closed. Node C's <c>attempt</c> is a fresh method-local <c>int</c>
    /// (declared at the top of <see cref="KindDispatchingExecutor.ExecuteAsync"/>'s own call), so it could
    /// never have observed node B's open-wait cycles regardless of whether the executor's bookkeeping were
    /// correct or badly broken -- that assertion is vacuous.
    ///
    /// This test puts the open-wait AND the attempt-numbering assertion on the SAME node/SAME
    /// <c>ExecuteAsync</c> call instead: the node arrives at an Open breaker, open-waits (driving fake time
    /// via the delay seam, exactly like the gate's own admission wait) until the cool-down elapses, is
    /// admitted as the half-open probe -- and THAT probe's own first connector call fails transiently with
    /// <c>MaxAttempts &gt;= 2</c>, so it schedules a retry whose <c>Attempt</c> must still read 1. If the gate's
    /// open-wait loop ever incremented the shared <c>attempt</c> counter (the mutation this test is designed
    /// to catch), the first (and only) <c>RetryScheduled</c> call below would report <c>Attempt == 2</c> or
    /// higher instead.</summary>
    [Fact]
    public async Task Open_wait_cycles_do_not_leak_into_the_probes_own_retry_attempt_numbering()
    {
        var fakeTime = new ManualTimeProvider();
        var registry = new BreakerRegistry(new BreakerConfig(2, TimeSpan.FromMinutes(1)), fakeTime);
        var driveTimeExecutor = new KindDispatchingExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero),
            new FixedRandom(0.5), delay: (d, _) =>
            {
                fakeTime.Advance(d);
                return Task.CompletedTask;
            });

        // Node A: two consecutive transient failures trip the threshold-2 breaker, exactly like the tests
        // above.
        var nodeA = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });
        var ctxA = _ctx with { Breakers = registry };
        var resultA = await NoDelayExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero))
            .ExecuteAsync(nodeA, ctxA, default);
        Assert.Equal(NodeStatus.Failed, resultA.Status);

        // Node B: dispatched while Open. The gate's open-wait loop drives fake time forward until the
        // cool-down elapses and admits node B as the half-open probe -- whose own connector call fails
        // transiently EXACTLY ONCE (fail_read_retry_limit(1)) before succeeding on the retry, so with
        // MaxAttempts=2 it schedules exactly one retry. That retry's Attempt must be 1 (and MaxAttempts
        // the resolved 2, unchanged) -- the open-wait cycle that preceded admission consumed zero attempts.
        var eventsB = new RecordingEvents();
        var nodeB = SourceLoadNode(500, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(1),
        });
        var ctxB = _ctx with { Events = eventsB, Breakers = registry };
        var resultB = await driveTimeExecutor.ExecuteAsync(nodeB, ctxB, default);

        Assert.Equal(NodeStatus.Success, resultB.Status);
        Assert.Equal(1, eventsB.NodeStartedCount); // fired once regardless of open-wait/retry cycles
        var firstRetry = Assert.Single(eventsB.RetryScheduledCalls);
        Assert.Equal(1, firstRetry.Attempt);
        Assert.Equal(2, firstRetry.MaxAttempts);
    }

    /// <summary><see cref="RetryAfter_floor_is_visible_in_the_executor_gates_recorded_delay"/>
    /// above primes the breaker directly via <see cref="CircuitBreaker.TryEnter(out TimeSpan, out long)"/>/
    /// <see cref="CircuitBreaker.RecordTransientFailure"/>, bypassing <see cref="KindDispatchingExecutor"/>'s
    /// own catch sites entirely -- it proves the breaker HONORS a floor once handed one, not that the
    /// executor's catches actually PASS <c>ex.RetryAfter</c> (as opposed to always passing <c>null</c>) when a
    /// real connector exception carries one. This test closes that gap end-to-end, via the InMemory
    /// reference connector's <c>fail_retry_after</c> fault-injection lever (seconds; see
    /// <c>FaultInjection.GetRetryAfter</c>'s doc in <c>Pz.Connectors.TestKit</c>): node A's own transient
    /// failure -- routed through the executor's real exhausted-attempts catch
    /// (<c>breaker?.RecordTransientFailure(ticket, ex.RetryAfter)</c>) -- trips a cool_down-1s breaker with a
    /// 10-minute <c>RetryAfter</c> floor, and node B's gate-open-wait delay request is asserted to reflect
    /// that floor.</summary>
    [Fact]
    public async Task RetryAfter_from_the_connector_floors_the_breakers_cool_down_end_to_end()
    {
        var fakeTime = new ManualTimeProvider();
        var registry = new BreakerRegistry(new BreakerConfig(1, TimeSpan.FromSeconds(1)), fakeTime);

        var recordedDelays = new List<TimeSpan>();
        var executor = new KindDispatchingExecutor(new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero),
            new FixedRandom(0.5), delay: (d, _) =>
            {
                recordedDelays.Add(d);
                return Task.CompletedTask;
            });

        // Node A: MaxAttempts=1, so its one transient failure is immediately attempts-exhausted -- the
        // executor's OWN catch (not a test priming the breaker directly) calls
        // breaker.RecordTransientFailure(ticket, ex.RetryAfter) with ex.RetryAfter = 10 minutes, tripping
        // the threshold-1 breaker.
        var nodeA = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_retry_after"] = 600, // seconds = 10 minutes; floor(10m) > cool_down(1s)
        });
        var ctxA = _ctx with { Breakers = registry };
        var resultA = await executor.ExecuteAsync(nodeA, ctxA, default);
        Assert.Equal(NodeStatus.Failed, resultA.Status);
        Assert.Equal("PZ0501", resultA.Error!.Code); // node A's own exhausted-retry failure, not a gate give-up

        // Node B: dispatched immediately after (same fake clock, unmoved -- this delay seam never advances
        // it), same source instance -> same breaker. The gate's open-wait requests the FLOORED cool-down
        // (10m, not the configured 1s cool_down) before giving up with PZ0506 -- proving ex.RetryAfter, not
        // null, reached the breaker via the executor's real catch site.
        var eventsB = new RecordingEvents();
        var nodeB = SourceLoadNode(500);
        var ctxB = _ctx with { Events = eventsB, Breakers = registry };
        var resultB = await executor.ExecuteAsync(nodeB, ctxB, default);

        Assert.Equal(NodeStatus.Failed, resultB.Status);
        Assert.Equal("PZ0506", resultB.Error!.Code);
        Assert.NotEmpty(recordedDelays);
        Assert.All(recordedDelays, d => Assert.True(d >= TimeSpan.FromMinutes(10)));
        Assert.Equal(TimeSpan.FromMinutes(10), recordedDelays[0]); // the RetryAfter floor, not the 1s cool_down
        Assert.Empty(eventsB.RetryScheduledCalls); // node B never reached the connector at all
    }

    [Fact]
    public async Task Probe_failure_reopens_the_breaker_and_the_next_node_gets_PZ0506()
    {
        var fakeTime = new ManualTimeProvider();
        var registry = new BreakerRegistry(new BreakerConfig(2, TimeSpan.FromMinutes(1)), fakeTime);
        var driveTimeExecutor = new KindDispatchingExecutor(new RetryPolicy(1, TimeSpan.Zero, TimeSpan.Zero),
            new FixedRandom(0.5), delay: (d, _) =>
            {
                fakeTime.Advance(d);
                return Task.CompletedTask;
            });

        // Node A: two consecutive transient failures trip the threshold-2 breaker.
        var nodeA = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });
        var ctxA = _ctx with { Breakers = registry };
        var resultA = await NoDelayExecutor(new RetryPolicy(2, TimeSpan.Zero, TimeSpan.Zero))
            .ExecuteAsync(nodeA, ctxA, default);
        Assert.Equal(NodeStatus.Failed, resultA.Status);

        // Node B: dispatched while Open, the gate drives fake time forward to admit it as the half-open
        // probe -- but its own connector call ALSO fails transiently, and with MaxAttempts=1 (no retry
        // left) the exhausted-attempt path reports that failure on the probe's own (still-current) ticket,
        // reopening the breaker for a fresh cool-down.
        var eventsB = new RecordingEvents();
        var nodeB = SourceLoadNode(500, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
        });
        var ctxB = _ctx with { Events = eventsB, Breakers = registry };
        var resultB = await driveTimeExecutor.ExecuteAsync(nodeB, ctxB, default);

        Assert.Equal(NodeStatus.Failed, resultB.Status);
        Assert.Equal("PZ0501", resultB.Error!.Code); // the probe's own connector failure, not a gate give-up
        Assert.Empty(eventsB.RetryScheduledCalls); // MaxAttempts=1 -- no retry was ever scheduled

        // Node C: dispatched immediately after the reopen, with a delay seam that never advances time --
        // the fresh cool-down never elapses within the gate's bounded wait, so node C gives up with
        // PZ0506 without the connector ever being invoked.
        var eventsC = new RecordingEvents();
        var nodeC = SourceLoadNode(200);
        var ctxC = _ctx with { Events = eventsC, Breakers = registry };
        var resultC = await NoDelayExecutor().ExecuteAsync(nodeC, ctxC, default);

        Assert.Equal(NodeStatus.Failed, resultC.Status);
        Assert.Equal("PZ0506", resultC.Error!.Code);
        Assert.Empty(eventsC.RetryScheduledCalls);
    }

    /// <summary>The InMemory reference connector has no fault-injection lever for
    /// <see cref="Pz.Connectors.Abstractions.PzConnectorException.RetryAfter"/> (see
    /// <c>Pz.Connectors.TestKit</c>'s <c>FaultInjection</c> -- only fail_transient/fail_message/
    /// fail_read_retry_limit exist), so this test exercises the CircuitBreaker/executor-gate boundary
    /// directly: it opens the breaker via the SAME API <see cref="KindDispatchingExecutor"/>'s
    /// transient-retry catch calls (<c>breaker.RecordTransientFailure(ticket, ex.RetryAfter)</c>), with a
    /// RetryAfter floor (10m) that exceeds the configured cool_down (1m) -- then proves the executor's
    /// gate forwards <see cref="CircuitBreaker.TryEnter"/>'s floor-extended <c>retryIn</c> into its delay
    /// seam unchanged.</summary>
    [Fact]
    public async Task RetryAfter_floor_is_visible_in_the_executor_gates_recorded_delay()
    {
        var fakeTime = new ManualTimeProvider();
        var registry = new BreakerRegistry(new BreakerConfig(1, TimeSpan.FromMinutes(1)), fakeTime);

        var primerNode = SourceLoadNode(1_000);
        var breaker = registry.For(primerNode)!; // same "conn:mem" breaker any node below will resolve to
        Assert.True(breaker.TryEnter(out _, out var primerTicket));
        breaker.RecordTransientFailure(primerTicket, TimeSpan.FromMinutes(10)); // floor(10m) > cool_down(1m)

        var recordedDelays = new List<TimeSpan>();
        var executor = new KindDispatchingExecutor(RetryPolicy.Default, new FixedRandom(0.5),
            delay: (d, _) =>
            {
                recordedDelays.Add(d);
                return Task.CompletedTask;
            });

        var node = SourceLoadNode(200);
        var ctx = _ctx with { Breakers = registry };
        var result = await executor.ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0506", result.Error!.Code);
        Assert.NotEmpty(recordedDelays);
        Assert.All(recordedDelays, d => Assert.True(d > TimeSpan.FromMinutes(1)));
        Assert.Equal(TimeSpan.FromMinutes(10), recordedDelays[0]); // the floor, not the shorter cool_down
    }

    /// <summary>Regression guard (mirrors <see cref="Transient_fails_twice_then_succeeds"/>): with
    /// <see cref="RunContext.Breakers"/> left at its default null, the executor gate must be a complete
    /// no-op -- identical NodeResult/attempt/event counts to a world where <see cref="BreakerRegistry"/>
    /// never existed.</summary>
    [Fact]
    public async Task Breakers_null_leaves_retry_behavior_byte_identical()
    {
        Assert.Null(_ctx.Breakers);

        var node = SourceLoadNode(1_000, new Dictionary<string, object?>
        {
            ["fail_read_at_batch"] = 0,
            ["fail_transient"] = true,
            ["fail_read_retry_limit"] = new RetryCounter(2),
        });

        var result = await NoDelayExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1_000, result.RowsMoved);
        Assert.Equal(2, _events.RetryScheduledCalls.Count);
        Assert.Equal(1, _events.RetryScheduledCalls[0].Attempt);
        Assert.Equal(2, _events.RetryScheduledCalls[1].Attempt);
        Assert.All(_events.RetryScheduledCalls, call => Assert.Equal(3, call.MaxAttempts));
        Assert.Equal(1, _events.NodeStartedCount);
    }

    /// <summary>Native-scan-only stub source, mirroring <c>NativePathTests.ConfigurableNativeSource</c>'s
    /// idiom (fragment-configurable; universal-path members throw so any accidental fall-through fails
    /// loudly) -- with one additive option these tests need: the first <paramref
    /// name="failAttempts"/> calls to <see cref="TryGetNativeScan"/> return a fragment whose evaluation
    /// raises a DuckDB error carrying <paramref name="failMessage"/> verbatim (via DuckDB's <c>error()</c>
    /// scalar function, same technique <c>NativePathTests.Native_scan_failure_leaves_no_staging_table</c>
    /// already uses); every call after that returns <paramref name="fragment"/> (the one that succeeds).
    /// <see cref="KindDispatchingExecutor"/> calls the executor's <c>ExecuteAsync</c> fresh per retry
    /// attempt (a new <c>OpenAsync</c>/<c>TryGetNativeScan</c> call each time), so the mutable
    /// <c>_attempts</c> counter on this ONE registered instance is what makes "fails on attempt 1,
    /// succeeds on attempt 2" observable across attempts.</summary>
    private sealed class ConfigurableNativeSource(string fragment, int failAttempts = 0, string failMessage = "boom")
        : ISourceConnector, ISource
    {
        private int _attempts;

        public ConnectorInfo Info => new("stub-native", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            _attempts++;
            var sql = _attempts <= failAttempts
                ? $"(select error('{failMessage}') from range(1))"
                : fragment;
            scan = new NativeScan(sql, []) { Mechanism = "stub_scan" };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    private sealed record RetryCall(int Attempt, int MaxAttempts, TimeSpan Delay, string Reason);

    private sealed record BreakerChangeCall(string Instance, string OldState, string NewState, string Trigger,
        TimeSpan CoolDown);

    private sealed class RecordingEvents : IRunEvents
    {
        private int _nodeStartedCount;
        public int NodeStartedCount => Volatile.Read(ref _nodeStartedCount);
        public List<RetryCall> RetryScheduledCalls { get; } = [];
        public List<BreakerChangeCall> BreakerStateChangedCalls { get; } = [];

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) => Interlocked.Increment(ref _nodeStartedCount);
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }

        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) =>
            RetryScheduledCalls.Add(new RetryCall(attempt, maxAttempts, delay, reason));

        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) =>
            BreakerStateChangedCalls.Add(new BreakerChangeCall(instance, oldState, newState, trigger, coolDown));

        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }

        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }
}
