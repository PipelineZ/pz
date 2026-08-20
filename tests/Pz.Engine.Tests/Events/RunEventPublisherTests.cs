using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Diagnostics.Events;
using Pz.Engine.Events;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Events;

/// <summary>Maps <see cref="IRunEvents"/> callbacks onto <see cref="RunEvent"/>s published to a
/// <see cref="RunEventBus"/>, stamped via an injected <see cref="TimeProvider"/> (the determinism
/// seam).</summary>
public class RunEventPublisherTests
{
    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static DagNode Node(string name) =>
        new(NodeId.Compute(name), NodeKind.SourceLoad, name, [], null, name);

    private static async Task<RunEvent> ReadOneAsync(RunEventBus bus)
    {
        bus.Complete();
        await foreach (var evt in bus.ReadAllAsync())
        {
            return evt;
        }

        throw new InvalidOperationException("no event published");
    }

    [Fact]
    public async Task NodeCompleted_maps_all_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var error = new PzError(PzErrorCode.NodeFailed, "boom", null, null, null);
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Failed, 42,
            TimeSpan.FromMilliseconds(1234), error);

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Equal("run-1", evt.RunId);
        Assert.Equal(node.Id.Value, evt.NodeId);
        Assert.Equal("SourceLoad", evt.Kind);
        Assert.Equal("orders", evt.Name);
        Assert.Equal("failed", evt.Status);
        Assert.Equal(42, evt.Rows);
        Assert.Equal(1234, evt.DurationMs);
        Assert.Equal(PzErrorCode.NodeFailed, evt.ErrorCode);
        Assert.Equal("boom", evt.ErrorMessage);
        Assert.Null(evt.Timings);
    }

    /// <summary>A <see cref="NodeResult"/> carrying <see cref="NodeTimings"/> maps onto
    /// the primitive-only <see cref="NodeTimingsPayload"/> in whole milliseconds (Pz.Diagnostics stays
    /// BCL-only — no TimeSpan crosses the boundary).</summary>
    [Fact]
    public async Task NodeCompleted_maps_timings_to_payload()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(1000), null,
            new NodeTimings(TimeSpan.FromMilliseconds(120), TimeSpan.FromMilliseconds(680)));

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.NotNull(evt.Timings);
        Assert.Equal(120, evt.Timings!.ProducerStallMs);
        Assert.Equal(680, evt.Timings.ConsumerStallMs);
    }

    /// <summary><see cref="NodeResult.Provenance"/> maps onto the wire
    /// value used by <c>run_results.json</c> — "reused" / "carried_forward" — and stays null
    /// when the node executed normally.</summary>
    [Theory]
    [InlineData(NodeProvenance.Reused, "reused")]
    [InlineData(NodeProvenance.CarriedForward, "carried_forward")]
    public async Task NodeCompleted_maps_provenance_to_wire_value(NodeProvenance provenance, string expected)
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(5), null, Provenance: provenance);

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Equal(expected, evt.Provenance);
    }

    [Fact]
    public async Task NodeCompleted_maps_null_provenance()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(5), null);

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Null(evt.Provenance);
    }

    /// <summary><see cref="NodeResult.Ops"/> maps onto the BCL-only
    /// <see cref="OpStatsPayload"/> (Pz.Diagnostics stays BCL-only — no <c>Pz.Engine.Resilience.OpStats</c>
    /// crosses the boundary), and stays null when the node had no operation gate.</summary>
    [Fact]
    public async Task Publisher_maps_ops()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(5), null, Ops: new Pz.Engine.Resilience.OpStats(7, 2, 350));

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Equal(new OpStatsPayload(7, 2, 350), evt.Ops);
    }

    [Fact]
    public async Task Publisher_maps_null_ops()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(5), null);

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Null(evt.Ops);
    }

    /// <summary><see cref="NodeResult.Cdc"/> maps onto the BCL-only
    /// <see cref="CdcPayload"/>, with <c>position</c> pulled from <see cref="NodeResult.SyncStateCandidate"/>
    /// (cdc reuses the sync-state seam for its candidate token) rather than
    /// <see cref="CdcStats"/> itself.</summary>
    [Fact]
    public async Task Publisher_maps_cdc()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders_cdc");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders_cdc", NodeStatus.Success, 40,
            TimeSpan.FromMilliseconds(5), null,
            SyncStateCandidate: new Pz.Engine.State.SyncState("0/1A2B3C4", "run-1"),
            Cdc: new CdcStats(30, 8, 2));

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Equal(new CdcPayload(30, 8, 2, "0/1A2B3C4"), evt.Cdc);
    }

    [Fact]
    public async Task Publisher_maps_cdc_position_null_when_no_sync_state_candidate()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders_cdc");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders_cdc", NodeStatus.Success, 0,
            TimeSpan.Zero, null, Cdc: new CdcStats(0, 0, 0));

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Equal(new CdcPayload(0, 0, 0, null), evt.Cdc);
    }

    [Fact]
    public async Task Publisher_maps_null_cdc()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");
        var result = new NodeResult(node.Id, NodeKind.SourceLoad, "orders", NodeStatus.Success, 42,
            TimeSpan.FromMilliseconds(5), null);

        publisher.NodeCompleted(result);

        var evt = Assert.IsType<NodeCompletedEvent>(await ReadOneAsync(bus));
        Assert.Null(evt.Cdc);
    }

    /// <summary>Mirrors <see cref="NodeCompleted_maps_all_fields"/> —
    /// every argument maps onto the matching <see cref="SourceDriftDetectedEvent"/> field, including the
    /// change/observed lists mapped onto their BCL-only payload twins, and `At` comes from the injected
    /// clock rather than wall-clock time.</summary>
    [Fact]
    public async Task SourceDriftDetected_maps_all_fields()
    {
        var fixedNow = new DateTimeOffset(2026, 8, 11, 9, 0, 0, TimeSpan.Zero);
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", new FixedTimeProvider(fixedNow));
        var node = Node("orders");
        var changes = new List<Pz.Engine.State.SchemaDriftDiffer.Change>
        {
            new("retyped", "amount", "BIGINT", "VARCHAR"),
        };
        var observed = new List<Pz.Engine.State.SchemaColumn>
        {
            new("amount", "VARCHAR"),
        };

        publisher.SourceDriftDetected(node, "pg_prod", "orders", "warn", changes, observed, "abc123");

        var evt = Assert.IsType<SourceDriftDetectedEvent>(await ReadOneAsync(bus));
        Assert.Equal(fixedNow, evt.At);
        Assert.Equal("run-1", evt.RunId);
        Assert.Equal(node.Id.Value, evt.NodeId);
        Assert.Equal("pg_prod", evt.Connection);
        Assert.Equal("orders", evt.Entity);
        Assert.Equal("warn", evt.Policy);
        Assert.Equal(new DriftChangePayload("retyped", "amount", "BIGINT", "VARCHAR"), Assert.Single(evt.Changes));
        Assert.Equal(new SchemaColumnPayload("amount", "VARCHAR"), Assert.Single(evt.Observed));
        Assert.Equal("abc123", evt.HintsHash);
    }

    [Fact]
    public async Task Events_are_stamped_with_injected_clock()
    {
        var fixedNow = new DateTimeOffset(2026, 7, 4, 10, 0, 0, TimeSpan.Zero);
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", new FixedTimeProvider(fixedNow));

        publisher.NodeStarted(Node("orders"));

        var evt = await ReadOneAsync(bus);
        Assert.Equal(fixedNow, evt.At);
    }

    [Fact]
    public async Task NodeStarted_maps_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");

        publisher.NodeStarted(node);

        var evt = Assert.IsType<NodeStartedEvent>(await ReadOneAsync(bus));
        Assert.Equal("run-1", evt.RunId);
        Assert.Equal(node.Id.Value, evt.NodeId);
        Assert.Equal("SourceLoad", evt.Kind);
        Assert.Equal("orders", evt.Name);
    }

    [Fact]
    public async Task NodeProgress_maps_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");

        publisher.NodeProgress(node, rowsSoFar: 100, bytesSoFar: 2048, batchesSoFar: 10);

        var evt = Assert.IsType<NodeProgressEvent>(await ReadOneAsync(bus));
        Assert.Equal(100, evt.Rows);
        Assert.Equal(2048, evt.Bytes);
        Assert.Equal(10, evt.Batches);
    }

    [Fact]
    public async Task RetryScheduled_maps_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");

        publisher.RetryScheduled(node, attempt: 2, maxAttempts: 3, delay: TimeSpan.FromSeconds(1.5),
            reason: "timeout");

        var evt = Assert.IsType<RetryScheduledEvent>(await ReadOneAsync(bus));
        Assert.Equal(2, evt.Attempt);
        Assert.Equal(3, evt.MaxAttempts);
        Assert.Equal(1500, evt.DelayMs);
        Assert.Equal("timeout", evt.Reason);
    }

    /// <summary>Maps <see cref="TimeSpan"/> coolDown down to whole milliseconds,
    /// mirroring every other duration crossing the Pz.Diagnostics boundary (e.g. <c>RetryScheduled</c>'s
    /// <c>DelayMs</c>).</summary>
    [Fact]
    public async Task BreakerStateChanged_maps_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);

        publisher.BreakerStateChanged("conn:pg_prod", "closed", "open", "5 consecutive transient failures",
            TimeSpan.FromSeconds(120));

        var evt = Assert.IsType<BreakerStateChangedEvent>(await ReadOneAsync(bus));
        Assert.Equal("run-1", evt.RunId);
        Assert.Equal("conn:pg_prod", evt.Instance);
        Assert.Equal("closed", evt.OldState);
        Assert.Equal("open", evt.NewState);
        Assert.Equal("5 consecutive transient failures", evt.Trigger);
        Assert.Equal(120_000, evt.CoolDownMs);
    }

    /// <summary>For any single node, the bus preserves the exact
    /// publish order Started → Progress* → Completed, since there is one channel and one reader.</summary>
    [Fact]
    public async Task Per_node_event_order_is_preserved_through_the_bus()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);
        var node = Node("orders");

        publisher.NodeStarted(node);
        publisher.NodeProgress(node, 10, 100, 1);
        publisher.NodeProgress(node, 20, 200, 2);
        publisher.NodeCompleted(new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, 20,
            TimeSpan.FromMilliseconds(5), null));
        bus.Complete();

        var events = new List<RunEvent>();
        await foreach (var evt in bus.ReadAllAsync())
        {
            events.Add(evt);
        }

        Assert.Equal(4, events.Count);
        Assert.IsType<NodeStartedEvent>(events[0]);
        Assert.IsType<NodeProgressEvent>(events[1]);
        Assert.IsType<NodeProgressEvent>(events[2]);
        Assert.IsType<NodeCompletedEvent>(events[3]);
    }

    [Fact]
    public async Task RunStarted_and_RunCompleted_map_fields()
    {
        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-1", TimeProvider.System);

        publisher.RunStarted("run-1", "hello-pz", 4);
        publisher.RunCompleted("run-1", RunStatus.CompletedWithFailures, 2, 1, 1, TimeSpan.FromSeconds(5));
        bus.Complete();

        var events = new List<RunEvent>();
        await foreach (var evt in bus.ReadAllAsync())
        {
            events.Add(evt);
        }

        var started = Assert.IsType<RunStartedEvent>(events[0]);
        Assert.Equal("hello-pz", started.ProjectName);
        Assert.Equal(4, started.NodeCount);

        var completed = Assert.IsType<RunCompletedEvent>(events[1]);
        Assert.Equal("completed_with_failures", completed.Status);
        Assert.Equal(2, completed.Succeeded);
        Assert.Equal(1, completed.Failed);
        Assert.Equal(1, completed.Skipped);
        Assert.Equal(5000, completed.DurationMs);
    }

    [Fact]
    public void RunCompleted_increments_run_completed_counter_with_status_tag()
    {
        long observed = 0;
        string? statusTag = null;
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == Pz.Diagnostics.Otel.PzMeters.Name && instrument.Name == "pz.run.completed")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            observed += measurement;
            foreach (var tag in tags)
            {
                if (tag.Key == "pz.run.status")
                {
                    statusTag = (string?)tag.Value;
                }
            }
        });
        listener.Start();

        var bus = new RunEventBus();
        var publisher = new RunEventPublisher(bus, "run-metrics", TimeProvider.System);

        publisher.RunCompleted("run-metrics", RunStatus.CompletedWithFailures, 1, 1, 0, TimeSpan.FromSeconds(2));

        Assert.Equal(1, observed);
        Assert.Equal("completed_with_failures", statusTag);
    }
}
