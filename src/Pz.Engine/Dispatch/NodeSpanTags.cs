using System.Diagnostics;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Dispatch;

/// <summary>The node span's attributes, from the node's result. Names and counts only: never SQL, connection
/// config, watermark values, sync tokens or CDC positions. A failure is exported as its error code and nothing else:
/// a connector's message can echo data values, so message text never reaches a span.</summary>
internal static class NodeSpanTags
{
    public static void Apply(Activity? span, NodeResult result)
    {
        if (span is null) return;

        span.SetTag("pz.node.kind", result.Kind.ToString());
        if (result.Status != NodeStatus.Skipped) span.SetTag("pz.node.rows", result.RowsMoved);

        if (result.Facts is { } facts)
        {
            span.SetTag("pz.connector", facts.Connector);
            span.SetTag("pz.connection", facts.Connection);
            span.SetTag("pz.node.strategy", facts.Strategy);
            if (facts.Attempts > 0) span.SetTag("pz.node.attempts", facts.Attempts);
            if (facts.Stages is { } s)
            {
                SetMs(span, "pz.stage.extract_ms", s.Extract);
                SetMs(span, "pz.stage.ingest_ms", s.Ingest);
                SetMs(span, "pz.stage.egress_ms", s.Egress);
                SetMs(span, "pz.stage.write_ms", s.Write);
            }

            if (facts.Remote is { } r)
            {
                if (r.Requests > 0) span.SetTag("pz.remote.requests", r.Requests);
                if (r.WireBytes > 0) span.SetTag("pz.remote.wire_bytes", r.WireBytes);
                if (r.RemoteMs > 0) span.SetTag("pz.remote.ms", r.RemoteMs);
                if (r.Throttled > 0) span.SetTag("pz.remote.throttled", r.Throttled);
                if (r.CostUnit is { } costUnit)
                {
                    span.SetTag("pz.remote.cost", r.CostUnits ?? 0);
                    span.SetTag("pz.remote.cost_unit", costUnit);
                }

                foreach (var extra in r.Extras) span.SetTag("pz.remote.extra." + extra.Key, extra.Value);
                if (r.SpansDropped > 0) span.SetTag("pz.remote.spans_dropped", r.SpansDropped);
            }
        }

        if (result.Ops is { } ops)
        {
            span.SetTag("pz.ops.executed", ops.Executed);
            span.SetTag("pz.ops.retried", ops.Retried);
            span.SetTag("pz.ops.throttle_wait_ms", ops.ThrottleWaitMs);
        }

        if (result.Timings is { } t)
        {
            span.SetTag("pz.node.producer_stall_ms", (long)t.ProducerStall.TotalMilliseconds);
            span.SetTag("pz.node.consumer_stall_ms", (long)t.ConsumerStall.TotalMilliseconds);
        }

        if (result.Provenance is { } provenance)
            span.SetTag("pz.node.provenance", provenance == NodeProvenance.Reused ? "reused" : "carried_forward");
        if (result.CaughtUp is { } caughtUp) span.SetTag("pz.node.caught_up", caughtUp);
        if (result.Partitions is { } p)
        {
            span.SetTag("pz.partitions.total", p.Total);
            span.SetTag("pz.partitions.completed", p.Completed);
            span.SetTag("pz.partitions.reused", p.Reused);
            span.SetTag("pz.partitions.resumed", p.Resumed);
        }

        if (result.Delivery is { } d)
        {
            span.SetTag("pz.delivery.abort_semantics", d.AbortSemantics);
            span.SetTag("pz.delivery.rows_visible", d.RowsVisible);
            span.SetTag("pz.delivery.resumed_rows", d.ResumedRows);
        }

        if (result.Cdc is { } c)
        {
            span.SetTag("pz.cdc.inserts", c.Inserts);
            span.SetTag("pz.cdc.updates", c.Updates);
            span.SetTag("pz.cdc.deletes", c.Deletes);
        }

        if (result.Status == NodeStatus.Failed)
        {
            var code = result.Error?.Code ?? PzErrorCode.NodeFailed;
            span.SetTag("error.type", code);
            span.SetStatus(ActivityStatusCode.Error, code);
            span.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                ["exception.type"] = code,
                ["pz.error.code"] = code,
            }));
        }
    }

    private static void SetMs(Activity span, string name, TimeSpan? value)
    {
        if (value is { } v) span.SetTag(name, (long)v.TotalMilliseconds);
    }
}
