using Pz.Diagnostics.Events;

namespace Pz.Cli.Rendering;

/// <summary>Bottleneck hint wording. Presentation only — recomputed by any consumer from
/// <see cref="NodeCompletedEvent.Timings"/> plus
/// <see cref="NodeCompletedEvent.DurationMs"/>, never itself part of the NDJSON schema. Shared by
/// <see cref="ConsoleRenderer"/> and <see cref="LiveTreeRenderer"/> so both renderers agree on wording.</summary>
public static class BottleneckHint
{
    /// <summary>A side's stall must cover at least this percentage of the node's total duration before a
    /// hint fires.</summary>
    public const int ThresholdPct = 60;

    /// <summary>Returns the hint line for <paramref name="evt"/>, or <c>null</c> when there's nothing to
    /// say — no timings, a zero/negative duration (can't divide), or neither side crosses the
    /// threshold.</summary>
    public static string? For(NodeCompletedEvent evt)
    {
        if (evt.Timings is not { } timings || evt.DurationMs <= 0)
        {
            return null;
        }

        var producerPct = timings.ProducerStallMs * 100 / evt.DurationMs;
        var consumerPct = timings.ConsumerStallMs * 100 / evt.DurationMs;

        return evt.Kind switch
        {
            "SourceLoad" => SourceLoadHint(evt.Name, producerPct, consumerPct),
            "SinkWrite" => SinkWriteHint(evt.Name, producerPct, consumerPct),
            _ => null,
        };
    }

    /// <summary>SourceLoad's channel: producer = the partition reader pushing into the channel
    /// (<c>writer.WriteAsync</c>); consumer = the ingest side draining it. A dominant consumer stall
    /// means ingest sat idle waiting on the source (`source-bound`); a dominant producer stall means the
    /// reader sat idle (blocked) waiting on ingest to drain (`ingest-bound`).</summary>
    private static string? SourceLoadHint(string name, long producerPct, long consumerPct)
    {
        if (consumerPct >= ThresholdPct)
        {
            return $"hint: {name}: source-bound — ingest idle {consumerPct}% of the node's runtime";
        }

        if (producerPct >= ThresholdPct)
        {
            return $"hint: {name}: ingest-bound — reader idle {producerPct}% of the node's runtime";
        }

        return null;
    }

    /// <summary>SinkWrite's channel: producer = the egress reader pulling from staging
    /// (<c>QueryArrowAsync</c>'s <c>MoveNextAsync</c>); consumer = the sink write
    /// (<c>WriteBatchAsync</c>). A dominant producer stall means staging is slow to feed rows
    /// (`staging-bound`); a dominant consumer stall means the sink is slow to accept them
    /// (`sink-bound`). The trailing clause names the side left waiting — the OTHER side from the
    /// verdict: a staging-bound node's writer sits idle while staging feeds it, and a sink-bound
    /// node's staging side sits idle while the writer grinds.</summary>
    private static string? SinkWriteHint(string name, long producerPct, long consumerPct)
    {
        if (producerPct >= ThresholdPct)
        {
            return $"hint: {name}: staging-bound — writer idle {producerPct}% of the node's runtime";
        }

        if (consumerPct >= ThresholdPct)
        {
            return $"hint: {name}: sink-bound — staging idle {consumerPct}% of the node's runtime";
        }

        return null;
    }
}
