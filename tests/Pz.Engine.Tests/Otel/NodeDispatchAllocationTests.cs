using Pz.Core.Dag;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Otel;

/// <summary><c>RunOrchestrator</c>'s two per-dispatch OTel call sites — the "node.&lt;kind&gt;" span name
/// (<c>RunOrchestrator.SpanNames</c>) and the <c>pz.node.kind</c> metric tag
/// (<c>RunOrchestrator.NodeKindTags</c>) — must allocate nothing per lookup, for every
/// <see cref="NodeKind"/>. Both are <c>static readonly FrozenDictionary</c> lookups rather than a
/// <c>$"node.{node.Kind}"</c> interpolation and a freshly built <c>KeyValuePair&lt;string, object?&gt;</c>,
/// so the delta must be zero-ish (well under 1KB across a thousand iterations) with or without a
/// listener — unlike <see cref="SpanParentageTests"/>, this file's assertion does not depend on whether an
/// <see cref="System.Diagnostics.ActivityListener"/> happens to be registered, since it measures the
/// cached-lookup path directly rather than a full node dispatch (which would otherwise swamp the signal
/// with unrelated executor/DuckDB allocations).</summary>
public sealed class NodeDispatchAllocationTests
{
    private static readonly NodeKind[] Kinds =
    [
        NodeKind.SourceLoad, NodeKind.Pipeline, NodeKind.Check, NodeKind.SinkWrite,
    ];

    [Fact]
    public void Cached_span_names_and_kind_tags_allocate_nothing_per_dispatch()
    {
        // Warm-up: JIT the lookup path and force FrozenDictionary's own one-time construction (already
        // triggered by the static field initializers, but this also exercises every key at least once)
        // before measuring.
        foreach (var kind in Kinds)
        {
            _ = RunOrchestrator.SpanNames[kind];
            _ = RunOrchestrator.NodeKindTags[kind];
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            foreach (var kind in Kinds)
            {
                _ = RunOrchestrator.SpanNames[kind];
                _ = RunOrchestrator.NodeKindTags[kind];
            }
        }

        var delta = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(delta < 1024,
            $"expected under 1KB allocated across 4000 cached span-name/tag lookups, got {delta} bytes");
    }

    [Fact]
    public void Every_node_kind_has_a_span_name_and_tag()
    {
        // Enum.GetValues, not the hand-curated `Kinds` array above: a fifth NodeKind added without a
        // matching SpanNames/NodeKindTags entry must fail this test (KeyNotFoundException) rather than
        // crash at runtime the first time RunOrchestrator dispatches that kind.
        foreach (var kind in Enum.GetValues<NodeKind>())
        {
            Assert.Equal($"node.{kind}", RunOrchestrator.SpanNames[kind]);
            var tag = RunOrchestrator.NodeKindTags[kind];
            Assert.Equal("pz.node.kind", tag.Key);
            Assert.Equal(kind.ToString(), tag.Value);
        }
    }
}
