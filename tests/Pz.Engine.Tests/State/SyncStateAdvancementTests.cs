using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>Mirrors the direct-NodeResult-construction tests in
/// <c>WatermarkFlowTests</c> (e.g. <c>Carried_forward_sink_blocks_advancement_when_its_source_fell_back</c>,
/// <c>Reused_source_with_carried_forward_sink_advances</c>) that build a <see cref="CompiledDag"/> plus
/// <see cref="NodeResult"/>s by hand rather than driving the dispatcher — the commit-gate + carried-forward
/// rule is a pure function of <see cref="NodeResult.SyncStateCandidate"/>/<see cref="NodeResult.Provenance"/>,
/// so no real extraction is needed to exercise it. Covers the same three gate behaviors as
/// <see cref="SyncStateAdvancement"/>'s doc comment, substituted onto a `sync:` dataset and a
/// <see cref="SyncStateStore"/>.</summary>
public sealed class SyncStateAdvancementTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-syncstate-advancement-tests", Guid.NewGuid().ToString("N"));
    private readonly SyncStateStore _store;

    public SyncStateAdvancementTests()
    {
        Directory.CreateDirectory(_dir);
        _store = SyncStateStore.Local(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SyncSourceLoadNode(NodeId id, string sourceName, string datasetName)
    {
        var dataset = new DatasetDef(datasetName, new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Auto, null));
        var source = new ConnectionDef(sourceName, "http", new Dictionary<string, object?>(), [dataset], $"sources/{sourceName}.yml");
        return new DagNode(id, NodeKind.SourceLoad, $"src_{sourceName}__{datasetName}", [], null, new SourceDatasetDef(source, dataset));
    }

    private static DagNode SinkNode(NodeId id, NodeId dependsOn, string input, string sinkName = "cap", string outputName = "out")
    {
        var sink = new ConnectionDef(sinkName, "inmemory", new Dictionary<string, object?>(), [], $"sinks/{sinkName}.yml") { Outputs = [new OutputDef(outputName, input, "replace", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(id, NodeKind.SinkWrite, $"{sinkName}.{outputName}", [dependsOn], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public void All_descendant_sinks_committed_advances_sync_state()
    {
        var sourceId = new NodeId("aaaaaaaaaaaaaaaa");
        var sinkId = new NodeId("bbbbbbbbbbbbbbbb");
        var key = SyncStateStore.Key("http", "widgets");

        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "widgets"),
            SinkNode(sinkId, sourceId, "src_http__widgets"),
        ]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__widgets",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-1", "run-1"));
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 10, TimeSpan.Zero, null);

        SyncStateAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("delta-token-1", stored!.Token);
        Assert.Equal("run-1", stored.RunId);
    }

    [Fact]
    public void Descendant_sink_absent_blocks_advancement()
    {
        var sourceId = new NodeId("cccccccccccccccc");
        var sinkId = new NodeId("dddddddddddddddd");
        var key = SyncStateStore.Key("http", "absent_sink");

        // Structural dag includes the sink, but this run's effective results never produced a NodeResult
        // for it at all (e.g. a partial --select) -- the sink is un-committed and must block advancement.
        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "absent_sink"),
            SinkNode(sinkId, sourceId, "src_http__absent_sink"),
        ]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__absent_sink",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-2", "run-1"));

        SyncStateAdvancement.Advance(dag, [sourceResult], _store);

        Assert.Null(_store.Get(key));
    }

    [Fact]
    public void Descendant_sink_failed_blocks_advancement()
    {
        var sourceId = new NodeId("eeeeeeeeeeeeeeee");
        var sinkId = new NodeId("ffffffffffffffff");
        var key = SyncStateStore.Key("http", "failed_sink");

        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "failed_sink"),
            SinkNode(sinkId, sourceId, "src_http__failed_sink"),
        ]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__failed_sink",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-3", "run-1"));
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Failed, 0, TimeSpan.Zero, new PzError("PZ0001", "boom", null, null, null));

        SyncStateAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        Assert.Null(_store.Get(key));
    }

    [Fact]
    public void Carried_forward_sink_blocks_advancement_when_its_source_fell_back()
    {
        var sourceId = new NodeId("f1f1f1f1f1f1f1f1");
        var sinkId = new NodeId("f2f2f2f2f2f2f2f2");
        var key = SyncStateStore.Key("http", "fellback");

        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "fellback"),
            SinkNode(sinkId, sourceId, "src_http__fellback"),
        ]);

        // SourceLoad fell back to re-extraction (Provenance == null, not Reused) and captured a NEW
        // token that the prior slice's carried sink never saw.
        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__fellback",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-new", "retry-run"));
        // The carried-forward sink: Success recorded from the PRIOR slice, never actually ran this retry.
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 5, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward);

        SyncStateAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        Assert.Null(_store.Get(key));
    }

    [Fact]
    public void Reused_source_with_carried_forward_sink_advances()
    {
        var sourceId = new NodeId("f3f3f3f3f3f3f3f3");
        var sinkId = new NodeId("f4f4f4f4f4f4f4f4");
        var key = SyncStateStore.Key("http", "reusedok");

        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "reusedok"),
            SinkNode(sinkId, sourceId, "src_http__reusedok"),
        ]);

        // Counterpart: the SourceLoad genuinely reused the prior slice (Provenance == Reused), so the
        // carried sink's recorded success DOES vouch for exactly this run's slice -> advancement is sound.
        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__reusedok",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-4", "retry-run"),
            Provenance: NodeProvenance.Reused);
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 5, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward);

        SyncStateAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        Assert.Equal("delta-token-4", _store.Get(key)!.Token);
    }

    [Fact]
    public void Null_candidate_leaves_store_untouched()
    {
        var sourceId = new NodeId("a1a1a1a1a1a1a1a1");
        var sinkId = new NodeId("a2a2a2a2a2a2a2a2");
        var key = SyncStateStore.Key("http", "nocandidate");
        var previous = new SyncState("prior-token", "prior-run");
        _store.Set(key, previous);

        var dag = new CompiledDag([
            SyncSourceLoadNode(sourceId, "http", "nocandidate"),
            SinkNode(sinkId, sourceId, "src_http__nocandidate"),
        ]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__nocandidate",
            NodeStatus.Success, 0, TimeSpan.Zero, null);
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 0, TimeSpan.Zero, null);

        SyncStateAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        Assert.Equal(previous, _store.Get(key));
    }

    [Fact]
    public void Dataset_with_no_sink_advances_on_source_success()
    {
        var sourceId = new NodeId("a5a5a5a5a5a5a5a5");
        var key = SyncStateStore.Key("http", "solo");
        var dag = new CompiledDag([SyncSourceLoadNode(sourceId, "http", "solo")]);

        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_http__solo",
            NodeStatus.Success, 5, TimeSpan.Zero, null,
            SyncStateCandidate: new SyncState("delta-token-5", "run-1"));

        SyncStateAdvancement.Advance(dag, [sourceResult], _store);

        Assert.Equal("delta-token-5", _store.Get(key)!.Token);
    }
}
