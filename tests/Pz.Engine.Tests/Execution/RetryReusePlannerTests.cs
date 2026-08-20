using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Artifacts;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Pure planning coverage for
/// <see cref="RetryReusePlanner.Plan"/> -- building the reuse manifest (which prior SourceLoads a
/// retry may copy instead of re-extracting) and the carried-forward SinkWrite results (the carry-forward
/// soundness rule). Dag shape throughout: <c>src -> pipe -> {sinkOk, sinkFail}</c>, hand-built exactly
/// like <see cref="Pz.Engine.Tests.State.WatermarkFlowTests"/> constructs its DagNodes (no template
/// rendering/DagCompiler needed -- the planner only reads Id/Kind/DependsOn off the compiled dag, so a
/// hand-built <see cref="CompiledDag"/> is a faithful, much simpler substitute).</summary>
public sealed class RetryReusePlannerTests : IDisposable
{
    private readonly string _projectDir = Path.Combine(Path.GetTempPath(), "pz-retry-reuse-tests", Guid.NewGuid().ToString("N"));

    private static readonly NodeId SrcId = new("1111111111111111");
    private static readonly NodeId PipeId = new("2222222222222222");
    private static readonly NodeId SinkOkId = new("3333333333333333");
    private static readonly NodeId SinkFailId = new("4444444444444444");

    public void Dispose()
    {
        try { Directory.Delete(_projectDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceNode(NodeId id) =>
        new(id, NodeKind.SourceLoad, "src_mem__foo", [], null,
            new SourceDatasetDef(
                new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
                    [new DatasetDef("foo", new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Incremental, new IncrementalDef("id")))],
                    "sources/mem.yml"),
                new DatasetDef("foo", new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Incremental, new IncrementalDef("id")))));

    private static DagNode PipelineNode(NodeId id, NodeId dependsOn) =>
        new(id, NodeKind.Pipeline, "pipe", [dependsOn], "select * from staging.src_mem__foo",
            new PipelineDef("pipe", "select * from staging.src_mem__foo", "table", [], [], "pipelines/pipe.sql"));

    private static DagNode SinkNode(NodeId id, NodeId dependsOn, string name) =>
        new(id, NodeKind.SinkWrite, $"{name}.out", [dependsOn], null,
            new SinkOutputDef(
                new ConnectionDef(name, "inmemory", new Dictionary<string, object?>(), [],
                    $"sinks/{name}.yml") { Outputs = [new OutputDef("out", "pipe", "replace", "fail_on_change", new Dictionary<string, object?>())] },
                new OutputDef("out", "pipe", "replace", "fail_on_change", new Dictionary<string, object?>())));

    private static CompiledDag BuildDag() => new([
        SourceNode(SrcId),
        PipelineNode(PipeId, SrcId),
        SinkNode(SinkOkId, PipeId, "sinkOk"),
        SinkNode(SinkFailId, PipeId, "sinkFail"),
    ]);

    /// <summary>Prior nodes with sinkFail recorded FAILED -- the ordinary "one sink failed, retry it"
    /// shape used by most scenarios.</summary>
    private List<PriorNode> HappyPathPriorNodes() =>
    [
        new PriorNode(SrcId.Value, "src", "success", "SourceLoad", 7, new PriorWatermark("id", "bigint", "42")),
        new PriorNode(PipeId.Value, "pipe", "success", "Pipeline", 7),
        new PriorNode(SinkOkId.Value, "sinkOk.out", "success", "SinkWrite", 5),
        new PriorNode(SinkFailId.Value, "sinkFail.out", "failed", "SinkWrite", 0),
    ];

    private void WriteStagingFile(string runId)
    {
        var paths = new RunPaths(_projectDir, runId);
        Directory.CreateDirectory(Path.GetDirectoryName(paths.StagingDbPath)!);
        File.WriteAllText(paths.StagingDbPath, "");
    }

    [Fact]
    public void Happy_path_manifests_source_and_carries_forward_the_untouched_sink()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "completed_with_failures", HappyPathPriorNodes());
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGet(SrcId, out var entry));
        Assert.Equal(7, entry.Rows);
        Assert.NotNull(entry.Watermark);
        Assert.Equal("42", entry.Watermark!.Value);
        Assert.Equal(new RunPaths(_projectDir, prior.RunId).StagingDbPath, entry.PriorStagingPath);

        var carriedSink = Assert.Single(carried);
        Assert.Equal(SinkOkId, carriedSink.Id);
        Assert.Equal(NodeStatus.Success, carriedSink.Status);
        Assert.Equal(NodeProvenance.CarriedForward, carriedSink.Provenance);
        Assert.Equal(5, carriedSink.RowsMoved);
    }

    [Fact]
    public void Full_refresh_yields_an_empty_manifest_and_no_carried_forward_results()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "completed_with_failures", HappyPathPriorNodes());
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: true);

        Assert.Equal(0, manifest.Count);
        Assert.Empty(carried);
    }

    [Fact]
    public void Missing_prior_staging_file_yields_an_empty_manifest_and_no_carried_forward_results()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "completed_with_failures", HappyPathPriorNodes());
        // Deliberately not calling WriteStagingFile -- the prior run dir/staging.duckdb never existed.
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.Equal(0, manifest.Count);
        Assert.Empty(carried);
    }

    [Fact]
    public void Changed_source_id_excludes_it_from_the_manifest_and_blocks_the_carried_forward_sink()
    {
        var dag = BuildDag();
        var priorNodes = new List<PriorNode>
        {
            // The prior run recorded the source under a DIFFERENT id -- simulates an edited source
            // (changed connector config/dataset options) between the failed run and this retry.
            new("9999999999999999", "src", "success", "SourceLoad", 7, new PriorWatermark("id", "bigint", "42")),
            new(PipeId.Value, "pipe", "success", "Pipeline", 7),
            new(SinkOkId.Value, "sinkOk.out", "success", "SinkWrite", 5),
            new(SinkFailId.Value, "sinkFail.out", "failed", "SinkWrite", 0),
        };
        var prior = new PriorRun("run-1", "completed_with_failures", priorNodes);
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.False(manifest.TryGet(SrcId, out _));
        Assert.Empty(carried); // sinkOk's SourceLoad ancestor is not reusable this retry -> not sound
    }

    [Fact]
    public void Edited_pipeline_id_leaves_the_source_reusable_but_blocks_the_carried_forward_sink()
    {
        var dag = BuildDag();
        var priorNodes = new List<PriorNode>
        {
            new(SrcId.Value, "src", "success", "SourceLoad", 7, new PriorWatermark("id", "bigint", "42")),
            // The prior run recorded the pipeline under a DIFFERENT id -- simulates an edited pipeline
            // SQL between the failed run and this retry. No prior success is recorded under the
            // CURRENT pipeline id at all.
            new("8888888888888888", "pipe", "success", "Pipeline", 7),
            new(SinkOkId.Value, "sinkOk.out", "success", "SinkWrite", 5),
            new(SinkFailId.Value, "sinkFail.out", "failed", "SinkWrite", 0),
        };
        var prior = new PriorRun("run-1", "completed_with_failures", priorNodes);
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGet(SrcId, out _)); // the source itself is unaffected -- still reusable
        Assert.Empty(carried); // but sinkOk's ancestor chain includes an edited (unrecorded) pipeline id
    }

    [Fact]
    public void Selected_sink_is_never_carried_forward_even_if_it_previously_succeeded()
    {
        var dag = BuildDag();
        var priorNodes = new List<PriorNode>
        {
            new(SrcId.Value, "src", "success", "SourceLoad", 7, new PriorWatermark("id", "bigint", "42")),
            new(PipeId.Value, "pipe", "success", "Pipeline", 7),
            new(SinkOkId.Value, "sinkOk.out", "success", "SinkWrite", 5),
            // Unlike the other scenarios, sinkFail also recorded SUCCESS in the prior run (e.g. an
            // explicit --select forces it to re-run regardless). Being in the selection must exclude it
            // from carried-forward no matter its recorded prior status.
            new(SinkFailId.Value, "sinkFail.out", "success", "SinkWrite", 3),
        };
        var prior = new PriorRun("run-1", "success", priorNodes);
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.DoesNotContain(carried, r => r.Id == SinkFailId);
    }

    [Fact]
    public void Failed_effective_source_load_becomes_a_partial_reuse_candidate()
    {
        var dag = BuildDag();
        var priorNodes = new List<PriorNode>
        {
            new(SrcId.Value, "src", "failed", "SourceLoad", 0),
            new(PipeId.Value, "pipe", "failed", "Pipeline", 0),
            new(SinkOkId.Value, "sinkOk.out", "failed", "SinkWrite", 0),
            new(SinkFailId.Value, "sinkFail.out", "failed", "SinkWrite", 0),
        };
        var prior = new PriorRun("run-1", "failed", priorNodes);
        WriteStagingFile(prior.RunId);
        // sinkFail's ancestor expansion pulls in pipe -> src, so the failed SourceLoad is in the
        // effective set (it will actually run this retry).
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, _) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGetPartial(SrcId, out var partial));
        Assert.Equal(new RunPaths(_projectDir, prior.RunId).StagingDbPath, partial.PriorStagingPath);
        Assert.False(manifest.TryGet(SrcId, out _)); // failed prior source is never a full-reuse candidate
    }

    [Fact]
    public void Successful_prior_source_load_is_never_a_partial_candidate()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "completed_with_failures", HappyPathPriorNodes());
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, _) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGet(SrcId, out _));         // prior success -> full reuse candidate
        Assert.False(manifest.TryGetPartial(SrcId, out _)); // never also a partial candidate
    }

    [Fact]
    public void Full_refresh_produces_no_partial_candidates()
    {
        var dag = BuildDag();
        var priorNodes = new List<PriorNode>
        {
            new(SrcId.Value, "src", "failed", "SourceLoad", 0),
            new(PipeId.Value, "pipe", "failed", "Pipeline", 0),
            new(SinkOkId.Value, "sinkOk.out", "failed", "SinkWrite", 0),
            new(SinkFailId.Value, "sinkFail.out", "failed", "SinkWrite", 0),
        };
        var prior = new PriorRun("run-1", "failed", priorNodes);
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, _) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: true);

        Assert.False(manifest.TryGetPartial(SrcId, out _));
        Assert.Equal(0, manifest.Count);
    }

    [Fact]
    public void Failed_effective_sink_gets_a_delivery_resume_entry()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "failed", HappyPathPriorNodes());
        WriteStagingFile(prior.RunId);
        // sinkFail recorded FAILED and is the retry selection itself -- an effective SinkWrite.
        var selection = new HashSet<NodeId> { SinkFailId };

        var (manifest, _) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGetDeliveryResume(SinkFailId, out var entry));
        Assert.Equal(new RunPaths(_projectDir, prior.RunId).StagingDbPath, entry.PriorStagingPath);
    }

    [Fact]
    public void Succeeded_prior_sink_gets_no_delivery_resume_entry()
    {
        var dag = BuildDag();
        var prior = new PriorRun("run-1", "completed_with_failures", HappyPathPriorNodes());
        WriteStagingFile(prior.RunId);
        // sinkOk recorded SUCCESS in the prior run and is itself the retry selection (an effective
        // SinkWrite) -- a success is a carry-forward candidate, never a delivery-resume one.
        var selection = new HashSet<NodeId> { SinkOkId };

        var (manifest, _) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.False(manifest.TryGetDeliveryResume(SinkOkId, out _));
    }

    [Fact]
    public void Independent_fully_succeeded_branch_source_is_excluded_from_the_manifest()
    {
        // Two independent branches: srcA -> pipeA -> sinkA (failed, the retry target) and
        // srcB -> pipeB -> sinkB (fully succeeded, never selected). srcB never runs this retry, so it must
        // NOT appear in the reuse manifest -- else `pz retry`'s "reusing N source load(s)" note overcounts.
        var srcAId = new NodeId("1111111111111111");
        var pipeAId = new NodeId("2222222222222222");
        var sinkAId = new NodeId("3333333333333333");
        var srcBId = new NodeId("5555555555555555");
        var pipeBId = new NodeId("6666666666666666");
        var sinkBId = new NodeId("7777777777777777");

        var dag = new CompiledDag([
            SourceNode(srcAId), PipelineNode(pipeAId, srcAId), SinkNode(sinkAId, pipeAId, "sinkA"),
            SourceNode(srcBId), PipelineNode(pipeBId, srcBId), SinkNode(sinkBId, pipeBId, "sinkB"),
        ]);
        var priorNodes = new List<PriorNode>
        {
            new(srcAId.Value, "srcA", "success", "SourceLoad", 7, new PriorWatermark("id", "bigint", "42")),
            new(pipeAId.Value, "pipeA", "success", "Pipeline", 7),
            new(sinkAId.Value, "sinkA.out", "failed", "SinkWrite", 0),
            new(srcBId.Value, "srcB", "success", "SourceLoad", 3, new PriorWatermark("id", "bigint", "10")),
            new(pipeBId.Value, "pipeB", "success", "Pipeline", 3),
            new(sinkBId.Value, "sinkB.out", "success", "SinkWrite", 3),
        };
        var prior = new PriorRun("run-1", "completed_with_failures", priorNodes);
        WriteStagingFile(prior.RunId);
        var selection = new HashSet<NodeId> { sinkAId };

        var (manifest, carried) = RetryReusePlanner.Plan(dag, prior, selection, _projectDir, fullRefresh: false);

        Assert.True(manifest.TryGet(srcAId, out _));  // srcA is an ancestor of the retried sinkA -> runs
        Assert.False(manifest.TryGet(srcBId, out _)); // srcB's branch fully succeeded, never selected
        Assert.Equal(1, manifest.Count);              // Count reflects only sources that will run
        // sinkB is prior-success and outside effective, but with srcB no longer in the manifest it is no
        // longer carried forward -- correct: srcB doesn't run this retry, so no advancement is pending for
        // its dataset (its watermark already advanced in the prior run when all its sinks committed).
        Assert.DoesNotContain(carried, r => r.Id == sinkBId);
    }
}
