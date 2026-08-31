using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Planning;

/// <summary>Shared DAG fixture for planner tests: a 3-node source -> pipeline -> sink chain, all wired
/// to connector name "stub" so the same stub source/sink pair can be swapped in per test.</summary>
internal static class TestDags
{
    public static (CompiledDag Dag, ConnectorRegistry Registry) SourcePipelineSink(
        ISourceConnector source, ISinkConnector sink, IReadOnlyDictionary<string, object?>? datasetOptions = null)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", source);
        registry.AddSink("stub", sink);

        var sourceDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(),
            [new DatasetDef("orders", datasetOptions ?? new Dictionary<string, object?>(), null)],
            "sources/stub.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_stub__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_stub__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [],
            "sinks/stub.yml") { Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>(),
                [])] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "stub.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return (new CompiledDag([loadNode, pipelineNode, sinkNode]), registry);
    }

    /// <summary>Same 3-node source -> pipeline -> sink shape as <see cref="SourcePipelineSink"/>, but for
    /// planner-gate tests that need a full caller-supplied <see cref="DatasetDef"/> (e.g. one carrying an
    /// <see cref="IncrementalDef"/>) paired with a <see cref="StubConfigurableCapabilitiesSource"/> set to
    /// exactly the declared <paramref name="capabilities"/> — the fixed-capability stubs above can't
    /// express "same connector, different capability flags" in one call site.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSource(
        DatasetDef dataset, ConnectorCapabilities capabilities, ReadHintPlan? hints = null) =>
        DagAndRegistryWithStubSource(dataset, new StubConfigurableCapabilitiesSource(capabilities), hints: hints);

    /// <summary>Overload taking a caller-supplied connector
    /// instance directly -- lets a read-shape test pass <see cref="StubFeedSource"/> (or any other
    /// purpose-built stub) instead of the fixed-natural-shape <see cref="StubConfigurableCapabilitiesSource"/>
    /// the capabilities-only overload above always builds.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSource(
        DatasetDef dataset, ISourceConnector source, OutputDef? output = null, ReadHintPlan? hints = null)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", source);
        registry.AddSink("stub", new StubUniversalSink());

        // Default sink is `replace` (StubUniversalSink declares ReplaceWrites). A feed-resolving source
        // feeding that trips the feed x replace refusal (PZ0335) -- feed-source callers pass an
        // append + duplicates: accept output instead.
        output ??= new OutputDef("out", $"stg_{dataset.Name}", "replace", "fail_on_change", new Dictionary<string, object?>());

        var sourceDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [dataset], "sources/stub.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, $"src_stub__{dataset.Name}",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0], hints));

        var pipelineDef = new PipelineDef($"stg_{dataset.Name}", $"select * from staging.src_stub__{dataset.Name}",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, $"stg_{dataset.Name}",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml") { Outputs = [output] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "stub.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return (new CompiledDag([loadNode, pipelineNode, sinkNode]), registry);
    }

    /// <summary>A feed-compatible sink output (append + duplicates: accept) -- for planner tests that pair
    /// a Feed-resolving source with <see cref="DagAndRegistryWithStubSource(DatasetDef, ISourceConnector, OutputDef?)"/>
    /// without tripping the feed x replace refusal (PZ0335) the default replace output would.</summary>
    public static OutputDef FeedCompatibleOutput() =>
        new("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>(), [], AcceptDuplicates: true);

    /// <summary>Feed-row pairing-matrix fixture -- a
    /// <see cref="StubFeedSource"/> (resolves Feed for every dataset) feeding one sink output whose write
    /// strategy the caller picks, on a <see cref="StubConfigurableCapabilitiesSink"/> declaring exactly
    /// <paramref name="sinkCapabilities"/>. Exercises ExecutionPlanner's feed-side PZ0214/PZ0335 pass for
    /// each (feed x write) cell without a hand-built DAG per test.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) FeedSourceToSink(
        OutputDef output, ConnectorCapabilities sinkCapabilities)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubFeedSource(ConnectorCapabilities.None));
        registry.AddSink("stub", new StubConfigurableCapabilitiesSink(sinkCapabilities));

        var dataset = new DatasetDef("orders", new Dictionary<string, object?> { ["table"] = "orders" }, null);
        var sourceDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [dataset], "sources/stub.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_stub__orders",
            [], null, new SourceDatasetDef(sourceDef, dataset));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_stub__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml") { Outputs = [output] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "stub.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, output));

        return (new CompiledDag([loadNode, pipelineNode, sinkNode]), registry);
    }

    /// <summary>Same 3-node shape, but for planner-gate tests that need a caller-supplied
    /// <see cref="OutputDef"/> (e.g. one declaring <c>partition_by</c>) paired with a
    /// <see cref="StubConfigurableCapabilitiesSink"/> set to exactly the declared
    /// <paramref name="capabilities"/> — the sink-side mirror of
    /// <see cref="DagAndRegistryWithStubSource"/>.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSink(
        OutputDef output, ConnectorCapabilities capabilities)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubUniversalSource());
        registry.AddSink("stub", new StubConfigurableCapabilitiesSink(capabilities));

        var sourceDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(), null)], "sources/stub.yml");
        var loadNode = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_stub__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));

        var pipelineDef = new PipelineDef("stg_orders", "select * from staging.src_stub__orders",
            "table", [], [], "pipelines/stg_orders.sql");
        var pipelineNode = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "stg_orders",
            [loadNode.Id], pipelineDef.RawSql, pipelineDef);

        var sinkDef = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml") { Outputs = [output] };
        var sinkNode = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "stub.out",
            [pipelineNode.Id], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return (new CompiledDag([loadNode, pipelineNode, sinkNode]), registry);
    }

    /// <summary>Source-only DAG (no pipeline/sink nodes -- PZ0317's gate only touches SourceLoad
    /// nodes) for pacing-capability-gate tests: one <see cref="ConnectionDef"/> instance
    /// carrying <paramref name="rateLimit"/>, feeding one SourceLoad node per name in
    /// <paramref name="datasetNames"/>. Distinct instance name ("orders_source") vs. connector name
    /// ("stub") so tests can assert the error names each independently. Multiple dataset names let
    /// the dedup test (one instance, several datasets) assert a single PZ0317 survives the per-node
    /// planning loop.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSourceRateLimit(
        ConnectorCapabilities capabilities, RateLimitDef? rateLimit, params string[] datasetNames)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubConfigurableCapabilitiesSource(capabilities));

        var datasets = datasetNames
            .Select(name => new DatasetDef(name, new Dictionary<string, object?>(), null))
            .ToList();
        var sourceDef = new ConnectionDef("orders_source", "stub", new Dictionary<string, object?>(), datasets,
            "sources/stub.yml", RateLimit: rateLimit);

        var nodes = datasets
            .Select((dataset, i) => new DagNode(new NodeId($"src-rl-{i:D2}"), NodeKind.SourceLoad,
                $"src_stub__{dataset.Name}", [], null, new SourceDatasetDef(sourceDef, dataset)))
            .ToList<DagNode>();

        return (new CompiledDag(nodes), registry);
    }

    /// <summary>Source-only DAG mirror of <see cref="DagAndRegistryWithStubSourceRateLimit"/>, but
    /// stamping <paramref name="syncMode"/> on every dataset instead of a rate limit -- the cdc
    /// dedup test needs several cdc datasets sharing one source instance to prove PZ0338 collapses
    /// to a single error per instance, the same shape PZ0317's dedup test proves above.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSourceSyncModes(
        ConnectorCapabilities capabilities, SyncModeDef? syncMode, params string[] datasetNames)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubConfigurableCapabilitiesSource(capabilities));

        var datasets = datasetNames
            .Select(name => new DatasetDef(name, new Dictionary<string, object?>(), null, syncMode))
            .ToList();
        var sourceDef = new ConnectionDef("orders_source", "stub", new Dictionary<string, object?>(), datasets,
            "sources/stub.yml");

        var nodes = datasets
            .Select((dataset, i) => new DagNode(new NodeId($"src-cdc-{i:D2}"), NodeKind.SourceLoad,
                $"src_stub__{dataset.Name}", [], null, new SourceDatasetDef(sourceDef, dataset)))
            .ToList<DagNode>();

        return (new CompiledDag(nodes), registry);
    }

    /// <summary>Source-only DAG mirror of <see cref="DagAndRegistryWithStubSourceRateLimit"/>, backed
    /// by <see cref="StubNativeOnlySourceWithGatedOperations"/> instead of the plain configurable-
    /// capabilities stub -- the azure-shaped connector: GatedOperations declared connector-wide, but
    /// this source's read path is native-only (<see cref="INativeOnlySource"/>), so it is never
    /// opened as <see cref="IOperationGateAware"/>. Pins the PZ0317 rule: rate_limit on such a
    /// source must be refused even though the naive
    /// GatedOperations-flag check alone passes.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithNativeOnlyStubSourceRateLimit(
        RateLimitDef? rateLimit, params string[] datasetNames)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new StubNativeOnlySourceWithGatedOperations());

        var datasets = datasetNames
            .Select(name => new DatasetDef(name, new Dictionary<string, object?>(), null))
            .ToList();
        var sourceDef = new ConnectionDef("orders_source", "stub", new Dictionary<string, object?>(), datasets,
            "sources/stub.yml", RateLimit: rateLimit);

        var nodes = datasets
            .Select((dataset, i) => new DagNode(new NodeId($"src-nr-{i:D2}"), NodeKind.SourceLoad,
                $"src_stub__{dataset.Name}", [], null, new SourceDatasetDef(sourceDef, dataset)))
            .ToList<DagNode>();

        return (new CompiledDag(nodes), registry);
    }

    /// <summary>Sink-only DAG mirror of <see cref="DagAndRegistryWithStubSourceRateLimit"/>: one
    /// <see cref="ConnectionDef"/> instance ("orders_sink") carrying <paramref name="rateLimit"/>, feeding
    /// a single SinkWrite node on connector "stub".</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSinkRateLimit(
        ConnectorCapabilities capabilities, RateLimitDef? rateLimit)
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub", new StubConfigurableCapabilitiesSink(capabilities));

        var sinkDef = new ConnectionDef("orders_sink", "stub", new Dictionary<string, object?>(), [],
            "sinks/stub.yml", RateLimit: rateLimit) { Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>())] };
        var sinkNode = new DagNode(new NodeId("sink-rl-00"), NodeKind.SinkWrite, "stub.out",
            [], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return (new CompiledDag([sinkNode]), registry);
    }

    /// <summary>Sink-only single-node DAG (no source/pipeline) for the unsigned-packaged-extension gate
    /// (PZ0359) tests: one <see cref="ConnectionDef"/> instance carrying <paramref name="allowUnsignedExtensions"/>,
    /// feeding a caller-supplied sink connector so each test can set the exact `LOAD` setup statement it
    /// wants to probe. Sink-only shape mirrors <see cref="DagAndRegistryWithStubSinkRateLimit"/>.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSinkSetup(
        ISinkConnector sink, bool allowUnsignedExtensions)
    {
        var registry = new ConnectorRegistry();
        registry.AddSink("stub", sink);

        var sinkDef = new ConnectionDef("stub_sink", "stub", new Dictionary<string, object?>(), [],
            "sinks/stub.yml", AllowUnsignedExtensions: allowUnsignedExtensions)
        {
            Outputs = [new OutputDef("out", "stg_orders", "replace", "fail_on_change", new Dictionary<string, object?>())],
        };
        var sinkNode = new DagNode(new NodeId("sink-ext-00"), NodeKind.SinkWrite, "stub.out",
            [], null, new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));

        return (new CompiledDag([sinkNode]), registry);
    }

    /// <summary>Source-only single-node DAG (no pipeline/sink), the source-side mirror of
    /// <see cref="DagAndRegistryWithStubSinkSetup"/> -- carries <paramref name="allowUnsignedExtensions"/>
    /// on the source's <see cref="ConnectionDef"/> and a caller-supplied source connector so a test can set
    /// the exact `LOAD` setup statement its <see cref="NativeScan"/> reports.</summary>
    public static (CompiledDag Dag, ConnectorRegistry Registry) DagAndRegistryWithStubSourceSetup(
        ISourceConnector source, bool allowUnsignedExtensions)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", source);

        var dataset = new DatasetDef("orders", new Dictionary<string, object?>(), null);
        var sourceDef = new ConnectionDef("stub_source", "stub", new Dictionary<string, object?>(), [dataset],
            "sources/stub.yml", AllowUnsignedExtensions: allowUnsignedExtensions);
        var loadNode = new DagNode(new NodeId("src-ext-00"), NodeKind.SourceLoad, "src_stub__orders",
            [], null, new SourceDatasetDef(sourceDef, dataset));

        return (new CompiledDag([loadNode]), registry);
    }
}
