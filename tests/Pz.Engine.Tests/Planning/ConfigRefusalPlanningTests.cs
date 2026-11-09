using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>What a connector throws while the planner probes it (opening the connection, resolving a dataset's read
/// shape, probing the native path) is classified, aggregated and coded -- never an exception escaping the plan,
/// which the CLI renders as PZ0500 "a bug in pz". Only a refusal coded <see cref="PzConnectorErrorCodes.Config"/>
/// is a config error (PZ0301); any other connector failure, transient or not, is PZ0330, and a host failure keeps
/// its own code. A connection-level refusal is reported once per connection, however many datasets and outputs
/// use it.</summary>
public sealed class ConfigRefusalPlanningTests
{
    private static PzConnectorException Config(string message) =>
        new(message, isTransient: false, code: PzConnectorErrorCodes.Config);

    /// <summary>Connection 'api' read twice (datasets 'orders' and 'items') and written once (output 'out').</summary>
    private static (CompiledDag Dag, ConnectorRegistry Registry) TwoDatasetsAndAnOutput(StubThrowingConnector connector)
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", connector);
        registry.AddSink("stub", connector);

        var api = new ConnectionDef("api", "stub", new Dictionary<string, object?>(),
            [
                new DatasetDef("orders", new Dictionary<string, object?>(), null),
                new DatasetDef("items", new Dictionary<string, object?>(), null),
            ], "connections.yml");
        var orders = new DagNode(new NodeId("1111111111111111"), NodeKind.SourceLoad, "src_api__orders", [], null,
            new SourceDatasetDef(api, api.Datasets[0]));
        var items = new DagNode(new NodeId("1111111111111112"), NodeKind.SourceLoad, "src_api__items", [], null,
            new SourceDatasetDef(api, api.Datasets[1]));
        var pipelineDef = new PipelineDef("joined", "select 1", "table", [], [], "pipelines/joined.sql");
        var pipeline = new DagNode(new NodeId("2222222222222222"), NodeKind.Pipeline, "joined", [orders.Id, items.Id],
            pipelineDef.RawSql, pipelineDef);
        var sinkDef = api with
        {
            Outputs = [new OutputDef("out", "joined", "replace", "fail_on_change", new Dictionary<string, object?>(), [])],
        };
        var sink = new DagNode(new NodeId("3333333333333333"), NodeKind.SinkWrite, "api.out", [pipeline.Id], null,
            new SinkOutputDef(sinkDef, sinkDef.Outputs[0]));
        return (new CompiledDag([orders, items, pipeline, sink]), registry);
    }

    private static async Task<IReadOnlyList<PzError>> PlanErrorsAsync(CompiledDag dag, ConnectorRegistry registry,
        IReadOnlySet<NodeId>? executing = null) => (await PlanFailureAsync(dag, registry, executing)).Errors;

    private static async Task<PzValidationException> PlanFailureAsync(CompiledDag dag, ConnectorRegistry registry,
        IReadOnlySet<NodeId>? executing = null) =>
        await Assert.ThrowsAsync<PzValidationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: executing));

    // A config refusal is the author's to fix (exit 2); a connector that failed rather than refused -- transient,
    // a defect -- means pz could not plan on its own terms (exit 3). A host failure is fatal exactly when the
    // host's description says so (the registry builder owns that mapping): the planner never reads an error code.
    [Theory]
    [InlineData("config", false)]
    [InlineData("transient", true)]
    [InlineData("unhandled", true)]
    [InlineData("host-fatal", true)]
    [InlineData("host-not-fatal", false)]
    public async Task A_plan_failure_is_fatal_only_when_a_connector_failed_rather_than_refused(string kind, bool fatal)
    {
        var connector = new StubThrowingConnector
        {
            OpenThrows = () => kind switch
            {
                "config" => Config("bad"),
                "transient" => new PzConnectorException("timed out", isTransient: true),
                "unhandled" => new PzConnectorException("unhandled NullReferenceException", isTransient: false,
                    code: PzConnectorErrorCodes.Unhandled),
                _ => new InvalidProgramException(kind),
            },
        };
        var (dag, registry) = TwoDatasetsAndAnOutput(connector);
        // The same code either way: only the host's own verdict decides.
        registry.DescribeHostFailure = ex => ex is InvalidProgramException
            ? new ConnectorHostFailure(PzErrorCode.ConnectorSpawnFailed, "host", null) { IsFatal = ex.Message == "host-fatal" }
            : null;

        var failure = await PlanFailureAsync(dag, registry);

        Assert.Equal(fatal, failure.IsFatal);
    }

    [Fact]
    public async Task A_config_refusal_of_a_dataset_is_PZ0301_naming_the_connection_and_dataset()
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            ShapeThrows = spec => spec.Dataset == "items" ? Config("option 'x' clashes with option 'y'") : null,
        });

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'api' dataset 'items': option 'x' clashes with option 'y'", error.Message);
        Assert.Equal("connections.yml", error.File);
        Assert.Equal("fix the options the connector names", error.Hint);
    }

    [Fact]
    public async Task A_connection_refused_at_open_is_reported_once_for_every_dataset_and_output()
    {
        var connector = new StubThrowingConnector { OpenThrows = () => Config("'base_url' must be an absolute URL") };
        var (dag, registry) = TwoDatasetsAndAnOutput(connector);

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'api': 'base_url' must be an absolute URL", error.Message);
        Assert.Equal("connections.yml", error.File);
    }

    [Fact]
    public async Task A_sink_connection_refused_at_open_is_PZ0301()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubNativeSource(),
            new StubThrowingConnector { OpenThrows = () => Config("'database' is required") });

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectorConfigInvalid, error.Code);
        Assert.Equal("connection 'stub': 'database' is required", error.Message);
        Assert.Equal("sinks/stub.yml", error.File);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(PzConnectorErrorCodes.Unhandled)]
    [InlineData("PZ9999")]
    public async Task A_connector_failure_that_is_not_a_config_refusal_is_PZ0330(string? code)
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            OpenThrows = () => new PzConnectorException("unhandled NullReferenceException: boom", isTransient: false, code: code),
        });

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
        Assert.Equal("connection 'api': connector 'stub' failed while planning: unhandled NullReferenceException: boom",
            error.Message);
        Assert.DoesNotContain("fix the options", error.Hint);
    }

    [Fact]
    public async Task A_transient_failure_while_planning_is_PZ0330_and_says_to_re_run()
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            ShapeThrows = _ => new PzConnectorException("connector process exited", isTransient: true),
        });

        var errors = await PlanErrorsAsync(dag, registry);

        Assert.Equal(2, errors.Count);
        Assert.All(errors, e => Assert.Equal(PzErrorCode.ConnectionCheckFailed, e.Code));
        Assert.Equal("connection 'api' dataset 'orders': connector 'stub' failed while planning (transient; re-run): " +
            "connector process exited", errors[0].Message);
    }

    [Fact]
    public async Task A_transient_native_probe_failure_is_not_a_native_path_mismatch()
    {
        var (dag, registry) = TestDags.SourcePipelineSink(new StubThrowingConnector
        {
            NativeScanThrows = () => new PzConnectorException("TryGetNativeScan timed out", isTransient: true),
        }, new StubUniversalSink());

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectionCheckFailed, error.Code);
        Assert.Contains("(transient; re-run): TryGetNativeScan timed out", error.Message);
    }

    [Fact]
    public async Task A_host_failure_keeps_the_hosts_own_code_and_hint()
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            OpenThrows = () => new InvalidProgramException("spawn failed: no such file"),
        });
        registry.DescribeHostFailure = ex => ex is InvalidProgramException
            ? new ConnectorHostFailure(PzErrorCode.ConnectorSpawnFailed, ex.Message, "check the executable")
            : null;

        var error = Assert.Single(await PlanErrorsAsync(dag, registry));

        Assert.Equal(PzErrorCode.ConnectorSpawnFailed, error.Code);
        Assert.Equal("connection 'api': spawn failed: no such file", error.Message);
        Assert.Equal("check the executable", error.Hint);
    }

    [Fact]
    public async Task An_engine_defect_still_escapes()
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            OpenThrows = () => new InvalidOperationException("engine bug"),
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None));
    }

    [Fact]
    public async Task A_refusal_outside_the_effective_set_is_recorded_not_raised()
    {
        var (dag, registry) = TwoDatasetsAndAnOutput(new StubThrowingConnector
        {
            ShapeThrows = _ => Config("dataset refused"),
        });

        var plan = await new ExecutionPlanner(registry)
            .PlanAsync(dag, forceUniversal: false, CancellationToken.None, executing: new HashSet<NodeId>());

        var load = plan.Nodes.First(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(
            "arrow stream: connector 'stub' refused while planning; PZ0301 deferred because this node is not part of the run",
            load.Reason);
    }
}
