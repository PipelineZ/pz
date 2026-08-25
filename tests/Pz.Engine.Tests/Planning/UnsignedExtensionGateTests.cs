using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>PZ0359: a native scan/copy whose setup loads a packaged (quoted-path) DuckDB extension is
/// never signature-verified, unlike a bare-name `LOAD` resolved from DuckDB's own signed repository.
/// Without `allow_unsigned_extensions: true` on the connection, the planner refuses the native tier for
/// that edge and falls back to the universal path -- this is a tier choice, not a hard validation
/// failure, so it never throws PzValidationException (see ExecutionPlanner.IsUnsignedPackagedExtensionLoad).</summary>
public sealed class UnsignedExtensionGateTests
{
    private const string PackagedLoad =
        "LOAD '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'";

    [Fact]
    public async Task Packaged_load_without_the_flag_falls_back_to_universal_sink()
    {
        var sink = new StubConfigurableSetupStatementsSink(PackagedLoad);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains("PZ0359", node.Reason);
        Assert.Contains("stub_sink", node.Reason);
        Assert.Contains("allow_unsigned_extensions: true", node.Reason);
        // Secret hygiene: the reason must never carry the SetupStatements text or the packaged path.
        Assert.DoesNotContain("LOAD", node.Reason);
        Assert.DoesNotContain(".pz/packages", node.Reason);
        Assert.DoesNotContain("ext.duckdb_extension", node.Reason);
    }

    [Fact]
    public async Task Packaged_load_with_the_flag_plans_native_sink()
    {
        var sink = new StubConfigurableSetupStatementsSink(PackagedLoad);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: true);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    [Fact]
    public async Task Signed_bare_name_load_passes_without_the_flag_sink()
    {
        var sink = new StubConfigurableSetupStatementsSink("LOAD delta");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    [Fact]
    public async Task No_setup_statements_plans_native_sink()
    {
        var sink = new StubConfigurableSetupStatementsSink();
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    [Fact]
    public async Task Packaged_load_without_the_flag_falls_back_to_universal_source()
    {
        var source = new StubConfigurableSetupStatementsSource(PackagedLoad);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceSetup(source, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains("PZ0359", node.Reason);
        Assert.Contains("stub_source", node.Reason);
        Assert.Contains("allow_unsigned_extensions: true", node.Reason);
        Assert.DoesNotContain("LOAD", node.Reason);
        Assert.DoesNotContain(".pz/packages", node.Reason);
    }

    [Fact]
    public async Task Packaged_load_with_the_flag_plans_native_source()
    {
        var source = new StubConfigurableSetupStatementsSource(PackagedLoad);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceSetup(source, allowUnsignedExtensions: true);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, node.Strategy);
    }

    [Fact]
    public async Task Signed_bare_name_load_passes_without_the_flag_source()
    {
        var source = new StubConfigurableSetupStatementsSource("LOAD delta");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSourceSetup(source, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal(EdgeStrategy.NativeScan, node.Strategy);
    }
}
