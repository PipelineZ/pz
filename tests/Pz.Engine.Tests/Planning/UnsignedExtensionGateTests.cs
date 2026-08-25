using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Planning;

/// <summary>PZ0359: a native scan/copy whose setup loads or installs a packaged (quoted-path) DuckDB
/// extension is never signature-verified, unlike a bare-name `LOAD`/`INSTALL` resolved from DuckDB's
/// own signed repository. Without `allow_unsigned_extensions: true` on the connection, the planner
/// refuses the native tier for that edge and falls back to the universal path -- this is a tier choice,
/// not a hard validation failure, so it never throws PzValidationException (see
/// ExecutionPlanner.HasUnsignedPackagedExtension). The detection is deliberately adversarial-input
/// tested here: a real connector's SetupStatements is only ever ONE well-formed element, but the gate
/// exists to catch a quoted path wherever one appears, not just the tidiest phrasing of it.</summary>
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
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
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
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
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

    // -- Adversarial-input detection: the gate must catch a quoted-path LOAD/INSTALL wherever it
    // appears in a SetupStatements element, not just as the whole trimmed string. Each case below is a
    // bypass a naive "does this element start with LOAD " check would miss.

    [Fact]
    public async Task A_second_statement_after_a_semicolon_is_still_caught()
    {
        // One SetupStatements element covering two SQL statements -- the packaged LOAD is not the
        // first one.
        var sink = new StubConfigurableSetupStatementsSink("INSTALL httpfs; LOAD '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }

    [Fact]
    public async Task A_load_preceded_by_a_line_comment_is_still_caught()
    {
        var sink = new StubConfigurableSetupStatementsSink("-- setup\nLOAD '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }

    [Theory]
    [InlineData("LOAD\t'/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'")]
    [InlineData("LOAD\n'/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'")]
    public async Task A_load_separated_from_its_argument_by_tab_or_newline_is_still_caught(string setup)
    {
        var sink = new StubConfigurableSetupStatementsSink(setup);
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }

    // -- INSTALL: the same property ("no unsigned packaged extension without consent") holds over
    // INSTALL, not just LOAD -- INSTALL '<path>' stages an unsigned extension on disk even with no
    // matching LOAD in the same setup, and a bare-name LOAD of an already-installed unsigned extension
    // would otherwise read as signed.

    [Fact]
    public async Task Packaged_install_without_the_flag_falls_back_to_universal()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "INSTALL '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
        Assert.Contains("stub_sink", node.Reason);
        Assert.Contains("allow_unsigned_extensions: true", node.Reason);
        // Same secret-hygiene pin as the LOAD case: no SetupStatements text in the reason.
        Assert.DoesNotContain("INSTALL", node.Reason);
        Assert.DoesNotContain(".pz/packages", node.Reason);
        Assert.DoesNotContain("ext.duckdb_extension", node.Reason);
    }

    [Fact]
    public async Task Bare_name_install_passes_without_the_flag()
    {
        var sink = new StubConfigurableSetupStatementsSink("INSTALL httpfs");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    /// <summary>The exact bypass shape a quoted-path INSTALL followed by a bare-name LOAD of the
    /// installed extension would be if only LOAD were gated: nothing in the LOAD statement itself
    /// names a path, so a LOAD-only check would wave it through.</summary>
    [Fact]
    public async Task Install_with_a_path_then_a_bare_load_of_it_is_still_caught()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "INSTALL './ext.duckdb_extension'; LOAD ext;");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }

    [Fact]
    public async Task Packaged_install_with_the_flag_plans_native()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "INSTALL '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: true);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    // -- Block comments: a `--` line comment was already stripped before the keyword and around its
    // argument (see the tab/newline/comment cases above); `/* ... */` needs the same treatment in both
    // positions, because DuckDB's own tokenizer treats a block comment as just another token boundary --
    // "LOAD/*c*/'x'" is exactly as legal to DuckDB as "LOAD 'x'".

    [Fact]
    public async Task A_load_preceded_by_a_block_comment_is_still_caught()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "/* block comment */LOAD '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
        Assert.DoesNotContain(".pz/packages", node.Reason);
        Assert.DoesNotContain("ext.duckdb_extension", node.Reason);
    }

    [Fact]
    public async Task A_load_separated_from_its_argument_by_a_block_comment_is_still_caught()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "LOAD/*c*/'/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
        Assert.DoesNotContain(".pz/packages", node.Reason);
        Assert.DoesNotContain("ext.duckdb_extension", node.Reason);
    }

    [Fact]
    public async Task A_signed_load_with_a_block_comment_before_its_argument_still_passes()
    {
        var sink = new StubConfigurableSetupStatementsSink("LOAD /*c*/ delta");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.NativeCopy, node.Strategy);
    }

    [Fact]
    public async Task A_nested_block_comment_before_the_keyword_is_still_caught()
    {
        var sink = new StubConfigurableSetupStatementsSink(
            "/* outer /* inner */ still outer */LOAD '/abs/path/inside/.pz/packages/x/1.0/ext.duckdb_extension'");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }

    /// <summary>An unparseable statement (a `/*` with no matching `*/`) must never fail open -- there is
    /// no safe way to classify text a syntactic scanner cannot fully consume, so the gate refuses the
    /// native tier rather than guess it is signed.</summary>
    [Fact]
    public async Task An_unterminated_block_comment_fails_closed()
    {
        var sink = new StubConfigurableSetupStatementsSink("/* never closed\nLOAD delta");
        var (dag, registry) = TestDags.DagAndRegistryWithStubSinkSetup(sink, allowUnsignedExtensions: false);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        var node = Assert.Single(plan.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal(EdgeStrategy.ArrowStream, node.Strategy);
        Assert.Contains(PzErrorCode.UnsignedExtensionRefused, node.Reason);
    }
}
