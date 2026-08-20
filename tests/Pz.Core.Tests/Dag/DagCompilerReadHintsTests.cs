using Pz.Core.Dag;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>DagCompiler asks the parser seam what the dataset's single reading pipeline lets pz push,
/// attaches the answer to the SourceLoad node, and folds it into that node's content hash. Extraction
/// itself is exercised against the real parser in <c>Pz.DuckDb.Tests/DuckDbReadHintsTests.cs</c> —
/// Pz.Core.Tests never touches DuckDB, so these facts stub the reader and test the wiring only.</summary>
public class DagCompilerReadHintsTests
{
    /// <summary>Returns a canned plan for every dataset and recognizes no watermark comparison.</summary>
    private sealed class HintingSqlAstReader(ReadHintPlan plan) : ISqlAstReader
    {
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels) => new([], [], sql);

        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) => plan;
    }

    private static SourceDatasetDef CompileWith(ReadHintPlan plan, out NodeId id)
    {
        var project = Project(
            [Pipe("stg", "select id, amount from {{ source('crm', 'orders') }} where status = 'open'")],
            sources: [Crm("orders")]);
        var dag = DagCompiler.Compile(project, Ctx(project), null, new HintingSqlAstReader(plan));
        var node = Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad);
        id = node.Id;
        return Assert.IsType<SourceDatasetDef>(node.Definition);
    }

    [Fact]
    public void Source_load_node_carries_hints_from_its_single_reader()
    {
        var def = CompileWith(new ReadHintPlan(["amount", "id", "status"], "status = 'open'"), out _);

        Assert.NotNull(def.Hints);
        Assert.Equal(new[] { "amount", "id", "status" }, def.Hints!.Columns!);
        Assert.Equal("status = 'open'", def.Hints.PredicateSql);
    }

    [Fact]
    public void Different_hints_produce_a_different_source_load_node_id()
    {
        // Hints feed the hash: the staged table's shape depends on them, so retry must not reuse a
        // staged table whose columns no longer match the SQL that produced it.
        NodeId IdFor(ReadHintPlan plan)
        {
            CompileWith(plan, out var id);
            return id;
        }

        Assert.NotEqual(IdFor(new ReadHintPlan(["id"], null)), IdFor(new ReadHintPlan(["id", "amount"], null)));
        Assert.NotEqual(IdFor(new ReadHintPlan(["id"], null)), IdFor(new ReadHintPlan(["id"], "status = 'open'")));
        Assert.NotEqual(IdFor(new ReadHintPlan(null, null)), IdFor(new ReadHintPlan(["id"], null)));
    }

    [Fact]
    public void No_parser_wired_means_no_hints_and_the_pre_pushdown_node_id()
    {
        // A caller with no ISqlAstReader pushes nothing, and its node hashes exactly as it would with
        // no pushdown at all.
        var project = Project(
            [Pipe("stg", "select id from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")]);

        var node = Assert.Single(DagCompiler.Compile(project, Ctx(project)).Nodes, n => n.Kind == NodeKind.SourceLoad);

        Assert.Null(Assert.IsType<SourceDatasetDef>(node.Definition).Hints);
        Assert.Equal(node.Id, Assert.Single(
            DagCompiler.Compile(project, Ctx(project), null, new HintingSqlAstReader(ReadHintPlan.None)).Nodes,
            n => n.Kind == NodeKind.SourceLoad).Id);
    }
}
