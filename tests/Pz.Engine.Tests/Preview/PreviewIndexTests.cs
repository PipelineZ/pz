using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Preview;

namespace Pz.Engine.Tests.Preview;

/// <summary>The preview index a run writes into its staging database: what <c>pz preview --run-dir</c>
/// reads instead of the project, so a run directory alone answers a preview.</summary>
public sealed class PreviewIndexTests
{
    private static NodeId Id(string suffix) => new(suffix.PadLeft(16, '0'));

    private static PipelineDef Pipe(string name, string materialization = "table", params string[] marks) =>
        new(name, "select 1", materialization, [], [], $"pipelines/{name}.sql") { RedactedColumns = marks };

    private static (CompiledDag Dag, IReadOnlyList<PipelineDef> Pipelines) Project()
    {
        var connection = new ConnectionDef("a", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("b", new Dictionary<string, object?>(), null) { RedactedColumns = ["email"] }],
            "connections.yml");
        var stg = Pipe("stg", "table", "contact");
        var v = Pipe("v", "view");
        var eph = Pipe("eph", "ephemeral");
        var dag = new CompiledDag([
            new DagNode(Id("1"), NodeKind.SourceLoad, "src_a__b", [], null,
                new SourceDatasetDef(connection, connection.Datasets[0])),
            new DagNode(Id("2"), NodeKind.Pipeline, "stg", [Id("1")], "select 1", stg),
            new DagNode(Id("3"), NodeKind.Pipeline, "v", [Id("2")], "select 1", v),
            new DagNode(Id("4"), NodeKind.Check, "stg__not_null_id", [Id("2")], null,
                new CheckNodeDef("stg", new CheckDef("not_null", ["id"], new Dictionary<string, object?>()))),
            new DagNode(Id("5"), NodeKind.SinkWrite, "lake__out", [Id("2")], null, new object()),
        ]);
        return (dag, [stg, v, eph]);
    }

    private static async Task<DuckSession> Indexed()
    {
        var duck = DuckSession.Open(":memory:");
        var (dag, pipelines) = Project();
        await PreviewIndex.WriteAsync(duck, dag, pipelines, default);
        return duck;
    }

    [Fact]
    public async Task Writes_one_row_per_source_and_pipeline_with_their_marks()
    {
        await using var duck = await Indexed();

        var rows = await duck.ScalarAsync<string>(
            "select string_agg(node || '|' || relation || '|' || array_to_string(redacted_columns, ','), ';' " +
            "order by node) from pz_meta.preview_nodes");

        Assert.Equal("src_a__b|staging.src_a__b|email;stg|staging.stg|contact;v|staging.v|", rows);
    }

    [Fact]
    public async Task Checks_sinks_and_ephemeral_pipelines_go_to_the_excluded_table()
    {
        await using var duck = await Indexed();

        var rows = await duck.ScalarAsync<string>(
            "select string_agg(node || '|' || kind, ';' order by node) from pz_meta.preview_excluded");

        Assert.Equal("eph|ephemeral;lake__out|sink_write;stg__not_null_id|check", rows);
    }

    [Fact]
    public async Task Lookup_on_a_file_without_the_index_reports_it_absent()
    {
        await using var duck = DuckSession.Open(":memory:");

        var lookup = await PreviewIndex.LookupAsync(duck, "stg", default);

        Assert.False(lookup.IndexPresent);
        Assert.Null(lookup.Target);
    }

    [Fact]
    public async Task Lookup_finds_target_and_excluded_kind()
    {
        await using var duck = await Indexed();

        var stg = await PreviewIndex.LookupAsync(duck, "stg", default);
        var eph = await PreviewIndex.LookupAsync(duck, "eph", default);
        var unknown = await PreviewIndex.LookupAsync(duck, "STG", default);

        Assert.Equal(new PreviewTarget("stg", "staging.stg", ["contact"]) with { RedactedColumns = stg.Target!.RedactedColumns },
            stg.Target);
        Assert.Equal(["contact"], stg.Target.RedactedColumns);
        Assert.Equal("ephemeral", eph.ExcludedKind);
        Assert.Null(eph.Target);
        Assert.True(unknown.IndexPresent);
        Assert.Null(unknown.Target);
        Assert.Null(unknown.ExcludedKind);
    }
}
