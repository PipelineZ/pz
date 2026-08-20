using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>An entity-side's options live in <c>entities:</c>
/// OR at the call site -- never split, never merged. There is no effective-config assembly, which is the
/// property that keeps two surfaces from becoming a precedence problem: a reader of one file always sees
/// the whole story for that entity-side.</summary>
public class WriteSurfaceTests
{
    private static ConnectionDef SinkWith(SinkWriteOptions write, string entity = "out") =>
        Sink() with
        {
            EntityWrites = new Dictionary<string, SinkWriteOptions> { [entity] = write },
        };

    private static OutputDef CompileOutput(PzProject project) =>
        ((SinkOutputDef)Assert.Single(DagCompiler.Compile(project, Ctx(project)).Nodes,
            n => n.Kind == NodeKind.SinkWrite).Definition!).Output;

    [Fact]
    public void A_yaml_write_block_supplies_a_bare_sink_call()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out') }} select 1 as id")],
            sinks: [SinkWith(SinkWriteOptions.Default with { Mode = "merge", Keys = ["id"] })]);

        var output = CompileOutput(p);

        Assert.Equal("merge", output.Mode);
        Assert.Equal(["id"], output.Keys);
    }

    [Fact]
    public void Declaring_both_surfaces_is_PZ0341_naming_the_pipeline()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace') }} select 1 as id")],
            sinks: [SinkWith(SinkWriteOptions.Default with { Mode = "merge", Keys = ["id"] })]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WriteSurfaceSplit);
        Assert.Contains("lake.out", error.Message, StringComparison.Ordinal);
        Assert.Equal("pipelines/a.sql", error.File);
    }

    // The distinction is "did the author type a kwarg", not "did the parsed value differ from the
    // default" -- `strategy: 'append'` is indistinguishable from the default once parsed, and silently
    // preferring the YAML there would reintroduce exactly the precedence rule the two-surface
    // design forbids: an option is declared on one surface or the other, never merged between them.
    [Fact]
    public void A_kwarg_equal_to_the_default_still_counts_as_declared()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'append') }} select 1 as id")],
            sinks: [SinkWith(SinkWriteOptions.Default with { Mode = "append" })]);

        Assert.Single(Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p))).Errors,
            e => e.Code == PzErrorCode.WriteSurfaceSplit);
    }

    [Fact]
    public void A_yaml_block_for_a_different_entity_does_not_collide()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace') }} select 1 as id")],
            sinks: [SinkWith(SinkWriteOptions.Default with { Mode = "merge" }, entity: "other")]);

        Assert.Equal("replace", CompileOutput(p).Mode);
    }

    // A connector-owned option (here sqlserver's `columns:`) is an
    // unknown kwarg to sink() -- it rides OutputDef.Options as a plain nested map exactly like any
    // other unrecognized kwarg (SinkFunctionTests.Unknown_kwargs_become_connector_options), so this
    // is a compile seam check, not a new mechanism: does the nested-map shape survive render + compile
    // intact enough for MsEffectiveTypes.ParseDeclared to read it via System.Collections.IDictionary.
    [Fact]
    public void Sink_columns_map_kwarg_flows_into_output_options()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', " +
                "columns: { status: 'nvarchar(20)' }) }} select 1 as id")],
            sinks: [Sink()]);

        var output = CompileOutput(p);

        var columns = Assert.IsAssignableFrom<System.Collections.IDictionary>(output.Options["columns"]);
        Assert.Equal("nvarchar(20)", columns["status"]!.ToString());
    }

    [Fact]
    public void Neither_surface_is_the_documented_default()
    {
        var p = Project([Pipe("a", "INSERT INTO {{ sink('lake', 'out') }} select 1 as id")], sinks: [Sink()]);

        var output = CompileOutput(p);

        Assert.Equal("append", output.Mode);
        Assert.Equal("fail_on_change", output.SchemaPolicy);
    }

    // The write side of the either/or is the only half that survives: PZ0343 would have refused two
    // pipelines passing call-site write options for one entity, but PZ0206 already refuses two pipelines
    // claiming one output at all -- whatever options each passes.
    [Fact]
    public void Two_pipelines_writing_one_entity_is_still_PZ0206()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace') }} select 1 as id"),
             Pipe("b", "INSERT INTO {{ sink('lake', 'out', strategy: 'merge', keys: ['id']) }} select 1 as id")],
            sinks: [Sink()]);

        Assert.Single(Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p))).Errors,
            e => e.Code == PzErrorCode.SinkBindingConflict);
    }
}
