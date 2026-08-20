using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>The read side of the either/or. An entity exists
/// because something reads or writes it, so a source() call may declare the whole read itself — but an
/// entity-side's options live in <c>entities:</c> OR at the call site, never both.</summary>
public class ReadSurfaceTests
{
    private static ConnectionDef Crm(params DatasetDef[] entities) =>
        new("crm", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" }, entities,
            "connections.yml");

    private static DatasetDef Entity(string name, string format = "csv") =>
        new(name, new Dictionary<string, object?> { ["format"] = format }, null);

    private static SourceDatasetDef CompileRead(PzProject project) =>
        (SourceDatasetDef)Assert.Single(DagCompiler.Compile(project, Ctx(project)).Nodes,
            n => n.Kind == NodeKind.SourceLoad).Definition!;

    [Fact]
    public void An_entity_declared_only_at_the_call_site_gets_a_node()
    {
        var p = Project(
            [Pipe("a", Into("out", strategy: "replace") +
                       "select 1 as id from {{ source('crm', 'orders', format: 'csv', path: 'o.csv') }}")],
            sources: [Crm()], sinks: [Sink()]);

        var def = CompileRead(p);

        Assert.Equal("orders", def.Dataset.Name);
        Assert.Equal("csv", def.Dataset.Options["format"]);
        Assert.Equal("o.csv", def.Dataset.Options["path"]);
    }

    [Fact]
    public void A_call_site_columns_contract_reaches_the_dataset()
    {
        var p = Project(
            [Pipe("a", Into("out", strategy: "replace") +
                       "select id from {{ source('crm', 'orders', columns: { id: 'bigint' }) }}")],
            sources: [Crm()], sinks: [Sink()]);

        Assert.Equal("bigint", CompileRead(p).Dataset.Columns!["id"]);
    }

    [Fact]
    public void A_yaml_read_block_supplies_a_bare_source_call()
    {
        var p = Project(
            [Pipe("a", Into("out", strategy: "replace") + "select 1 as id from {{ source('crm', 'orders') }}")],
            sources: [Crm(Entity("orders", format: "parquet"))], sinks: [Sink()]);

        Assert.Equal("parquet", CompileRead(p).Dataset.Options["format"]);
    }

    [Fact]
    public void Declaring_both_read_surfaces_is_PZ0341_naming_the_pipeline()
    {
        var p = Project(
            [Pipe("a", Into("out", strategy: "replace") +
                       "select 1 as id from {{ source('crm', 'orders', format: 'csv') }}")],
            sources: [Crm(Entity("orders"))], sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.WriteSurfaceSplit);
        Assert.Contains("crm.orders", error.Message, StringComparison.Ordinal);
        Assert.Equal("pipelines/a.sql", error.File);
    }

    // Both directions aggregate into one report: a project wrong on both surfaces says so once.
    [Fact]
    public void Read_and_write_surface_splits_are_reported_together()
    {
        var sink = Sink() with
        {
            EntityWrites = new Dictionary<string, SinkWriteOptions>
            {
                ["out"] = SinkWriteOptions.Default with { Mode = "replace" },
            },
        };
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace') }} " +
                       "select 1 as id from {{ source('crm', 'orders', format: 'csv') }}")],
            sources: [Crm(Entity("orders"))], sinks: [sink]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Equal(2, ex.Errors.Count(e => e.Code == PzErrorCode.WriteSurfaceSplit));
    }

    [Fact]
    public void An_unknown_connection_is_still_PZ0201()
    {
        var p = Project([Pipe("a", "select 1 from {{ source('nope', 'orders') }}")]);

        var error = Assert.Single(
            Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p))).Errors,
            e => e.Code == PzErrorCode.UnresolvedRef);

        Assert.Contains("no connection named 'nope'", error.Message, StringComparison.Ordinal);
    }

    // An entity that exists in the YAML but nothing reads still produces no node -- declaring an entity
    // is not the same as using it, and that was true before this step too.
    [Fact]
    public void An_unread_yaml_entity_produces_no_node()
    {
        var p = Project(
            [Pipe("a", Into("out", strategy: "replace") + "select 1 as id from {{ source('crm', 'orders') }}")],
            sources: [Crm(Entity("orders"), Entity("unused"))], sinks: [Sink()]);

        Assert.Equal("orders",
            Assert.Single(DagCompiler.Compile(p, Ctx(p)).Nodes, n => n.Kind == NodeKind.SourceLoad).Name
                .Replace("src_crm__", "", StringComparison.Ordinal));
    }

    // A call-site read declares the same thing as the YAML block it replaces, so it must hash the same:
    // moving an option between the two surfaces is cut-and-paste, not a re-extraction.
    [Fact]
    public void Both_surfaces_produce_the_same_node_id()
    {
        var atCallSite = Project(
            [Pipe("a", Into("out", strategy: "replace") +
                       "select 1 as id from {{ source('crm', 'orders', format: 'parquet') }}")],
            sources: [Crm()], sinks: [Sink()]);
        var inYaml = Project(
            [Pipe("a", Into("out", strategy: "replace") + "select 1 as id from {{ source('crm', 'orders') }}")],
            sources: [Crm(Entity("orders", format: "parquet"))], sinks: [Sink()]);

        Assert.Equal(
            Assert.Single(DagCompiler.Compile(inYaml, Ctx(inYaml)).Nodes, n => n.Kind == NodeKind.SourceLoad).Id,
            Assert.Single(DagCompiler.Compile(atCallSite, Ctx(atCallSite)).Nodes, n => n.Kind == NodeKind.SourceLoad).Id);
    }
}
