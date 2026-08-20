using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>The compile-time cdc column of the (read x write)
/// pairing matrix -- refusals (PZ0335, mirroring the incremental x replace rule in
/// <see cref="PairingMatrixTests"/>), on_delete consent (PZ0336), and delete-key routing (PZ0337).
/// Mirrors <see cref="DagCompilerTests"/>/<see cref="PairingMatrixTests"/>' project-fixture style
/// (via <see cref="TestProjects"/>).</summary>
public class CdcCompileTests
{
    private static ConnectionDef CrmCdc(params string[] datasets) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            datasets.Select(d => new DatasetDef(d, new Dictionary<string, object?>(), null,
                new SyncModeDef(SyncMode.Cdc, null))).ToList(),
            "connections.yml");

    /// <summary>A second, non-cdc (full) single-dataset source -- for the multi-source-pipeline
    /// (join) test cases.</summary>
    private static ConnectionDef ErpFull(string dataset) =>
        new("erp", "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [new DatasetDef(dataset, new Dictionary<string, object?> { ["path"] = $"{dataset}.csv", ["format"] = "csv" }, null)],
            "sources/erp.yml");

    // 1. cdc dataset -> replace output: PZ0335, message names the dataset, the output, and the cell
    //    ("cdc x replace"), hint says "use write.strategy: merge with on_delete".
    [Fact]
    public void Cdc_dataset_feeding_replace_output_is_PZ0335()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "replace") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.Contains("cdc", error.Message);
        Assert.Contains("replace", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("use write.strategy: merge with on_delete", error.Hint);
    }

    // 2. cdc dataset -> append output (with or without duplicates: accept): PZ0335 ("cdc x append";
    //    cdc delivery is change events, append would land raw events as rows).
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Cdc_dataset_feeding_append_output_is_PZ0335_regardless_of_duplicates_consent(bool acceptDuplicates)
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append", duplicates: acceptDuplicates ? "accept" : null) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.Contains("cdc", error.Message);
        Assert.Contains("append", error.Message);
        Assert.Contains("change events", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("use write.strategy: merge with on_delete", error.Hint);
    }

    // 3. cdc dataset -> merge output WITHOUT on_delete: PZ0336 naming the three choices
    //    "declare write.on_delete: delete, soft, or ignore".
    [Fact]
    public void Cdc_dataset_feeding_merge_output_without_on_delete_is_PZ0336()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"]) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CdcConsentMissing);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("declare write.on_delete: delete, soft, or ignore", error.Hint);
    }

    // 4. cdc dataset -> merge output with on_delete: ignore: compiles clean.
    [Fact]
    public void Cdc_dataset_feeding_merge_output_with_on_delete_ignore_compiles()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"], onDelete: "ignore") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    // 5. non-cdc project, merge output WITH on_delete: delete: PZ0337 "on_delete requires a cdc-fed
    //    input".
    [Fact]
    public void NonCdc_dataset_feeding_merge_output_with_on_delete_is_PZ0337()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"], onDelete: "delete") + "select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CdcDeleteRouteInvalid);
        Assert.Contains("lake.out1", error.Message);
        Assert.Contains("on_delete requires a cdc-fed input", error.Message);
    }

    // 6. pipeline joining a cdc dataset AND a second (full) dataset -> merge with on_delete: delete:
    //    PZ0337 "delete keys cannot be routed through a multi-source pipeline"; with on_delete:
    //    ignore the same DAG compiles (upserts only) but still requires on_delete to be DECLARED
    //    (rule 3 applies).
    [Fact]
    public void Multi_source_pipeline_feeding_merge_output_with_on_delete_delete_is_PZ0337()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"], onDelete: "delete") + "" +
                "select a.* from {{ source('crm', 'orders') }} a join {{ source('erp', 'orders') }} b on a.id = b.id")],
            sources: [CrmCdc("orders"), ErpFull("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CdcDeleteRouteInvalid);
        Assert.Contains("lake.out1", error.Message);
        Assert.Contains("delete keys cannot be routed through a multi-source pipeline", error.Message);
    }

    [Fact]
    public void Multi_source_pipeline_feeding_merge_output_with_on_delete_ignore_compiles()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"], onDelete: "ignore") + "" +
                "select a.* from {{ source('crm', 'orders') }} a join {{ source('erp', 'orders') }} b on a.id = b.id")],
            sources: [CrmCdc("orders"), ErpFull("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    [Fact]
    public void Multi_source_pipeline_feeding_merge_output_without_on_delete_is_still_PZ0336()
    {
        // rule 3 (consent missing) applies regardless of source count -- undeclared on_delete on a
        // cdc-fed merge output is refused whether the pipeline is single- or multi-source.
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"]) + "" +
                "select a.* from {{ source('crm', 'orders') }} a join {{ source('erp', 'orders') }} b on a.id = b.id")],
            sources: [CrmCdc("orders"), ErpFull("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.CdcConsentMissing);
        Assert.Contains("lake.out1", error.Message);
    }

    // 7. valid single-source case: SinkOutputDef.CdcDeleteOrigin == new CdcOrigin("crm", "orders").
    [Fact]
    public void Valid_single_source_cdc_delete_output_stamps_CdcDeleteOrigin()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"], onDelete: "delete") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project));

        var sinkNode = Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        var sinkDef = Assert.IsType<SinkOutputDef>(sinkNode.Definition);
        Assert.Equal(new CdcOrigin("crm", "orders"), sinkDef.CdcDeleteOrigin);
    }

    // 8. errors aggregate: one project exhibiting rules 1+3 reports both.
    [Fact]
    public void Replace_violation_and_missing_consent_violation_on_two_outputs_both_aggregate()
    {
        var project = Project(
            [
                Pipe("stg1", Into("out1", "replace") + "select * from {{ source('crm', 'orders') }}"),
                Pipe("stg2", Into("out2", "merge", ["id"]) + "select * from {{ source('crm', 'shipments') }}"),
            ],
            // One dataset per pipeline: PZ0349 refuses a source read by two pipelines,
            // and this fact is about sink-side errors aggregating, not about source sharing.
            sources: [CrmCdc("orders", "shipments")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair && e.Message.Contains("lake.out1"));
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.CdcConsentMissing && e.Message.Contains("lake.out2"));
        Assert.Equal(2, ex.Errors.Count);
    }
}
