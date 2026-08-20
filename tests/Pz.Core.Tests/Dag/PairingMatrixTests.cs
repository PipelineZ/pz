using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>The compile-time (read x write)
/// pairing matrix for an EXPLICITLY declared <c>sync: {mode: incremental}</c> dataset -- `replace`
/// (PZ03D/<see cref="PzErrorCode.IncompatiblePair"/>) and `append` without consent (PZ0214,
/// <see cref="PzErrorCode.IncrementalAppendUnacknowledged"/>) both violate the effectively-once
/// guarantee an incremental read relies on; `merge` and a full (no sync block) dataset never trip
/// either rule. Mirrors <see cref="DagCompilerTests"/>' project-fixture style
/// (via <see cref="TestProjects"/>). Plan-time feed-shape cells (an implicit/`mode: auto` dataset
/// resolving to Feed) are ExecutionPlanner's guard -- out of scope here.</summary>
public class PairingMatrixTests
{
    // (a) incremental dataset -> replace output: PZ03D.
    [Fact]
    public void Incremental_dataset_feeding_replace_output_is_PZ03D()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "replace") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("write.strategy", error.Hint);
        Assert.Contains("merge", error.Hint);
    }

    // (b) incremental -> append without consent: PZ0214, hint names the write: block form.
    [Fact]
    public void Incremental_dataset_feeding_append_output_without_consent_is_PZ0214()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append") + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged);
        Assert.Contains("crm.orders", error.Message);
        Assert.Contains("lake.out1", error.Message);
        Assert.NotNull(error.Hint);
        Assert.Contains("write:\n  strategy: append\n  duplicates: accept", error.Hint);
    }

    // (c) incremental -> append with duplicates: accept: compiles.
    [Fact]
    public void Incremental_dataset_feeding_append_output_with_consent_compiles()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "append", duplicates: "accept")
                + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    // (d) incremental -> merge: compiles.
    [Fact]
    public void Incremental_dataset_feeding_merge_output_compiles()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"]) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmIncremental("orders", "id", new Dictionary<string, string> { ["id"] = "bigint" })],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    // (e) full (no sync block) x each of replace/append/merge: compiles.
    [Theory]
    [InlineData("replace")]
    [InlineData("append")]
    [InlineData("merge")]
    public void Full_dataset_feeding_any_write_strategy_compiles(string mode)
    {
        string[] keys = mode == "merge" ? ["id"] : [];
        var project = Project(
            [Pipe("stg", Into("out1", mode, keys) + "select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(project, Ctx(project)); // must not throw
        Assert.NotEmpty(dag.Nodes);
    }

    // (f) aggregate: one project with a PZ03D violation and a PZ0214 violation reports both.
    [Fact]
    public void Replace_and_append_violations_on_two_outputs_both_aggregate()
    {
        var project = Project(
            [
                Pipe("stg1", Into("out1", "replace") + "select * from {{ source('crm', 'orders') }}"),
                Pipe("stg2", Into("out2", "append") + "select * from {{ source('crm', 'shipments') }}"),
            ],
            // One dataset per pipeline: PZ0349 refuses a source read by two pipelines,
            // and this fact is about sink-side errors aggregating, not about source sharing.
            sources: [CrmIncrementalMany("id", new Dictionary<string, string> { ["id"] = "bigint" },
                "orders", "shipments")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair && e.Message.Contains("lake.out1"));
        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncrementalAppendUnacknowledged && e.Message.Contains("lake.out2"));
        Assert.Equal(2, ex.Errors.Count);
    }

    // SQL watermark() against a dataset declaring `sync: {mode: auto}` on a feed connector is refused
    // as PzErrorCode.SyncStateConflict -- the PZ0214/PZ03D pairing rules above only ever look at
    // Incremental datasets, so they leave this case alone.
    [Fact]
    public void Sql_watermark_declaration_targeting_a_sync_dataset_is_refused()
    {
        var pipeline = new PipelineDef("p", "-- raw sql --", "table", [], [], "pipelines/p.sql");
        var wref = new WatermarkRef("crm", "orders");
        var rendered = new RenderResult("select ...", new HashSet<DepRef> { new DepRef.Source("crm", "orders") })
        {
            WatermarkRefs = [wref],
        };
        var comparison = new WatermarkComparison(wref.Sentinel, "updated_at", "src_crm__orders",
            Inclusive: false, ValueExprSql: $"'{wref.Sentinel}'");
        var analysis = new WatermarkAnalysis([comparison], [], "REWRITTEN");
        var reader = new StubSqlAstReader(analysis);
        var source = new ConnectionDef("crm", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?>(),
                new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" },
                new SyncModeDef(SyncMode.Auto, null))],
            "connections.yml");
        var sources = new Dictionary<string, ConnectionDef> { ["crm"] = source };

        var result = WatermarkInference.Run(
            [new WatermarkInference.PipelineInput(pipeline, rendered, "assembled sql")], sources, reader);

        var error = Assert.Single(result.Errors);
        Assert.Equal(PzErrorCode.SyncStateConflict, error.Code);
        Assert.False(result.Synthesized.ContainsKey(("crm", "orders")));
    }

    private sealed class StubSqlAstReader(WatermarkAnalysis analysis) : ISqlAstReader
    {

        // The read-hints half of the seam is exercised against the real parser in Pz.DuckDb.Tests;
        // pushing nothing keeps these facts about the pairing matrix alone.
        public ReadHintPlan ExtractReadHints(string sql, string baseTable, string? cursorColumn) =>
            ReadHintPlan.None;
        public WatermarkAnalysis Analyze(string sql, IReadOnlyList<string> sentinels) => analysis;
    }

    // The cdc row of the same published (read x write) pairing
    // matrix -- replace/append refused (PZ0335), merge requires an explicit on_delete choice (PZ0336),
    // merge with on_delete compiles. Full case coverage (consent-missing message, delete-routing
    // PZ0337, CdcDeleteOrigin stamping, aggregation) lives in CdcCompileTests; this keeps the whole
    // published matrix exercised in one place.
    private static ConnectionDef CrmCdc(params string[] datasets) =>
        new("crm", "postgres", new Dictionary<string, object?>(),
            datasets.Select(d => new DatasetDef(d, new Dictionary<string, object?>(), null,
                new SyncModeDef(SyncMode.Cdc, null))).ToList(),
            "connections.yml");

    [Theory]
    [InlineData("replace")]
    [InlineData("append")]
    public void Cdc_dataset_feeding_replace_or_append_output_is_PZ0335(string mode)
    {
        var project = Project(
            [Pipe("stg", Into("out1", mode) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.IncompatiblePair && e.Message.Contains("lake.out1"));
    }

    [Fact]
    public void Cdc_dataset_feeding_merge_output_without_on_delete_is_PZ0336()
    {
        var project = Project(
            [Pipe("stg", Into("out1", "merge", ["id"]) + "select * from {{ source('crm', 'orders') }}")],
            sources: [CrmCdc("orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(project, Ctx(project)));

        Assert.Contains(ex.Errors, e => e.Code == PzErrorCode.CdcConsentMissing && e.Message.Contains("lake.out1"));
    }
}
